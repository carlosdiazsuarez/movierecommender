'''
Created on 25/05/2015

@author: cdiaz
'''

import re
import web
import sys

import Recommender
import Recommender_part2
from Recommender import RDF_GRAPH_FINAL


render = web.template.render('templates/')

urls = (
    '/', 'index',
    '/coldstart', 'coldstart',
    '/search', 'search',
    '/movie', 'movie'
)
  
##########################################################################
# initialize global variables
SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA= 'SOURCE ITEM NOT EXISTING IN GLOBAL SCHEMA'
SPARQL_ENDPOINT = "http://localhost:8890/sparql"
METADATA = 'METADATA_recommender.xml'
metadata_content  = [] # list of dictionaries
metadata_mappings = {} # dictionary with mappings of global concept with URI

##########################################################################
# read metadata
metadata_content = Recommender.readMetadata_sources(METADATA)
metadata_mappings = Recommender.readMetadata_mappings(METADATA)
   
class index:
    def GET(self):
        
        userId = web.cookies().get('userId')
        if userId == None:
            f = open('WebRecommender.status', 'r')
            counter = int(f.readline())
            web.setcookie('userId', str(counter), 3600)
            f.close()
            counter += 1
            f = open('WebRecommender.status', 'w')
            f.write(str(counter))
            f.close()

        return render.index()
    
class coldstart:
    def GET(self):
        userId = web.cookies().get('userId')
        web.setcookie('userId', str(userId), 3600)
        
        source_name = 'GOOGLE_MOVIE_SHOWTIMES_source'
        input = web.input(city=None)       
        Recommender.GMS_request_movies_info(input.city, source_name, metadata_mappings, metadata_content)
        
        '''
        Do Data Lake Transformations
        '''                            
        Recommender.VIRTUOSO_doDataLakeTransformations()
        
        theaters = Recommender.VIRTUOSO_request_theaters_byCity(input.city, RDF_GRAPH_FINAL)
        
        movies = Recommender.VIRTUOSO_request_movies_byCity(input.city, RDF_GRAPH_FINAL)
        
        return render.coldstart(input.city, theaters, movies )                                      

class search:
    def GET(self):
        userId = web.cookies().get('userId')
        web.setcookie('userId', str(userId), 3600)
        
        input = web.input(movie=None)
        Recommender.IMDB_search_movies_byName(input.movie, metadata_mappings, metadata_content)
        
        '''
        Do Data Lake Transformations
        '''                            
        Recommender.VIRTUOSO_doDataLakeTransformations()
        
        movies = Recommender.VIRTUOSO_request_movies_byAlternateName(input.movie, RDF_GRAPH_FINAL)
                            
        return render.search(input.movie, movies)
    
class movie:
    def GET(self):
        userId = web.cookies().get('userId')
        web.setcookie('userId', str(userId), 3600)
        
        source_name1 = 'DBPEDIA_source'
        source_name2 = 'OMDB_source'
        source_name3 = 'TWITTER_source'
        input = web.input(name=None, uri=None)
        
        '''
        Request the movie info from the sources
        '''
        Recommender.DBPEDIA_request_movie_info(input.uri, source_name1, metadata_mappings, metadata_content)
        Recommender.OMDB_request_movie_info(input.name, source_name2, metadata_mappings, metadata_content)
        Recommender.TWITTER_SOURCE_request_movie_info(input.name, source_name3, metadata_mappings, metadata_content)

        '''
        Insert the user interaction in Virtuoso 
        '''
        Recommender.VIRTUOSO_insert_user_interaction(str(userId), input.uri)
        
        '''
        Insert the content based recommendations in Virtuoso
        '''
        Recommender.content_based_recommender(input.uri, metadata_mappings, metadata_content)
                                    
        '''
        Do Data Lake Transformations
        '''                            
        Recommender.VIRTUOSO_doDataLakeTransformations()
        
        '''
        Request movie DBpedia info from Virtuoso 
        '''
        info = Recommender.VIRTUOSO_request_movie_info_byURI(input.uri, RDF_GRAPH_FINAL)
        
        '''
        Apply entity resolution to the other sources (OMDB) and append results to previous info
        '''
        '''
        triples = Recommender.VIRTUOSO_entity_resolution_byName(input.name, source_name2)        
        for triple in triples:
            info.append(triple)
        '''
        
        '''
        IMPORTANT: FIRST, TAKE THE VALUE FROM DBPEDIA
                    IF MISSING, TAKE THE VALUE FROM OMDB
        '''
        movie_name = ""
        movie_image = ""
        movie_desc = ""
        movie_year = ""
        movie_genre = ""
        movie_directors = []
        movie_actors = []
        for field in info:
            if field[1] == 'https://schema.org/name':
                if movie_name == "":
                    movie_name = field[2]
            if field[1] == 'https://schema.org/image':
                if movie_image == "":
                    movie_image = field[2]
            if field[1] == 'https://schema.org/description':
                if movie_desc == "":
                    movie_desc = field[2]                    
            if field[1] == 'https://schema.org/copyrightYear':
                if movie_year == "":
                    movie_year = field[2]                                        
            if field[1] == 'https://schema.org/genre':
                if movie_genre == "":
                    movie_genre = field[2]
            if field[1] == 'https://schema.org/director':
                movie_directors.append(field[2])
            if field[1] == 'https://schema.org/actor':
                movie_actors.append(field[2])                        
                        
        '''
        Get content-based recommendations 
        '''        
        cbrs = Recommender.VIRTUOSO_request_content_based_recommendation(input.uri)
        
        '''
        Get user-based recommendations from user interaction
        '''
        ubrs = Recommender.VIRTUOSO_request_user_based_recommender_byUserInteraction(userId, input.uri)
        
        '''
        Get user-based recommendations from Movilens
        '''
        ubrs_Movilens = Recommender_part2.MOVILENS_get_user_based_recommendations()        
        ubrs2 = Recommender.VIRTUOSO_request_user_based_recommender_byMovieNames(ubrs_Movilens, metadata_mappings, metadata_content)        
        
        '''
        Get enrichment as Tweets
        '''
        tweets = Recommender.VIRTUOSO_request_tweets_byName(input.name, source_name3)
                          
        return render.movie(input.uri, movie_name, movie_image, movie_desc, movie_year, movie_genre, movie_directors, movie_actors, cbrs, ubrs, ubrs2, tweets)
    
if __name__ == "__main__":    
    '''
    Insert the user based recommendations from Movilens in Virtuoso
    '''
    if str(sys.argv[2]) == "true":
        Recommender_part2.MOVILENS_initialization()
    
    app = web.application(urls, globals())

    app.run()