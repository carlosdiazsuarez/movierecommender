'''
Created on 25/05/2015

@author: cdiaz
'''

import web

import Recommender


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
        return render.index()
    
class coldstart:
    def GET(self):
        source_name = 'GOOGLE_MOVIE_SHOWTIMES_source'
        input = web.input(city=None)       
        Recommender.GMS_request_movies_info(input.city, source_name, metadata_mappings, metadata_content)
        theaters = Recommender.VIRTUOSO_request_theaters_byCity(input.city)
        movies = Recommender.VIRTUOSO_request_movies_byCity(input.city)
        return render.coldstart(input.city, theaters, movies )                                      

class search:
    def GET(self):
        input = web.input(movie=None)
        Recommender.IMDB_search_movies_byName(input.movie, metadata_mappings, metadata_content)
        movies = Recommender.VIRTUOSO_request_movies_byAlternateName(input.movie)                    
        return render.search(input.movie, movies)
    
class movie:
    def GET(self):
        source_name1 = 'DBPEDIA_source'
        source_name2 = 'OMDB_source'
        input = web.input(name=None, uri=None)
        Recommender.DBPEDIA_request_movie_info(input.uri, source_name1, metadata_mappings, metadata_content)
        Recommender.OMDB_request_movie_info(input.name, source_name2, metadata_mappings, metadata_content)
        Recommender.content_based_recommender(input.uri, metadata_mappings, metadata_content)                        

        info = Recommender.VIRTUOSO_request_movie_info_byURI(input.uri)
        triples = Recommender.VIRTUOSO_entity_resolution_byName(input.name, source_name2)
        
        for triple in triples:
            info.append(triple)

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
                
        cbrs = Recommender.VIRTUOSO_request_content_based_recommendation(input.uri)
                          
        return render.movie(input.uri, movie_name, movie_image, movie_desc, movie_year, movie_genre, movie_directors, movie_actors, cbrs)
    
if __name__ == "__main__":       
    app = web.application(urls, globals())
    app.run()