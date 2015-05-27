'''
Created on 25/05/2015

@author: cdiaz
'''

import web

import Recommender
from connectors.imdb.imdb_connector import IMDB


render = web.template.render('templates/')

urls = (
    '/', 'index',
    '/coldstart', 'coldstart',
    '/recommendation', 'recommendation'
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
        for source in metadata_content:
            if source['source_name'] == source_name:   
                Recommender.GMS_SOURCE_request_movies_info(input.city, source, metadata_mappings, metadata_content)
                theaters = Recommender.VIRTUOSO_request_theaters_byCity(input.city)
                movies = Recommender.VIRTUOSO_request_movies_byCity(input.city)
        return render.coldstart(input.city, theaters, movies )                                      

class recommendation:
    def GET(self):
        input = web.input(movie=None)
  
        titles = []
        titles.append(input.movie)
        
        ''' Search orginal title '''
        imdbConnector = IMDB()
        ids =  imdbConnector.getIDs(input.movie)        
        for id in ids:
            results = imdbConnector.getTitleExtra(id)
            for result in results:
                titles.append(result)        
        
        ''' Eliminamos las repeticiones '''
        titles = list(set(titles))
            
        movies = []    
        for title in titles: 
            results = Recommender.VIRTUOSO_request_movie_byName(title)
            for result in results:
                movies.append(result)
                
        ''' Eliminamos las repeticiones '''
        movies = list(set(movies))                
            
        return render.recommendation(input.movie, movies)

if __name__ == "__main__":       
    app = web.application(urls, globals())
    app.run()