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
    '/search', 'search'
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

class search:
    def GET(self):
        input = web.input(movie=None)
        Recommender.search_movies_byName(input.movie, metadata_mappings, metadata_content)
        movies = Recommender.VIRTUOSO_request_movies_byAlternateName(input.movie)                    
        return render.search(input.movie, movies)

if __name__ == "__main__":       
    app = web.application(urls, globals())
    app.run()