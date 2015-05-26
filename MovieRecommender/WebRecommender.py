'''
Created on 25/05/2015

@author: cdiaz
'''

import web

from Recommender import readMetadata_sources, readMetadata_mappings, \
    GMS_SOURCE_request_movies_info
from connectors.virtuoso.virtuoso_connector import VirtuosoConnector


render = web.template.render('templates/')

urls = (
    '/', 'index',
    '/coldstart', 'coldstart'
)
   
##########################################################################
# initialize global variables
SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA= 'SOURCE ITEM NOT EXISTING IN GLOBAL SCHEMA'
SPARQL_ENDPOINT = "http://localhost:8890/sparql"
METADATA = 'METADATA_recommender.xml'
metadata_content  = [] # list of dictionaries
metadata_mappings = {} # dictionary with mappings of global concept with URI
RDF_GRAPH_RECOMMENDER = 'OD_RDF_Graph_Recommender' 

##########################################################################
# read metadata
metadata_content = readMetadata_sources(METADATA)
metadata_mappings = readMetadata_mappings(METADATA)

##########################################################################
# initialize virtuoso
vConn = VirtuosoConnector()

     
class index:
    def GET(self):
        return render.index()
    
class coldstart:
    def GET(self):
        source_name = 'GOOGLE_MOVIE_SHOWTIMES_source'
        input = web.input(city=None)
        triples = []        
        for source in metadata_content:
            if source['source_name'] == source_name:   
                triples = GMS_SOURCE_request_movies_info(input.city, source, metadata_mappings, metadata_content)
        vConn.insert(triples, RDF_GRAPH_RECOMMENDER)                
        return render.coldstart(input.city, triples)                                      

if __name__ == "__main__":       
    app = web.application(urls, globals())
    app.run()