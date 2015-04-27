'''
Created on 14/04/2015

@author: cdiaz
'''

import sys, getopt
from SPARQLWrapper import SPARQLWrapper, JSON, RDF

class DBpedia(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        
    def getURI(self, name):
        sparql = SPARQLWrapper("http://dbpedia.org/sparql")   
        sparql.setQuery("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX schema: <http://schema.org/>
            SELECT DISTINCT ?uri WHERE {
                ?uri a schema:Movie .
                ?uri foaf:name ?name . 
                FILTER regex(?name, '""" +  name + """')
            }
            """)
        
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        
        return results
        
    def getProperty(self, uri):
        sparql = SPARQLWrapper("http://dbpedia.org/sparql")   
        sparql.setQuery("""
            PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>
            SELECT * 
            WHERE {
               <""" + uri + """> ?p ?o .
            }
            """)
        results = sparql.query()
 
        return results