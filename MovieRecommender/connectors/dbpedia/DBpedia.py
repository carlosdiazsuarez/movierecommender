'''
Created on 14/04/2015

@author: cdiaz
'''

import sys, getopt
import sparql 

class DBpedia(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        
    def getURIs(self, name):        
        q = ("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX schema: <http://schema.org/>
            SELECT DISTINCT ?uri ?name WHERE {
                ?uri a schema:Movie .
                ?uri foaf:name ?name . 
                FILTER regex(?name, '""" +  name + """')
                FILTER (langMatches(lang(?name),"en"))
            }
        """)
        results = sparql.query('http://dbpedia.org/sparql', q)
                
        return results
    
    def getResource(self, uri):
        q = ("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX schema: <http://schema.org/>
            SELECT ?p ?o WHERE {
                <""" + uri + """> ?p ?o .
                FILTER (langMatches(lang(?o),"en"))
            }
        """)
        result = sparql.query('http://dbpedia.org/sparql', q)
                
        return result
