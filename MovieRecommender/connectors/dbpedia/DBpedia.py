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
        
    def query(self, q):
        print q 
        
        result = sparql.query('http://dbpedia.org/sparql', q)
                
        return result        
        
        
    def getURIs(self, name):        
        q = ("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX schema: <http://schema.org/>
            SELECT DISTINCT ?uri ?name WHERE {
                ?uri a schema:Movie .
                ?uri foaf:name ?name . 
                FILTER regex(?name, '""" +  name + """')
            }
        """)
        
        print q 
        
        result = sparql.query('http://dbpedia.org/sparql', q)
                
        return result
        
    
    def getResource(self, uri):
        q = ("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX schema: <http://schema.org/>

            SELECT DISTINCT ?p ?o WHERE {
               <""" + uri + """> ?p ?o .
               FILTER (regex(?o, "http") || lang(?o) = 'en' || datatype(?o) != xsd:string )
            }
            """)
        
        print q
        
        result = sparql.query('http://dbpedia.org/sparql', q)
                
        return result


