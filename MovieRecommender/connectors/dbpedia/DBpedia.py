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
            FILTER contains(?name, """ + '"' +  name + '"' + """ )
            }
        """)        
        
        print q 
        
        try:
        
            result = sparql.query('http://dbpedia.org/sparql', q)
            return result
            
        except sparql.SparqlException as e:
            print 'Exception: '
            print e.message
            print 'Query: '
            print q
            pass
        
    
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


