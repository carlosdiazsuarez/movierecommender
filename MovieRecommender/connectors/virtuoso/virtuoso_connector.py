'''
Created on Apr 14, 2015

@author: Jaime
'''

import json
from rdflib.plugins.sparql.algebra import triples
from rdflib.term import Literal
from sparql import SparqlException
import sparql
import urllib2


#from SPARQLWrapper import SPARQLWrapper, RDF, JSON, POST, SELECT, SPARQLExceptions
class VirtuosoConnector(object):
    '''
    classdocs
    '''
    global VIRTUOSO_SPARQL_ENDPOINT
    global handler
    global opener        
    global sparql  
        
    def __init__(self):
        self.VIRTUOSO_SPARQL_ENDPOINT = "http://localhost:8890/sparql"
        
        self.handler=urllib2.HTTPHandler(debuglevel=1)
        self.opener = urllib2.build_opener(self.handler)
        urllib2.install_opener(self.opener)
        #self.sparql = SPARQLWrapper(self.VIRTUOSO_SPARQL_ENDPOINT, returnFormat="json")
        
        print '\n' + '*'*40
        print 'VIRTUOSO CONNECTOR REGISTERED (using https://pypi.python.org/pypi/sparql-client/ python package)'
        print '*'*40
        
    def query(self, query):
        # launch
        try:
            result = sparql.query(self.VIRTUOSO_SPARQL_ENDPOINT, query)
            print query
            print "query succeded, result: " + result.variables[0]
            
            triples = []
            for row in result:
                values = sparql.unpack_row(row)
                triples.append([values[0], values[1], values[2]])
            return triples
            
        except sparql.SparqlException as e:
            print 'Exception: '
            print e
            print 'Query: '
            print query
            pass           

        
    ''' 
        A more generic insert function
    '''    
    def insert(self, in_triples, in_RDFGraph):

        print '\n' + '*'*40
        print 'VIRTUOSO CONNECTOR insert'
        print '*'*40
        
        
        query = 'INSERT IN GRAPH <' + in_RDFGraph +'>\n'        
        query += '{\n'
        for triple in in_triples:
            if "http" in triple[2]:
                query += '<' + triple[0] + '> <' + triple[1] + '> <' + triple[2] + '> .\n'
            else: 
                query += '<' + triple[0] + '> <' + triple[1] + '> "' + triple[2] + '" .\n'
        query += '}'
        
        # launch
        try:
            result = sparql.query(self.VIRTUOSO_SPARQL_ENDPOINT, query)
            print query
            print "insert succeded, result: " + result.variables[0]
            return result
            
        except sparql.SparqlException as e:
            print 'Exception: '
            print e
            print 'Query: '
            print query
            pass
        
    def insert_triples_movie_info(self, in_film_name, in_triples_lists, in_RDFgraph, in_source_name):
        print '\n' + '*'*40
        print 'VIRTUOSO CONNECTOR insert_triples_movie_info'
        print '*'*40
        #PARAMETER: in_triples_lists 
            #1- list of elements (i.e. [ [...], [...], [...] ]. 
            #2- Each element [...] is a triple like [SUBJECT, PREDICATE, OBJECT]
            #i.e. [['A Clockwork Orange', u'https://schema.org/description', u'Protagonist Alex DeLarge is an "ultraviolent" youth in futuristic Britain. As with all luck, his eventually runs out and he\'s arrested and convicted of murder and rape. While in prison, Alex learns of an experimental program in which convicts are programed to detest violence. If he goes through the program, his sentence will be reduced and he will be back on the streets sooner than expected. But Alex\'s ordeals are far from over once he hits the mean streets of Britain that he had a hand in creating.'], ['A Clockwork Orange', u'https://schema.org/inLanguage', u'English'], ['A Clockwork Orange', u'https://schema.org/name', u'A Clockwork Orange'], ['A Clockwork Orange', u'https://schema.org/Country', u'UK, USA'], ['A Clockwork Orange', u'https://schema.org/author', u'Stanley Kubrick (screenplay), Anthony Burgess (novel)'], ['A Clockwork Orange', u'https://schema.org/aggregateRating', u'8.4'], ['A Clockwork Orange', u'https://schema.org/director', u'Stanley Kubrick'], ['A Clockwork Orange', u'https://schema.org/actor', u'Malcolm McDowell, Patrick Magee, Michael Bates, Warren Clarke'], ['A Clockwork Orange', u'https://schema.org/copyrightYear', u'1971'], ['A Clockwork Orange', u'https://schema.org/genre', u'Crime, Drama, Sci-Fi'], ['A Clockwork Orange', u'https://schema.org/award', u'Nominated for 4 Oscars. Another 12 wins & 13 nominations.'], ['A Clockwork Orange', u'https://schema.org/duration', u'136 min'], ['A Clockwork Orange', u'https://schema.org/image', u'http://ia.media-imdb.com/images/M/MV5BMTY3MjM1Mzc4N15BMl5BanBnXkFtZTgwODM0NzAxMDE@._V1_SX300.jpg']]
        #PARAMETER: in_graph
            # graph to put the information into

        # this is an example of a hardcoded query
        query = """
        INSERT IN GRAPH <Jaime_Graph_Predator>
        { <Predator_9_MAGIC_URI> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Movie> .
        <Predator_9_MAGIC_URI> <https://schema.org/name> "Predator 9" .
        }
        """
        #print 'HARDCODED QUERY IS....\n' + query +'\n\n'
      
        # generate a insert query
        SPQRQL_query_insert_part = 'INSERT IN GRAPH <' + in_RDFgraph +'>' + '\n'
        
        SPQRQL_query_triple_part = '{' 
        SPQRQL_query_triple_part += '\n'
        SPQRQL_query_triple_part += '<' + in_triples_lists[0][0] + '> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Movie>.'
        SPQRQL_query_triple_part += '\n'
        for i in range(0,len(in_triples_lists)): 
            #discover if its numeric to avoid quotations ("") on string
            if in_triples_lists[i][2].replace('.','',1).isdigit():
                single_triple = '<' + in_triples_lists[i][0] + '> <' + in_triples_lists[i][1] + '> ' + in_triples_lists[i][2] + ' .'
            else:
                #single_triple = '<' + in_triples_lists[i][0] + '> <' + in_triples_lists[i][1] + '> '+ '"' + in_triples_lists[i][2] + '"' + '.' 
                single_triple = '<' + in_triples_lists[i][0] + '> <' + in_triples_lists[i][1] + '> '+ '"' + in_triples_lists[i][2].replace('"', '') + '"' + '.' 
            SPQRQL_query_triple_part += single_triple + '\n'
        SPQRQL_query_triple_part += '}'
        
        SPARQL_full_insert_query = SPQRQL_query_insert_part + SPQRQL_query_triple_part
        #print 'FRANKESNTEIN QUERY IS...\n' + SPARQL_full_insert_query
        
        query = SPARQL_full_insert_query
        print query
        
        # launch
        try:
            #self.sparql.setQuery(query)
            #self.sparql.setMethod(POST)    
            #result = self.sparql.query()
            result = sparql.query(self.VIRTUOSO_SPARQL_ENDPOINT, query)
            #data = result.convert()
            #print data
            #print json.dumps(data, indent= 2, separators=(',',':'))
            
            #print '\n' + '*'*40
            print 'VIRTUOSO insert_triples_movie_info SUCCESSFULLY: (film: ' + in_film_name +') ('+ in_source_name + ')'
            #print '*'*40

            
        except sparql.SparqlException as e:
            #print '\n' + '*'*40
            print 'VIRTUOSO insert_triples_movie_info FAILED!!!!!!: (film: ' + in_film_name +') ('+ in_source_name + ')'
            #print '*'*40
            print 'Exception: '
            print e
            print 'Query: '
            print query
            pass
        
        


