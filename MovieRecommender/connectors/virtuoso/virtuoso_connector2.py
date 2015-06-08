'''
Created on Apr 14, 2015

@author: Jaime
'''
from SPARQLWrapper import SPARQLExceptions
from SPARQLWrapper.Wrapper import SPARQLWrapper, POST
import urllib2
import sparql

class virtuoso_connector(object):
    '''
    classdocs
    '''
    global VIRTUOSO_SPARQL_ENDPOINT
    global handler
    global opener        
    global sparql  
        
    def __init__(self, in_http_location, in_port, in_filder_endPoint):
        # self.VIRTUOSO_SPARQL_ENDPOINT = "http://localhost:8890/sparql"
        self.VIRTUOSO_SPARQL_ENDPOINT = in_http_location + ':' + in_port + in_filder_endPoint
        # print 'PRINTING FRANKENSTEIN VIRTUOSO SETTING: '+ self.VIRTUOSO_SPARQL_ENDPOINT
        
        self.handler=urllib2.HTTPHandler(debuglevel=1)
        self.opener = urllib2.build_opener(self.handler)
        urllib2.install_opener(self.opener)
        self.sparql = SPARQLWrapper(self.VIRTUOSO_SPARQL_ENDPOINT, returnFormat="json")
        
        print '\n' + '*'*40
        print 'VIRTUOSO CONNECTOR REGISTERED (HTTP_LOCATION : ' + in_http_location + ', PORT: ' + in_port  + ', END POINT FOLDER: ' + in_filder_endPoint +')'
        print 'VIRTUOSO CONNECTOR REGISTERED (using SPARQLWrapper python package)'
        print '*'*40
     
    def cloneGraph(self, in_source_graph, out_destination_graph):
        query = """
        INSERT {
                GRAPH <http://graph.com/new> {
                ?s ?p ?o }
        }
        WHERE {
               GRAPH <http://graph.com/old> {
                ?s ?p ?o
                }
        }
        """

        # generate a insert query
        SPQRQL_cloneGraph = 'INSERT { \n'
        SPQRQL_cloneGraph += 'GRAPH <' + out_destination_graph +'>' + '{' + '\n'
        SPQRQL_cloneGraph += '?s ?p ?o } \n'
        SPQRQL_cloneGraph += '}'
        SPQRQL_cloneGraph += 'WHERE { \n'
        SPQRQL_cloneGraph += 'GRAPH <' + in_source_graph +'>' + '{ ' + '\n'
        SPQRQL_cloneGraph += '?s ?p ?o'
        SPQRQL_cloneGraph += '} \n'
        SPQRQL_cloneGraph += '}'
   
        print query
        print SPQRQL_cloneGraph
        query = SPQRQL_cloneGraph
        
        # launch
        try:
            self.sparql.setQuery(query)
            self.sparql.setMethod(POST)    
            result = self.sparql.query()
            data = result.convert()
            #print data
            #print json.dumps(data, indent= 2, separators=(',',':'))
            
            print '\n' + '*'*40
            print 'VIRTUOSO cloneGraph for annotations SUCCESSFULLY: (original: ' + in_source_graph +') (clone: '+ out_destination_graph + ')'
            print '*'*40

            
        except SPARQLExceptions.QueryBadFormed:
            print '\n' + '*'*40
            print 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX - SPARQLExceptions.QueryBadFormed'
            print 'VIRTUOSO cloneGraph for annotations FAILED!!!!!!: (original: ' + in_source_graph +') (clone: '+ out_destination_graph + ')'
            print '*'*40
            pass

        
        
    def insert_triples_movie_info(self, in_film_name, in_triples_lists, in_RDFgraph, in_source_name):
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
        
        # launch
        try:
            self.sparql.setQuery(query)
            self.sparql.setMethod(POST)    
            result = self.sparql.query()
            data = result.convert()
            #print data
            #print json.dumps(data, indent= 2, separators=(',',':'))
            
            print '\n' + '*'*40
            print 'VIRTUOSO insert_triples_movie_info SUCCESSFULLY: (film: ' + in_film_name +') ('+ in_source_name + ')'
            print '*'*40

            
        except SPARQLExceptions.QueryBadFormed:
            print '\n' + '*'*40
            print 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX - SPARQLExceptions.QueryBadFormed'
            print 'VIRTUOSO insert_triples_movie_info FAILED!!!!!!: (film: ' + in_film_name +') ('+ in_source_name + ')'
            print '*'*40
            pass
        








    
    def insert_triples_user_rating_movie(self, in_triples_lists, in_RDFgraph, in_source_name):
        #PARAMETER: in_triples_lists 
            #                                   REIFICATION 3 TRIPLES       REIFICATION 3 TRIPLES
            #                                      Jaime Predator 5          Jaime Predator2 4.6
            #1- list of elements (i.e. [         [...], [...], [...]         [...], [...], [...]     ]. 
            #[
            #['URI_Property_UserRatingMovie_RATING_1], 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'https://schema.org/Rating_UserMovieRating'].
            #['URI_Property_UserRatingMovie_RATING_1', 'https://schema.org/Rating', u'Serenity_(2005)_MOVIELENS_source'], 
            #['URI_Property_UserRatingMovie_RATING_1', 'https://schema.org/Rating', 5.0], 
            #['URI_Property_UserRatingMovie_RATING_1', 'https://schema.org/Rating', 'URI_MOVIELENS_USER_344']
            #]
            #2- Each element [...] is a triple like [SUBJECT, PREDICATE, OBJECT]
            #i.e. [['A Clockwork Orange', u'https://schema.org/description', u'Protagonist Alex DeLarge is an "ultraviolent" youth in futuristic Britain. As with all luck, his eventually runs out and he\'s arrested and convicted of murder and rape. While in prison, Alex learns of an experimental program in which convicts are programed to detest violence. If he goes through the program, his sentence will be reduced and he will be back on the streets sooner than expected. But Alex\'s ordeals are far from over once he hits the mean streets of Britain that he had a hand in creating.'], ['A Clockwork Orange', u'https://schema.org/inLanguage', u'English'], ['A Clockwork Orange', u'https://schema.org/name', u'A Clockwork Orange'], ['A Clockwork Orange', u'https://schema.org/Country', u'UK, USA'], ['A Clockwork Orange', u'https://schema.org/author', u'Stanley Kubrick (screenplay), Anthony Burgess (novel)'], ['A Clockwork Orange', u'https://schema.org/aggregateRating', u'8.4'], ['A Clockwork Orange', u'https://schema.org/director', u'Stanley Kubrick'], ['A Clockwork Orange', u'https://schema.org/actor', u'Malcolm McDowell, Patrick Magee, Michael Bates, Warren Clarke'], ['A Clockwork Orange', u'https://schema.org/copyrightYear', u'1971'], ['A Clockwork Orange', u'https://schema.org/genre', u'Crime, Drama, Sci-Fi'], ['A Clockwork Orange', u'https://schema.org/award', u'Nominated for 4 Oscars. Another 12 wins & 13 nominations.'], ['A Clockwork Orange', u'https://schema.org/duration', u'136 min'], ['A Clockwork Orange', u'https://schema.org/image', u'http://ia.media-imdb.com/images/M/MV5BMTY3MjM1Mzc4N15BMl5BanBnXkFtZTgwODM0NzAxMDE@._V1_SX300.jpg']]
        #PARAMETER: in_graph
            # graph to put the information into

        # this is an example of a hardcoded query
        query = """
        INSERT IN GRAPH <OD_RDF_Graph_Recommender>
        { <URI_Property_UserRatingMovie_RATING_1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Rating_UserMovieRating>.
        <URI_Property_UserRatingMovie_RATING_1> <https://schema.org/Rating> <Serenity_(2005)_MOVIELENS_source> .
        <URI_Property_UserRatingMovie_RATING_1> <https://schema.org/Rating> 5.0 .
        <URI_Property_UserRatingMovie_RATING_1> <https://schema.org/Rating> <URI_MOVIELENS_USER_Jaime> .
        }
        """
        print 'HARDCODED QUERY IS....\n' + query +'\n\n'
      
        # generate a insert query
        SPQRQL_query_insert_part = 'INSERT IN GRAPH <' + in_RDFgraph +'>' + '\n'
        
        SPQRQL_query_triple_part = '{' 
        SPQRQL_query_triple_part += '\n'

        for i in range(0,len(in_triples_lists)): 
            #discover if its numeric to avoid quotations ("") on string    
            #if in_triples_lists[i][2].replace('.','',1).isdigit():
            if isinstance(in_triples_lists[i][2], (int, long, float, complex)):
                single_triple = '<' + in_triples_lists[i][0] + '> <' + in_triples_lists[i][1] + '> ' + str(in_triples_lists[i][2]) + ' .'
            else:
                #single_triple = '<' + in_triples_lists[i][0] + '> <' + in_triples_lists[i][1] + '> '+ '"' + in_triples_lists[i][2] + '"' + '.' 
                single_triple = '<' + in_triples_lists[i][0] + '> <' + in_triples_lists[i][1] + '> <' + in_triples_lists[i][2] + '>' + '.' 
            SPQRQL_query_triple_part += single_triple + '\n'
        SPQRQL_query_triple_part += '}'
        
        SPARQL_full_insert_query = SPQRQL_query_insert_part + SPQRQL_query_triple_part
        #print 'FRANKESNTEIN QUERY IS...\n' + SPARQL_full_insert_query
        
        query = SPARQL_full_insert_query
        
        # launch
        try:
            self.sparql.setQuery(query)
            self.sparql.setMethod(POST)    
            result = self.sparql.query()
            data = result.convert()
            #print data
            #print json.dumps(data, indent= 2, separators=(',',':'))
            
            print '\n' + '*'*40
            print 'VIRTUOSO insert_triples_user_rating_movie SUCCESSFULLY: (' + in_source_name + ')'
            print '*'*40

            
        except SPARQLExceptions.QueryBadFormed:
            print '\n' + '*'*40
            print 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX - SPARQLExceptions.QueryBadFormed'
            print 'VIRTUOSO insert_triples_user_rating_movie FAILED!!!!!!: (' + in_source_name + ')'
            print '*'*40
            pass
        

    # returns a dictionary like this:
    #{
    #'Jaime': {'Predator 1': 5, 'Predator 2':4.5},    
    #'Jesulin Ubrique': {'Toros 1': 1, 'Toros 2':2, 'Toros 3': 3, 'Toros 4': 4},
    #}
    def get_USERS_and_RATINGS(self, in_RDFgraph, in_source_name):
        # this is an example of a hardcoded query
        query = """
        SELECT ?user ?movie ?rating FROM <OD_RDF_Graph_Recommender>
        WHERE
        {
        ?Tertiaryrating <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Rating_UserMovieRating>.
        ?Tertiaryrating <https://schema.org/Rating> ?rating.
        ?Tertiaryrating <https://schema.org/RatingMovie> ?movie.
        ?Tertiaryrating <https://schema.org/RatingUser> ?user.
        }
        ORDER BY ?user ?movie ?rating
        """
        #print 'HARDCODED QUERY IS....\n' + query +'\n\n'
      
        # generate a insert query
        SPQRQL_query_select_part = 'SELECT ?user ?movie ?rating FROM <' + in_RDFgraph +'>' + '\n'
        SPQRQL_query_where_part = 'WHERE \n'
        SPQRQL_query_where_part += '{'
        SPQRQL_query_where_part += '\n'
        SPQRQL_query_where_part += '?Tertiaryrating <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Rating_UserMovieRating>.'
        SPQRQL_query_where_part += '\n'
        SPQRQL_query_where_part += '?Tertiaryrating <https://schema.org/Rating> ?rating.'
        SPQRQL_query_where_part += '\n'
        SPQRQL_query_where_part += '?Tertiaryrating <https://schema.org/RatingMovie> ?movie.'
        SPQRQL_query_where_part += '\n'
        SPQRQL_query_where_part += '?Tertiaryrating <https://schema.org/RatingUser> ?user.'
        SPQRQL_query_where_part += '\n'
        SPQRQL_query_where_part += '}'
        SPQRQL_query_where_part += '\n'
        
        SPQRQL_query_order_part = 'ORDER BY ?user ?movie ?rating'
        
        SPARQL_full_select_query = SPQRQL_query_select_part + SPQRQL_query_where_part + SPQRQL_query_order_part
        print 'FRANKESNTEIN QUERY IS...\n' + SPARQL_full_select_query  + '\n'
        
        query = SPARQL_full_select_query
        
        # launch
        try:
            self.sparql.setQuery(query)
            self.sparql.setMethod(POST)    
            result = self.sparql.query()
            data_results = result.convert()
            #print data_results
            #print json.dumps(data_results, indent= 2, separators=(',',':'))
            
            # return the dic
            prefs={}
            for result in data_results["results"]["bindings"]:
                #print 'USER     : ' + result['user']['value']
                #print 'MOVIE    : ' + result['movie']['value']
                #print 'RATING   : ' + result['rating']['value']
                #print 
                user = result['user']['value']
                movie = result['movie']['value']
                rating = result['rating']['value']
                prefs.setdefault(user,{})
                prefs[user][movie]=float(rating)
                
            #print 'RETRIEVED RATINGS!!!!!!!!!!!!!!!' + json.dumps(prefs, indent=2)
            
            print '\n' + '*'*40
            print 'VIRTUOSO get_USERS_and_RATINGS SUCCESSFULLY: ('+ in_source_name + ')'
            print '*'*40
            
            
            return prefs
            
        except SPARQLExceptions.QueryBadFormed:
            print '\n' + '*'*40
            print 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX - SPARQLExceptions.QueryBadFormed'
            print 'VIRTUOSO get_USERS_and_RATINGS FAILED!!!!!!: ('+ in_source_name + ')'
            print '*'*40
            pass
    
    ''' 
        A more generic insert function
    '''    
    def insert(self, in_triples, in_RDFGraph):

        print '\n' + '*'*40
        print 'VIRTUOSO CONNECTOR insert'
        print '*'*40
        
        for triple in in_triples:
            '''
            query = 'PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> '
            query += 'PREFIX foaf:  <http://xmlns.com/foaf/0.1/> '
            '''
            query = 'INSERT IN GRAPH <' + in_RDFGraph + '> '        
            query += '{ '

            if isinstance(triple[2], (int, long, float, complex)):
                query += '<' + triple[0] + '> <' + triple[1] + '> <' + str(triple[2]) + '> . '
            elif "http" in triple[2]:
                query += '<' + triple[0] + '> <' + triple[1] + '> <' + triple[2] + '> . '
            else: 
                query += '<' + triple[0] + '> <' + triple[1] + '> "' + triple[2].replace('"','') +  '" . '
            
            query += '}'
        
            # launch
            try:
                result = sparql.query(self.VIRTUOSO_SPARQL_ENDPOINT, query)
                print query
                            
            except sparql.SparqlException as e:
                print 'Exception: '
                print e.message
                print 'Query: '
                print query
                return -1
        
        return 0

    
