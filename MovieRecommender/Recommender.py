'''
Created on 14/04/2015

@author: cdiaz jandres
'''

import codecs
from collections import Counter
import json
import logging
import os
from pattern.server import Row
import re, math
import sparql
import string
import sys
from xml.dom import minidom

from connectors.dbpedia.DBpedia import DBpedia
from connectors.googlemoviesshowtimes.gms_connector import GoogleMovieShowtimes
from connectors.http_api_request.http_api_request_connector import http_api_request_connector
from connectors.twitter.twitter_patternPkg_connector import twitter_patternPkg_connector
from connectors.twitter.twitter_streaming_connector import twitter_streaming_connector
from connectors.virtuoso.virtuoso_connector import VirtuosoConnector


##########################################################################
# initialize virtuoso
RDF_GRAPH_RECOMMENDER = 'OD_RDF_Graph_Recommender' 
virtuosoConnector = VirtuosoConnector()


SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA= 'SOURCE ITEM NOT EXISTING IN GLOBAL SCHEMA'

# does the mapping global concept with uris
def readMetadata_mappings(metadata_file):
    print '\n' + '*'*40
    print 'PROCEEDING TO READ METADATA (MAPPINGS):'
    print '*'*40
    workingDir=os.getcwd()
    xmldoc = minidom.parse(metadata_file)    
    field_mappings = xmldoc.getElementsByTagName('uri_property') 

    global_concept_mappings_dict = {}    
    for single_mapping in field_mappings :
        global_concept_name = single_mapping.attributes['name'].value
        global_concept_uri = single_mapping.childNodes[0].nodeValue
        global_concept_name = re.sub(r'\s+', '', global_concept_name)
        global_concept_uri = re.sub(r'\s+', '', global_concept_uri)                                                
        global_concept_mappings_dict[global_concept_name] =  global_concept_uri
    #print global_concept_mappings_dict
    #print json.dumDESAps(global_concept_mappings_dict, indent=2)

    print json.dumps(global_concept_mappings_dict, indent=2)

    return global_concept_mappings_dict
    



# returns a list of dictionaries
#[{'query_type': u'HTTP_API_request', 'source_name': u'OMBD_source', 'location': u'http://www.omdbapi.com/', 'attributes': [u'movie_name', u'Title', u'movie_desc', u'Plot', u'movie_genre', u'Genre', u'movie_director', u'Director', u'movie_author', u'Writer', u'movie_actor', u'Actors', u'movie_year', u'Year', u'movie_runtime', u'Runtime', u'movie_language', u'Language', u'movie_country', u'Country', u'movie_awards', u'Awards', u'movie_rating', u'imdbRating', u'movie_poster', u'Poster']}, {'query_type': u'SPARQL', 'source_name': u'DBPEDIA_source', 'location': u'http://dbpedia.org/page/', 'attributes': [u'movie_name', u'name', u'movie_desc', u'description', u'movie_genre', u'genre', u'movie_director', u'director', u'movie_author', u'author', u'movie_actor', u'actor', u'movie_year', u'copyrightYear', u'movie_runtime', u'duration', u'movie_studio', u'productionCompany', u'movie_language', u'inLanguage', u'movie_country', u'Country', u'movie_budget', u'budget', u'movie_comment', u'comment', u'movie_awards', u'award', u'movie_rating', u'aggregateRating', u'movie_poster', u'image']}, {'query_type': u'TOPIC', 'source_name': u'TWITTER_source', 'location': u'TWEEPY_Python', 'attributes': [u'information_date', u'created_at', u'movie_name', u'movie_name', u'user_name', u'user_name', u'movie_comment', u'text']}, {'query_type': u'HTTP_request', 'source_name': u'GOOGLE_showtimes_source', 'location': u'https://www.google.es/movies', 'attributes': [u'information_date', u'update_date', u'cinema_name', u'cinema', u'cinema_location', u'location', u'movie_name', u'movie_name']}, {'query_type': u'SQL', 'source_name': u'MOVIELENS_source', 'location': u'https://localhost:8080', 'attributes': [u'movie_name', u'Title', u'movie_genre', u'Genre', u'user_gender', u'Title', u'user_age', u'Plot', u'user_occupation', u'Genre', u'user_zipcode', u'Title', u'user_rating', u'Rating', u'information_Date', u'Timestamp']}]
def readMetadata_sources(metadata_file):
    print '\n' + '*'*40
    print 'PROCEEDING TO READ METADATA (SOURCES):'
    print '*'*40
    workingDir=os.getcwd()
    print 'Working Dir: ', workingDir
    print 'Full Path Metadata File: ' + workingDir + '/' + metadata_file
    xmldoc = minidom.parse(metadata_file)    
    data_sources = xmldoc.getElementsByTagName('source_name') 

    sources_all = []    
    for s in data_sources :
        source_dict = {}
        source_name = s.attributes['name'].value  
        source_location = s.getElementsByTagName('location')[0].firstChild.nodeValue
        source_query_type = s.getElementsByTagName('query_type')[0].firstChild.nodeValue        
        
        # remove whitespaces from xml
        source_name = re.sub(r'\s+', '', source_name)
        source_location = re.sub(r'\s+', '', source_location)
        source_query_type = re.sub(r'\s+', '', source_query_type)
                
        source_dict['source_name'] = source_name
        source_dict['location'] = source_location
        source_dict['query_type'] = source_query_type
        
        attributes = []        
        attribute_list = s.getElementsByTagName('attr_property')
        for attr in attribute_list:
            field_in_schema = attr.attributes['name'].value
            fields_in_source = attr.childNodes[0].nodeValue
            field_in_schema = re.sub(r'\s+', '', field_in_schema)
            fields_in_source = re.sub(r'\s+', '', fields_in_source)                                                
            attributes.append(field_in_schema)
            attributes.append(fields_in_source)        
            source_dict['attributes'] =  attributes
        sources_all.append(source_dict)
    
    # Print the sources 
    print 'Sources: '
    for source in sources_all:
        print json.dumps(source, indent=2)
        
    return sources_all   
        
def parse_movieName_http_api(in_title):
    return re.sub(' \(film\)$', '', in_title).replace (" ", "+")
     
        
def build_http_api_request_film(in_location, in_title):
    p_movie_type = 'movie'
    p_year = ""
    p_plot = 'full'
    in_title = parse_movieName_http_api(in_title)
    #return 'http://www.omdbapi.com/?t=' + in_title + '&y=' + p_year + '&plot=full' +  '&r=json'            #+ '&type=movie'
    return in_location + '/?t=' + in_title + '&y=' + p_year + '&plot=full' +  '&r=json'            #+ '&type=movie'
      
def sourceField_2_globalSchemaURI(in_source_item, in_source_name, in_metadata_mappings, in_metadata_content):
    Global_schema_Field_URI = SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA
    #print in_source_item
    #print in_source_name
    # HOW TO GET THE URI:
    # take in_source_item
    # go to metadata_content, take dictionary corresponding to source_name 
    # take list of elements from attributes key 
    # pos = in_source_item
    # get element in pos-1. That is the field in global schema 
    
    for source in in_metadata_content:                   # list of dictionaries
        if source['source_name'] == in_source_name:     
            if in_source_item in source['attributes']:
                #print 'searching for :' + in_source_item
                pos = source['attributes'].index(in_source_item)
                #print 'source[attributes] = ' + str(source['attributes'])
                #print 'POS: '+ str(pos)
                global_schema_field = source['attributes'][pos-1]
                #print 'FOUND SOURCE FOUND ITEM!!!', global_schema_field
                Global_schema_Field_URI = in_metadata_mappings[global_schema_field]
                #print 'FOUND SOURCE FOUND ITEM!!!' + global_schema_field + '------->' + Global_schema_Field_URI
    #print Global_schema_Field_URI
    return Global_schema_Field_URI                
        
def HTTP_API_SOURCE_request_film_info(in_film_to_search, in_source, in_metadata_mappings, in_metadata_content):
    #print '------ SOURCE NAME:         ', in_source['source_name']
    #print '       SOURCE LOCATION:     ', in_source['location']    
    #print '       SOURCE QUERY TYPE:   ', in_source['query_type']
    #print '       SOURCE ATTRIBUTES:   ', in_source['attributes']
    
    # get information from the source (WEB CONNECTION)
    http_api_conn = http_api_request_connector()
    print in_source['location']
    print in_film_to_search
    request_string = build_http_api_request_film(in_source['location'], in_film_to_search)
    print request_string
    json_movie_result = http_api_conn.getMovie_Information(request_string)
    #print '\nHTTP_API_request RESPONSE: ..... '
        # print json_movie_result
    #print json.dumps(json_movie_result, indent=2)


    # get mapping (source_field_name -> global_schema_field_name)            
    #print '\nMAPPING WITH METADATA: ..... '
    all_triples = []
    for source_item in json_movie_result:
        #print 'Source Item Name:  ', source_item
        #print 'Source Item Value: ', json_movie_result[source_item]
        source_item_value = json_movie_result[source_item]
        property_URI = sourceField_2_globalSchemaURI(source_item, in_source['source_name'], in_metadata_mappings, in_metadata_content)
        
        if property_URI != SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA:     
            # build single tripple and append it with the others
            single_triple = []
            film_URI = in_film_to_search.replace(' ', '_') # URI crashes in SPARQL is blanks exist
            film_URI += '_' + in_source['source_name']
            single_triple.append(film_URI)
            single_triple.append(property_URI)
            single_triple.append(source_item_value)
            all_triples.append(single_triple)
            # all_triples contains all triples (each is lists of 3 elements) from the HTTP source
    #print 'PRINTING ALL RESULTING TRIPLES'
    #print all_triples                           
    #print all_triples[1]
    #for i in range(0,len(all_triples)): 
    #    print all_triples[i][0] + ' - ' + all_triples[i][1] + ' - ' + all_triples[i][2]

    # if movie was not found, return empty list    
    return all_triples
    
def TWITTER_SOURCE_request_film_info (in_film_to_search, in_source, in_metadata_mappings, in_metadata_content):
    print 'FIRING TWITTER'
    
    # get information from the source (WEB CONNECTION)
    twitter_secureLoad_conn = twitter_patternPkg_connector ()
    print in_source['location']
    print in_film_to_search
    
    
    # get a lit of Jsons or dictionaries
    print '\nTWEETER RESPONSE: ... '
    json_twitters_movie_result = twitter_secureLoad_conn.getTweetSecureLoad(in_film_to_search)
        # print json_movie_result
    
    #print 'PRINTING TWEET RESTLS IN TWITTER_SOURCE_request_film_info'
    all_triples = []
    for i in range(0,len(json_twitters_movie_result)): 
        #print '-- TWITTER: '+ str(i)
        tweet_json = json_twitters_movie_result[i]
        #print tweet_json['tweet_id']
        #print tweet_json['created_at']
        #print tweet_json['topic']         # tweet_json['movie_name']
        #print tweet_json['text']
        #print len(tweet_json)
        #print '\n'  

        # get mapping (source_field_name -> global_schema_field_name)            
        #print 'MAPPING WITH METADATA: ..... (TWITTER_SOURCE_request_film_info)'
        
        for source_item in tweet_json:
            #print '\n-- -- TWITTER: '+ str(i) + ' - Accessing each secton in 1 tweeter'
            #print 'Source Item Name:  ', source_item
            #print 'Source Item Value: ', tweet_json[source_item]
            
            source_item_value = tweet_json[source_item]
            
            property_URI = sourceField_2_globalSchemaURI(source_item, in_source['source_name'], in_metadata_mappings, in_metadata_content)
            #print 'property URI was: '+ property_URI
            #print in_film_to_search
            
            if property_URI != SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA:     
                # build single tripple and append it with the others
                single_triple = []
                film_URI = in_film_to_search.replace(' ', '_') # URI crashes in SPARQL is blanks exist
                film_URI += '_' + in_source['source_name']
                single_triple.append(film_URI)
                single_triple.append(property_URI)
                single_triple.append(source_item_value)
                all_triples.append(single_triple)
                # all_triples contains all triples (each is lists of 3 elements) from the HTTP source
        
        #print '\nPRINTING ALL RESULTING TRIPLES'
        #print all_triples    
                               
    #print all_triples[1]
    #for i in range(0,len(all_triples)): 
    #    print all_triples[i][0] + ' - ' + all_triples[i][1] + ' - ' + all_triples[i][2]

    # if movie was not found, return empty list    
    return all_triples

def VIRTUOSO_request_movies_byCity(in_city_name):

    print '\n' + '*'*40    
    print 'VIRTUOSO_request_movies_byCity'
    print '*'*40    
        
    query = 'SELECT ?s ?p ?o\n'
    query += 'WHERE {\n'
    query += '?s ?p ?o .\n'
    query += '?s <https://schema.org/event> ?o .\n' 
    query += '?s <https://schema.org/location> ?address .\n'
    query += 'FILTER regex(?address, "' + in_city_name + '" )\n'
    query += '}'
    
    print query
    
    triples = virtuosoConnector.query(query)
    
    return triples
    


def GMS_SOURCE_request_movies_info(in_city_name, in_source, in_metadata_mappings, in_metadata_content):
    
    print '\n' + '*'*40    
    print 'GMS_SOURCE_request_movies_info'
    print '*'*40    
    
    gmsConnector = GoogleMovieShowtimes(in_city_name)
    triples = []
        
    results = gmsConnector.parse()
    
    for k, v in results.iteritems():
        for theater in v:            
            theaterURI = theater['name'].replace(' ', '_') + "_GMS_source"            
            property_URI = sourceField_2_globalSchemaURI(k, in_source['source_name'], in_metadata_mappings, in_metadata_content)
            if property_URI != SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA:
                triple = []            
                triple.append(theaterURI)
                triple.append("rdf:type")
                triple.append(property_URI)
                triples.append(triple)
                print triple                            
            for kt, vt in theater.iteritems():
                if type(vt) is not list:
                    property_URI = sourceField_2_globalSchemaURI(kt, in_source['source_name'], in_metadata_mappings, in_metadata_content)
                    if property_URI != SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA:
                        triple = []
                        triple.append(theaterURI)
                        triple.append(property_URI)
                        triple.append(vt)
                        triples.append(triple)
                        print triple
                else: 
                    for movie in vt:
                        for km, vm in movie.iteritems():                                
                                property_URI = sourceField_2_globalSchemaURI(km, in_source['source_name'], in_metadata_mappings, in_metadata_content)
                                if property_URI != SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA:
                                    triple = []
                                    triple.append(theaterURI)
                                    triple.append(property_URI)
                                    triple.append(vm)
                                    triples.append(triple)                                                                        
                                    print triple

    result = virtuosoConnector.insert(triples, RDF_GRAPH_RECOMMENDER)                

    return result


def DBPEDIA_SOURCE_request_film_info (in_movie_uri, in_source, in_metadata_mappings, in_metadata_content):        
    
    dbpedia = DBpedia()
        
    triples = []
    result = dbpedia.getResource(in_movie_uri)        
    for row in result:
        
            source_item = row[0].value
                                    
            property_URI = sourceField_2_globalSchemaURI(source_item, in_source['source_name'], in_metadata_mappings, in_metadata_content)
            
            if property_URI != SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA:
                source_item_value = row[1].value     
                single_triple = []
                single_triple.append(in_movie_uri)
                single_triple.append(property_URI)
                single_triple.append(source_item_value)
                triples.append(single_triple)
            
    return triples

        
       
def print_movie_triples(in_film_all_triples):
    print 'PRINTING ALL RESULTING TRIPLES'
    print in_film_all_triples                           
    print in_film_all_triples[1]
    for i in range(0,len(in_film_all_triples)): 
        print in_film_all_triples[i][0] + ' - ' + in_film_all_triples[i][1] + ' - ' + in_film_all_triples[i][2]
    
def print_film_not_found(in_film_to_search, in_source_name):
    #print '\n' + '*'*40
    print 'FILM not found in source: (FILM: ' + in_film_to_search + ', SOURCE: ' + in_source_name +')'
    #print '*'*40
    
def print_metadata_not_found(in_film_to_search, in_source_name):
    #print '\n' + '*'*40
    print 'METADATA not found in source: (FILM: ' + in_film_to_search + ', SOURCE: ' + in_source_name +')'
    #print '*'*40    
 
def JAIME_wants_to_load_his_file_with_100_films(metadata_mappings, metadata_content):
    print '\n\n\n' + '*'*40
    print '*'*40
    print 'JAIME SEARCHING & LOADING  100 FILMS (according to METADATA).... '
    print '*'*40
    print '*'*40
    
    workingDir=os.getcwd() +'\\'
    MOVIES_TO_BE_LOAD_INITIAL_LOAD = 'OD_100_MOVIE_NAMES_DBpedia.txt'
    f_movie_1stload_name = workingDir + MOVIES_TO_BE_LOAD_INITIAL_LOAD
    print '\n\n- FILE NAME (100 MOVIES): ' +f_movie_1stload_name

    f_1stload_movie = codecs.open(f_movie_1stload_name, "r", "utf-8")
    film_to_search = f_1stload_movie.readline()
    film_to_search = film_to_search[0: (len(film_to_search) -2)] # get rid of \r\n
    print 'READING FROM FILE: ' + film_to_search    
        
    while film_to_search != '':    
        #film_to_search = film_to_search[0: (len(film_to_search) -2)] # get rid of \r\n
        for source in metadata_content:
            if source['query_type'] == 'HTTP_API_request' :            # this is basically omdb
                print '\n\nHTTP_API_request (' + source['source_name'] +'): ....................\n\n'
                # get all triples from film in source
                film_all_triples = []
                film_all_triples = HTTP_API_SOURCE_request_film_info(film_to_search, source, metadata_mappings, metadata_content)
                if len(film_all_triples) > 0:
                    # print_movie_triples(film_all_triples)
                    # load into virtuoso
                    virtuoso_conn.insert_triples_movie_info (film_to_search, film_all_triples, RDF_GRAPH_RECOMMENDER, source['source_name'])
                else:
                    print_film_not_found(film_to_search, source['source_name'])
        
            if (source['query_type']) == 'TWITTER_Topic':
                print '\n\nTWITTER RESPONSE: ..................................................\n\n'
                # get all triples from film in source
                film_all_triples = []
                film_all_triples = TWITTER_SOURCE_request_film_info(film_to_search, source, metadata_mappings, metadata_content)
                #print 'LONGITUD: ' +  str(len(film_all_triples))
                #print 'ALL TWITTER TRIPPLES: ' + str(film_all_triples)
                if len(film_all_triples) > 0:
                    # print_movie_triples(film_all_triples)
                    # load into virtuoso
                    virtuoso_conn.insert_triples_movie_info (film_to_search, film_all_triples, RDF_GRAPH_RECOMMENDER, source['source_name'])
                else:
                    print_film_not_found(film_to_search, source['source_name'])
                 
                    
        film_to_search = f_1stload_movie.readline()
        film_to_search = film_to_search[0: (len(film_to_search) -2)] # get rid of \r\n
        print '\n\nREADING FROM FILE: ' + film_to_search    
    



        
def main():
    
    ##########################################################################
    # global variables
    
    #global SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA 
    global SPARQL_ENDPOINT
    global RDF_GRAPH_RECOMMENDER
    global virtuoso_conn
    global dbpedia 
   
    ##########################################################################
    # initialize variables
    
    SPARQL_ENDPOINT = "http://localhost:8890/sparql"
    METADATA = 'METADATA_recommender.xml'
    metadata_content  = [] # list of dictionaries
    metadata_mappings = {} # dictionary with mappings of global concept with URI
    RDF_GRAPH_RECOMMENDER = 'OD_RDF_Graph_Recommender' 
    movies = [] # list of movies
    
    ##########################################################################
    # read metadata
    metadata_content = readMetadata_sources(METADATA)
    metadata_mappings = readMetadata_mappings(METADATA)

    ##########################################################################
    # initialize virtuoso
    virtuoso_conn = VirtuosoConnector()

    ##########################################################################
    # Name of movie to get information    
    print '\n' + '*'*40
    film_to_search = 'War of the Worlds'    
    print 'FILM TO SEARCH: ' + film_to_search
    print '*'*40

    # This will use the name of the movie provided by the user
    # to get all the dbpedia_resultspossible URIs that match.
    dbpedia = DBpedia()
    dbpedia_results = dbpedia.getURIs(film_to_search)  
    
    print '\n' + '*'*40
    print 'URIs / NAMES FOUND ON DBPEDIA:'
    print '*'*40     

    for row in dbpedia_results:
        values = sparql.unpack_row(row)
        movies.append((values[0], values[1]))
        
    for movie in movies:                       
        print 'URI: ' + movie[0]
        print 'Name: ' + movie[1]
        
    ##########################################################################
    # start searching in all sources 1 MOVIE
    
    print '\n' + '*'*40
    print 'STARTING TO LOAD DATA (according to METADATA)... '
    print '*'*40
    
    
    # search that film in ALL SOURCES
    for source in metadata_content:
        if source['query_type'] == 'HTTP_API_request' :            # this is basically omdb
            print '\n' + '*'*40
            print 'HTTP API request (' + source['source_name'] +'): ...'
            print '*'*40
            # get all triples from film in source
            film_all_triples = []
            film_all_triples = HTTP_API_SOURCE_request_film_info(film_to_search, source, metadata_mappings, metadata_content)
            if len(film_all_triples) > 0:
                # print_movie_triples(film_all_triples)
                # load into virtuoso
                virtuoso_conn.insert_triples_movie_info (film_to_search, film_all_triples, RDF_GRAPH_RECOMMENDER, source['source_name'])
            else:
                print_film_not_found(film_to_search, source['source_name'])

        if (source['query_type']) == 'TWITTER_Topic':
            print '\n' + '*'*40
            print 'TWITTER request (' + source['source_name'] +'): ...'
            print '*'*40
            # get all triples from film in source
            film_all_triples = []
            film_all_triples = TWITTER_SOURCE_request_film_info(film_to_search, source, metadata_mappings, metadata_content)
            #print 'ALL TWITTER TRIPPLES: ' + str(film_all_triples)
            if len(film_all_triples) > 0:
                # print_movie_triples(film_all_triples)
                # load into virtuoso
                virtuoso_conn.insert_triples_movie_info (film_to_search, film_all_triples, RDF_GRAPH_RECOMMENDER, source['source_name'])
            else:
                print_film_not_found(film_to_search, source['source_name'])
            
        if (source['query_type']) == 'SQL':
            print '\n' + '*'*40
            print 'MOVIELENS request (' + source['source_name'] +'): ... (To be implemented)'
            print '*'*40
            # IMPLEMENT THIS
            
        if (source['query_type']) == 'SPARQL':
            print '\n' + '*'*40
            print 'SPARQL request (' + source['source_name'] +'): ...'
            print '*'*40
            for movie in movies:                       
                movie_uri = movie[0]
                movie_name = movie[1]
                
                # get all triples from film in source
                movie_triples = []
                movie_triples = DBPEDIA_SOURCE_request_film_info(movie_uri, source, metadata_mappings, metadata_content)
                if len(movie_triples) > 0:
                    # print_movie_triples(film_all_triples)
                    # load into virtuoso
                    virtuoso_conn.insert_triples_movie_info (movie_uri, movie_triples, RDF_GRAPH_RECOMMENDER, source['source_name'])
                else:
                    print_metadata_not_found(movie_name, source['source_name'])
                                                
            
        if (source['query_type']) == 'HTTP_request':
            print '\n' + '*'*40
            print 'GOOGLE MOVIE SHOWTIMES request (' + source['source_name'] +'): ... (To be implemented)'
            print '*'*40
            # IMPLEMENT THIS
            
    print '\n' + '*'*40
    print 'DATA LOAD FINISHED FOR: ' + film_to_search
    print '*'*40
    

    ##########################################################################
    # jaime taking 100 movie names, getting all their information from the sources + into virtuoso
    
    if 1 == 2:
        # THIS IS JUST FOR JAIME TO TEST BIG LOADS into virtuoso
        JAIME_wants_to_load_his_file_with_100_films(metadata_mappings, metadata_content)


    
if __name__ == '__main__':
    logging.basicConfig()
    main()
