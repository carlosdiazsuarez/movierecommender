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
import urllib
from xml.dom import minidom

from connectors.dbpedia.DBpedia import DBpedia
from connectors.googlemoviesshowtimes.gms_connector import GoogleMovieShowtimes
from connectors.http_api_request.http_api_request_connector import http_api_request_connector
from connectors.imdb.imdb_connector import IMDB
from connectors.twitter.twitter_patternPkg_connector import twitter_patternPkg_connector
from connectors.virtuoso.virtuoso_connector import VirtuosoConnector


##########################################################################
# initialize virtuoso
RDF_GRAPH_RECOMMENDER = 'OD_RDF_Graph_Recommender' 

# Connectors
virtuosoConnector = VirtuosoConnector()
dbpedia = DBpedia()
imdbConnector = IMDB()

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


def OMDB_request_movie_info(in_film_to_search, in_source, in_metadata_mappings, in_metadata_content):
    
    print '\n' + '*'*40    
    print 'OMDB_request_movie_info'
    print '*'*40
    
    for source in in_metadata_content:
        if source['source_name'] == in_source:
            location = source['location']
    
    http_api_conn = http_api_request_connector()
    request_string = build_http_api_request_film(location, in_film_to_search)
    json_movie_result = http_api_conn.getMovie_Information(request_string)


    triples = []
    for source_item in json_movie_result:
        source_item_value = json_movie_result[source_item]
        property_URI = sourceField_2_globalSchemaURI(source_item, in_source, in_metadata_mappings, in_metadata_content)
        
        if property_URI != SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA:     

            triple = []
            film_URI = in_film_to_search.replace(' ', '_') # URI crashes in SPARQL is blanks exist
            film_URI += '_' + in_source
            triple.append(film_URI)
            triple.append(property_URI)
            triple.append(source_item_value)
            triples.append(triple)

    result = 0            
    if len(triples) > 0: 
        result = virtuosoConnector.insert_triples_movie_info(in_film_to_search, triples, RDF_GRAPH_RECOMMENDER, in_source)                
                            
    return result
               
        
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
    
def TWITTER_SOURCE_request_movie_info (in_film_to_search, in_source, in_metadata_mappings, in_metadata_content):
    
    print '\n' + '*'*40    
    print 'TWITTER_SOURCE_request_movie_info'
    print '*'*40
        
    print 'FIRING TWITTER'
    
    for source in in_metadata_content:
        if source['source_name'] == in_source:
            location = source['location']
    
    twitter_secureLoad_conn = twitter_patternPkg_connector ()
    
    print '\nTWITTER RESPONSE: ... '
    json_twitters_movie_result = twitter_secureLoad_conn.getTweetSecureLoad(in_film_to_search)
    all_triples = []
    for i in range(0,len(json_twitters_movie_result)): 
        tweet_json = json_twitters_movie_result[i]
        
        for source_item in tweet_json:
            
            source_item_value = tweet_json[source_item]
            
            property_URI = sourceField_2_globalSchemaURI(source_item, in_source, in_metadata_mappings, in_metadata_content)
            
            if property_URI != SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA:     
                single_triple = []
                film_URI = in_film_to_search.replace(' ', '_')
                film_URI += '_' + in_source
                single_triple.append(film_URI)
                single_triple.append(property_URI)
                single_triple.append(source_item_value)
                all_triples.append(single_triple)
                
    result = 0            
    if len(all_triples) > 0:     
        result = virtuosoConnector.insert(all_triples, RDF_GRAPH_RECOMMENDER)

    return result

def IMDB_search_movies_byName(in_name, in_metadata_mappings, in_metadata_content):
    
    print '\n' + '*'*40    
    print 'IMDB_search_movies_byName'
    print '*'*40
    
    titles = []
    titles.append(in_name)
    
    ''' Search orginal title '''
    ids =  imdbConnector.getIDs(in_name)        
    for id in ids:
        results = imdbConnector.getTitleExtra(id)
        for result in results:
            titles.append(result)        
    
    ''' Eliminamos las repeticiones '''
    titles = list(set(titles))
    
    print 'IMDB has return the following titles'
    for title in titles:
        print title
        
    movies = []    
    for title in titles: 
        results = DBPEDIA_request_movie_byName(title, in_metadata_mappings, in_metadata_content)
        for result in results:
            movies.append(result)
            
    ''' Eliminamos las repeticiones '''
    movies = list(set(movies))

    triples = []
    for movie in movies:
        triple = []
        triple.append(movie[0])
        triple.append('https://schema.org/alternateName')
        triple.append(in_name)
        triples.append(movie)
        triples.append(triple)             
    
    result = 0            
    if len(triples) > 0:        
        result = virtuosoConnector.insert(triples, RDF_GRAPH_RECOMMENDER)                
                          
    return movies

def DBPEDIA_request_movie_byName(in_name, in_metadata_mappings, in_metadata_content):        
    
    print '\n' + '*'*40    
    print 'DBPEDIA_request_movie_byName'
    print '*'*40
    
    print 'Searching in DBpedia for: ', in_name    

    movies = []   
    # This will use the name of the movie provided by the user
    # to get all the dbpedia_resultspossible URIs that match.    
    dbpedia_results = dbpedia.getURIs(in_name)  
    
    property_URI = sourceField_2_globalSchemaURI('http://xmlns.com/foaf/0.1/name', 'DBPEDIA_source', in_metadata_mappings, in_metadata_content)
    if property_URI != SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA:    
        for row in dbpedia_results:
            values = sparql.unpack_row(row)
            movies.append((values[0], property_URI, values[1]))
            
        for movie in movies:                                   
            print movie
    
    return movies

def VIRTUOSO_request_theaters_byCity(in_city_name):

    print '\n' + '*'*40    
    print 'VIRTUOSO_request_theaters_byCity'
    print '*'*40    
        
    query = 'SELECT ?s ?p ?o\n'
    query += 'WHERE {\n'
    query += '?s ?p ?o .\n'
    query += '?s <https://schema.org/name> ?o .\n'
    query += '?s <rdf:type> <https://schema.org/MovieTheater> .\n' 
    query += '?s <https://schema.org/location> ?address .\n'
    query += 'FILTER contains(?address, "' + in_city_name + '" )\n'
    query += '}'
        
    triples = virtuosoConnector.query(query)
    
    return triples

def VIRTUOSO_request_movies_byAlternateName(in_alternateName):

    print '\n' + '*'*40    
    print 'VIRTUOSO_request_movies_byAlternateName'
    print '*'*40    
        
    query = 'SELECT ?s ?p ?name\n'
    query += 'WHERE {\n'
    query += '?s <https://schema.org/name> ?name .\n'
    query += '?s <https://schema.org/alternateName> ?o .\n'
    query += 'FILTER contains(?o , "' + in_alternateName + '" )\n'
    query += '}'
        
    results = virtuosoConnector.query(query)
    
    for result in results:
        print result
    
    return results


def VIRTUOSO_request_tweets_byName(in_name, in_source_name):
        
    print '\n' + '*'*40    
    print 'VIRTUOSO_request_tweets_byName'
    print '*'*40    
        
    query = 'SELECT ?s ?p ?o\n'
    query += 'WHERE {\n'
    query += '?s <https://schema.org/comment>  ?o .\n'
    query += 'FILTER contains( str(?s), "' + in_name.replace(' ','_') + '_' + in_source_name + '" )\n'
    query += '}'
        
    results = virtuosoConnector.query(query)
    
    for result in results:
        print result
    
    return results

def VIRTUOSO_entity_resolution_byName(in_name, in_source_name):
        
    print '\n' + '*'*40    
    print 'VIRTUOSO_request_movies_byName'
    print '*'*40    
        
    query = 'SELECT ?s ?p ?o\n'
    query += 'WHERE {\n'
    query += '?s ?p ?o .\n'
    query += 'FILTER contains(str(?s), "' + in_name.replace(' ','_') + '_' + in_source_name + '" )\n'
    query += '}'
        
    results = virtuosoConnector.query(query)
    
    for result in results:
        print result
    
    return results
        

def VIRTUOSO_request_movie_info_byURI(in_URI):

    print '\n' + '*'*40    
    print 'VIRTUOSO_request_movies_byURI'
    print '*'*40    
        
    query = 'SELECT ?s ?p ?o\n'
    query += 'WHERE {\n'
    query += '?s ?p ?o .\n'
    query += 'FILTER (?s = <' + in_URI + '> )\n'    
    query += '}'        
    results = virtuosoConnector.query(query)
    
    for result in results:
        print result
    
    return results


def VIRTUOSO_request_movies_byCity(in_city_name):

    print '\n' + '*'*40    
    print 'VIRTUOSO_request_movies_byCity'
    print '*'*40    
        
    query = 'SELECT ?s ?p ?o\n'
    query += 'WHERE {\n'
    query += '?s ?p ?o .\n'
    query += '?s <https://schema.org/event> ?o .\n' 
    query += '?s <https://schema.org/location> ?address .\n'
    query += 'FILTER contains(?address, "' + in_city_name + '" )\n'
    query += '}'
       
    triples = virtuosoConnector.query(query)
    
    #Remove what is not part of the name
    for triple in triples:
        triple[2] = triple[2].replace('(Digital)', '')
        triple[2] = triple[2].replace('(Dig)', '')
        triple[2] = triple[2].replace('3D', '')
        triple[2] = triple[2].replace('V.O.S.E.', '')
        triple[2] = triple[2].replace('V.O.S.', '')
        
    return triples
    


def GMS_request_movies_info(in_city_name, in_source, in_metadata_mappings, in_metadata_content):
    
    print '\n' + '*'*40    
    print 'GMS_request_movies_info'
    print '*'*40    
    
    gmsConnector = GoogleMovieShowtimes(in_city_name)
    triples = []
        
    results = gmsConnector.parse()
    
    for k, v in results.iteritems():
        for theater in v:            
            theaterURI = theater['name'].replace(' ', '_') + "_GMS_source"            
            property_URI = sourceField_2_globalSchemaURI(k, in_source, in_metadata_mappings, in_metadata_content)
            if property_URI != SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA:
                triple = []            
                triple.append(theaterURI)
                triple.append("rdf:type")
                triple.append(property_URI)
                triples.append(triple)
                print triple                            
            for kt, vt in theater.iteritems():
                if type(vt) is not list:
                    property_URI = sourceField_2_globalSchemaURI(kt, in_source, in_metadata_mappings, in_metadata_content)
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
                                property_URI = sourceField_2_globalSchemaURI(km, in_source, in_metadata_mappings, in_metadata_content)
                                if property_URI != SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA:
                                    triple = []
                                    triple.append(theaterURI)
                                    triple.append(property_URI)
                                    triple.append(vm)
                                    triples.append(triple)                                                                        
                                    print triple

    result = 0            
    if len(triples) > 0: 
        result = virtuosoConnector.insert(triples, RDF_GRAPH_RECOMMENDER)                

    return result

def VIRTUOSO_insert_user_interaction(in_userId, in_movie_uri):
    
    print '\n' + '*'*40    
    print 'VITUOSO_insert_user_interaction'
    print '*'*40
    
    triples = []
    
    triple = []
    triple.append('USER_'+str(in_userId))
    triple.append('https://schema.org/UserInteraction')
    triple.append(in_movie_uri)
    triples.append(triple)
    
    result = 0            
    if len(triples) > 0:     
        result = virtuosoConnector.insert(triples, RDF_GRAPH_RECOMMENDER)                
                            
    return result

def DBPEDIA_request_movie_info (in_movie_uri, in_source, in_metadata_mappings, in_metadata_content):        
           
    print '\n' + '*'*40    
    print 'DBPEDIA_request_movie_info'
    print '*'*40                   
           
    triples = []                
    results = dbpedia.getResource(in_movie_uri)                                                    
    for row in results:        
            source_item = row[0].value                            
            property_URI = sourceField_2_globalSchemaURI(source_item, in_source, in_metadata_mappings, in_metadata_content)            
            if property_URI != SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA:
                source_item_value = row[1].value     
                single_triple = []
                single_triple.append(in_movie_uri)
                single_triple.append(property_URI)
                single_triple.append(source_item_value)
                triples.append(single_triple)
                        
    result = 0            
    if len(triples) > 0:     
        result = virtuosoConnector.insert(triples, RDF_GRAPH_RECOMMENDER)                
                            
    return result

        
       
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
 
def content_based_recommender(in_movie_uri, in_metadata_mappings, in_metadata_content):
    
    print '\n' + '*'*40    
    print 'CONTENT BASED RECOMMENDER (content_based_recommender)'
    print '*'*40  
    
    results = VIRTUOSO_request_movie_info_byURI(in_movie_uri)
    
    for result in results:
        if result[1] == "https://schema.org/director":
            q = ("""
                SELECT ?s ?o
                WHERE {
                     ?s <http://xmlns.com/foaf/0.1/name> ?o .
                     ?s <http://dbpedia.org/ontology/director> <""" + result[2] + """> .
                }
            """)
            results = dbpedia.query(q)
            
            for result in results:
                try:
                    print "\nCONTENT BASED RECOMMENDATION:"
                    print result
                    DBPEDIA_request_movie_info(result[0].value, 'DBPEDIA_source', in_metadata_mappings, in_metadata_content)
                    OMDB_request_movie_info(result[1].value, 'OMDB_source', in_metadata_mappings, in_metadata_content)
                except:
                    print "Unexpected error:", sys.exc_info()[0]
                    pass                      
    return

def VIRTUOSO_request_user_based_recommender_byMovieNames(in_triples, in_metadata_mappings, in_metadata_content):
    print '\n' + '*'*40    
    print 'VIRTUOSO_request_user_based_recommender_byMovieNames'
    print '*'*40    

    all_movies = []
    for triple in in_triples:
        movies = DBPEDIA_request_movie_byName(triple[2], in_metadata_mappings, in_metadata_content)
        for movies in movies:
            all_movies.append(movies)
                         
    return all_movies

def VIRTUOSO_request_user_based_recommender_byUserInteraction(in_userId, in_movie_uri):
    print '\n' + '*'*40    
    print 'VIRTUOSO_request_user_based_recommender_byUserInteraction'
    print '*'*40    
        
    username = 'USER_'+in_userId
    
    query = 'SELECT ?s ?p ?o\n'
    query += 'WHERE {\n'
    query += '?s <https://schema.org/UserInteraction> <'+ in_movie_uri + '> .\n' 
    query += '}'
       
    triples = virtuosoConnector.query(query)
    
    final_movies = []
    for triple in triples:
        user = triple[0]
        if user != username:
            movies = []
            query = 'SELECT ?s ?p ?o\n'
            query += 'WHERE {\n'
            query += '<'+ user +'> <https://schema.org/UserInteraction> ?o .\n' 
            query += '}'
            movies = virtuosoConnector.query(query)
            for movie in movies:
                if movie[2] != in_movie_uri:
                    final_movies.append(movie[2])
                    
    final_movies = list(set(final_movies))
                    
    recommendations = []
    for movie_uri in final_movies:
        query = 'SELECT ?s ?p ?o\n'
        query += 'WHERE {\n'
        query += '?s <https://schema.org/name> ?o .\n'
        query += 'FILTER (?s = <' + movie_uri + '>)'
        query += '}'
        results = virtuosoConnector.query(query)
        for recommendation in results:
            recommendations.append(recommendation)
            
    return recommendations
    

def VIRTUOSO_request_content_based_recommendation(in_movie_uri):
    print '\n' + '*'*40    
    print 'VIRTUOSO_request_content_based_recommendation'
    print '*'*40    
        
    query = 'SELECT ?s ?p ?o\n'
    query += 'WHERE {\n'
    query += '<' + in_movie_uri + '> <https://schema.org/director> ?o .\n' 
    query += '}'
       
    triples = virtuosoConnector.query(query)
    
    recommendations = []
    for triple in triples:
        query = 'SELECT ?s ?p ?o\n'
        query += 'WHERE {\n'
        query += '?s <https://schema.org/name> ?o .\n'        
        query += '?s <https://schema.org/director> ?director .\n'
        query += 'FILTER contains(str(?director), "'+  triple[2] + '" )'
        query += '}'
        results = virtuosoConnector.query(query) 
        for result in results:
            if result[0] != in_movie_uri:
                recommendations.append(result)        
            
    return recommendations        