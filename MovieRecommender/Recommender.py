'''
Created on 14/04/2015

@author: cdiaz jandres
'''

import re, math
import string
import json
from collections import Counter
from xml.dom import minidom
import sys
import os
import codecs

    

from connectors.omdb.omdb_connector import omdb_connector
from connectors.http_api_request.http_api_request_connector import http_api_request_connector
from connectors.virtuoso.virtuoso_connector import virtuoso_connector

from connectors.twitter.twitter_patternPkg_connector import twitter_patternPkg_connector
from connectors.twitter.twitter_streaming_connector import twitter_streaming_connector


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
    #print json.dumps(global_concept_mappings_dict, indent=2)

    print json.dumps(global_concept_mappings_dict, indent=2)
    print 'Mappings done'
    return global_concept_mappings_dict
    



# returns a list of dictionaries
#[{'query_type': u'HTTP_API_request', 'source_name': u'OMBD_source', 'location': u'http://www.omdbapi.com/', 'attributes': [u'movie_name', u'Title', u'movie_desc', u'Plot', u'movie_genre', u'Genre', u'movie_director', u'Director', u'movie_author', u'Writer', u'movie_actor', u'Actors', u'movie_year', u'Year', u'movie_runtime', u'Runtime', u'movie_language', u'Language', u'movie_country', u'Country', u'movie_awards', u'Awards', u'movie_rating', u'imdbRating', u'movie_poster', u'Poster']}, {'query_type': u'SPARQL', 'source_name': u'DBPEDIA_source', 'location': u'http://dbpedia.org/page/', 'attributes': [u'movie_name', u'name', u'movie_desc', u'description', u'movie_genre', u'genre', u'movie_director', u'director', u'movie_author', u'author', u'movie_actor', u'actor', u'movie_year', u'copyrightYear', u'movie_runtime', u'duration', u'movie_studio', u'productionCompany', u'movie_language', u'inLanguage', u'movie_country', u'Country', u'movie_budget', u'budget', u'movie_comment', u'comment', u'movie_awards', u'award', u'movie_rating', u'aggregateRating', u'movie_poster', u'image']}, {'query_type': u'TOPIC', 'source_name': u'TWITTER_source', 'location': u'TWEEPY_Python', 'attributes': [u'information_date', u'created_at', u'movie_name', u'movie_name', u'user_name', u'user_name', u'movie_comment', u'text']}, {'query_type': u'HTTP_request', 'source_name': u'GOOGLE_showtimes_source', 'location': u'https://www.google.es/movies', 'attributes': [u'information_date', u'update_date', u'cinema_name', u'cinema', u'cinema_location', u'location', u'movie_name', u'movie_name']}, {'query_type': u'SQL', 'source_name': u'MOVIELENS_source', 'location': u'https://localhost:8080', 'attributes': [u'movie_name', u'Title', u'movie_genre', u'Genre', u'user_gender', u'Title', u'user_age', u'Plot', u'user_occupation', u'Genre', u'user_zipcode', u'Title', u'user_rating', u'Rating', u'information_Date', u'Timestamp']}]
def readMetadata_sources(metadata_file):
    print '\n' + '*'*40
    print 'PROCEEDING TO READ METADATA (SOURCES):'
    print '*'*40
    workingDir=os.getcwd()
    print 'Working Dir: ', workingDir
    print 'Full Path Metadata File: ' + workingDir + '\\' + metadata_file
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
    
    print sources_all
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
                pos = source['attributes'].index(in_source_item)
                global_schema_field = source['attributes'][pos-1]
                # print 'FOUND SOURCE FOUND ITEM!!!', global_schema_field
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
    print '\nHTTP_API_request RESPONSE: ..... '
        # print json_movie_result
    #print json.dumps(json_movie_result, indent=2)


    # get mapping (source_field_name -> global_schema_field_name)            
    print '\nMAPPING WITH METADATA: ..... '
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
    
def print_movie_triples(in_film_all_triples):
    print 'PRINTING ALL RESULTING TRIPLES'
    print in_film_all_triples                           
    print in_film_all_triples[1]
    for i in range(0,len(in_film_all_triples)): 
        print in_film_all_triples[i][0] + ' - ' + in_film_all_triples[i][1] + ' - ' + in_film_all_triples[i][2]
    
def print_film_not_found(in_film_to_search, in_source_name):
    print '\n' + '*'*40
    print 'FILM not found in source: (FILM: ' + in_film_to_search + ', SOURCE: ' + in_source_name +')'
    print '*'*40
 
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
        
    while film_to_search != '':    
        film_to_search = film_to_search[0: (len(film_to_search) -2)] # get rid of \r\n
        print '\n\n- Searching Movie: ' + film_to_search + '\n'
        for source in metadata_content:
            if source['query_type'] == 'HTTP_API_request' :            # this is basically omdb
                # get all triples from film in source
                film_all_triples = []
                film_all_triples = HTTP_API_SOURCE_request_film_info(film_to_search, source, metadata_mappings, metadata_content)
                if len(film_all_triples) > 0:
                    # print_movie_triples(film_all_triples)
                    # load into virtuoso
                    virtuoso_conn.insert_triples_movie_info (film_to_search, film_all_triples, RDF_GRAPH_RECOMMENDER, source['source_name'])
                else:
                    print_film_not_found(film_to_search, source['source_name'])
        
        film_to_search = f_1stload_movie.readline()




        
def main():

    ##########################################################################
    # global variables
    
    global SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA 
    global SPARQL_ENDPOINT
    global RDF_GRAPH_RECOMMENDER
    global virtuoso_conn
    
    ##########################################################################
    # initialize variables
    
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
    virtuoso_conn = virtuoso_connector()

    ##########################################################################
    # Name of movie to get information
    
    print '\n' + '*'*40
    film_to_search = '1941'
                        # film_to_search = 'Ni de conia la encuentro'
                        ######## chindler's list WILL FAIL!!!!   ########
    print 'FILM TO SEARCH: ' + film_to_search
    print '*'*40

    ##########################################################################
    # start searching in all sources 1 MOVIE
    
    print '\n' + '*'*40
    print 'FILM SEARCHING (according to METADATA).... '
    print '*'*40
    
    
    # search that film in ALL SOURCES
    for source in metadata_content:
        if source['query_type'] == 'HTTP_API_request' :            # this is basically omdb
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
            print 'TWITTER RESPONSE: .....            (To be implemented)'
            # IMPLEMENT THIS
            
        if (source['query_type']) == 'SQL':
            print 'MOVIELENS RESPONSE: .....          (To be implemented)'
            # IMPLEMENT THIS
            
        if (source['query_type']) == 'SPARQL':
            print 'DBPEDIA RESPONSE: .....            (To be implemented)'
            # IMPLEMENT THIS
            
        if (source['query_type']) == 'HTTP_request':
            print 'GOOGLE SHOWTIMES RESPONSE: .....   (To be implemented)'
            # IMPLEMENT THIS
            

    print 'FINISHED ALL SEARCHES: ' + film_to_search
    
    
    ##########################################################################
    # jaime taking 100 movie names, getting all their information from the sources + into virtuoso
    if 1 == 2:
        # THIS IS JUST FOR JAIME TO TEST BIG LOADS into virtuoso
        JAIME_wants_to_load_his_file_with_100_films(metadata_mappings, metadata_content)


    ################################################## JAIME END 

if __name__ == '__main__':
    main()
