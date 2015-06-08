'''
Created on 14/04/2015

@author: jandres
'''
import codecs
from os.path import os
from pattern.db import json
import re
from xml.dom import minidom

from Recommender import readMetadata_sources, readMetadata_mappings
from connectors.http_api_request.http_api_request_connector import http_api_request_connector
from connectors.movielens.movielens_connector.movielens_connector import movielens_connector
from connectors.twitter.twitter_patternPkg_connector import twitter_patternPkg_connector
from connectors.virtuoso.virtuoso_connector2 import virtuoso_connector
from recommender_algorithm.collaboratory_filtering_UserBased import collaboratory_filtering_UserBased

##########################################################################
# initialize variables
SOURCE_ITEM_NOT_IN_GLOBAL_SCHEMA= 'SOURCE ITEM NOT EXISTING IN GLOBAL SCHEMA'
SPARQL_ENDPOINT = "http://localhost:8890/sparql"
METADATA_FILE = 'METADATA_recommender.xml'
metadata_content  = [] # list of dictionaries
metadata_mappings = {} # dictionary with mappings of global concept with URI
RDF_GRAPH_RECOMMENDER = 'OD_RDF_Graph_Recommender' 
RDF_GRAPH_RECOMMENDER_ANNOTATED = 'OD_RDF_Graph_Recommender_Annotated' 
last_RatingIDNUM_in_System = 0

# returns a dictionary with Virtuoso properties
def readMetadata_SemanticDB(metadata_file):
    print '\n' + '*'*40
    print 'PROCEEDING TO READ METADATA (SEMANTIC DB VIRTUOSO):'
    print '*'*40
    workingDir=os.getcwd()
    print 'Working Dir: ', workingDir
    print 'Full Path Metadata File: ' + workingDir + '\\' + metadata_file
    xmldoc = minidom.parse(metadata_file)    
    semanticDB_branch = xmldoc.getElementsByTagName('SemanticDB_Virtuoso') 
    
    semanticDB_dict = {}
    
    for semanticDB_branch in xmldoc.getElementsByTagName('SemanticDB_Virtuoso'):    
        semanticDB_http_location = semanticDB_branch.getElementsByTagName('http_location')[0].firstChild.nodeValue
        semanticDB_port = semanticDB_branch.getElementsByTagName('port')[0].firstChild.nodeValue
        semanticDB_folder_endPoint = semanticDB_branch.getElementsByTagName('folder_end_point')[0].firstChild.nodeValue
        
    # remove whitespaces from xml
    semanticDB_http_location = re.sub(r'\s+', '', semanticDB_http_location)
    semanticDB_port = re.sub(r'\s+', '', semanticDB_port)
    semanticDB_folder_endPoint = re.sub(r'\s+', '', semanticDB_folder_endPoint)

    semanticDB_dict['http_location'] = semanticDB_http_location
    semanticDB_dict['port'] = semanticDB_port
    semanticDB_dict['folder_end_point'] = semanticDB_folder_endPoint
    
    print semanticDB_dict
    return semanticDB_dict   



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
    
def TWITTER_SOURCE_request_film_info (in_film_to_search, in_source, in_metadata_mappings, in_metadata_content):
    print 'FIRING TWITTER'
    
    # get information from the source (WEB CONNECTION)
    twitter_secureLoad_conn = twitter_patternPkg_connector ()
    print in_source['location']
    print in_film_to_search
     
    # get a lit of Jsons or dictionaries
    print '\nTWEETER RESPONSE: ..... '
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
        
    
def MOVIELENS_SOURCE_Load_from_Disk_TO_Memory_in_triples_format(in_source, in_metadata_mappings, in_metadata_content):
    print 'FIRING MOVIELENS'
    # harcoded name of keys to look in the metadata because Movielens delivers a data file, no JSON or similar
    Reification_RatingMovie_relation = 'USER_RATING_MOVIE_Relation_RatingMovie'
    Reification_RatingUser_relation = 'USER_RATING_MOVIE_Relation_RatingUser'
    Reification_RatingNumber_relation = 'USER_RATING_MOVIE_Relation_RatingNumber'

    workingDir=os.getcwd()
    
    print in_source['location']
    # get information from the source (WEB CONNECTION)
    movielens_conn = movielens_connector (in_source['location'])
    
    print '\nMOVIELENS RESPONSE: ..... '
    movieLens_prefs_UserRatings = {}
    movieLens_prefs_UserRatings = movielens_conn.load_parse_MovieLens2015_UserRatings(workingDir + '/' + in_source['location'])
    # returns a dictionary like this:
    #'Jaime': {'Predator 1': 5, 'Predator 2':4.5},    
    #'Jesulin Ubrique': {'Toros 1': 1, 'Toros 2':2, 'Toros 3': 3, 'Toros 4': 4},

    
    # get mapping (source_field_name -> global_schema_field_name)            
    print '\nMAPPING WITH METADATA: ..... '
    all_triples = []
    global last_RatingIDNUM_in_System
    last_RatingIDNUM_in_System = 1

    #for user_key in movieLens_prefs_UserRatings:
    for user_key in sorted(movieLens_prefs_UserRatings):
        #print 'user_key:  ', user_key
        #print 'user_ALL_movie_vs_ratings: ', movieLens_prefs_UserRatings[user_key]
        user_ALL_movie_vs_ratings = movieLens_prefs_UserRatings[user_key]
        
        # iterate over the ratings of this user
    #if user_key == 'Jaime':
        #for movie_rating in user_ALL_movie_vs_ratings:
        for movie_rating in sorted(user_ALL_movie_vs_ratings):
            #property_URI = sourceField_2_globalSchemaURI(source_item, in_source['source_name'], in_metadata_mappings, in_metadata_content)
            #property_URI = 'https://schema.org/Rating' 
            #property_URI_RatingMovie   = 'https://schema.org/RatingMovie' # harcoded because Movielens delivers a data file, no JSON or similar
            #property_URI_RatingUser    = 'https://schema.org/RatingUser' 
            #property_URI_RatingLiteral = 'https://schema.org/Rating' 
            property_URI_RatingMovie   = in_metadata_mappings[Reification_RatingMovie_relation]   #'https://schema.org/RatingMovie' #     
            property_URI_RatingUser    = in_metadata_mappings[Reification_RatingUser_relation]   #'https://schema.org/RatingUser' 
            property_URI_RatingLiteral = in_metadata_mappings[Reification_RatingNumber_relation]   #'https://schema.org/Rating' 
            
            propertyURI_TypeOf = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
            propertyURI_TernaryRating_USER_MOVIE_RATING = 'https://schema.org/Rating_UserMovieRating'
            
            
            
            URI_USER = 'URI_MOVIELENS_USER_' + user_key
            URI_PROPERTY_USERRATINGMOVIE_RATING_CLASS = 'URI_Property_UserRatingMovie_RATING_' + str(last_RatingIDNUM_in_System)

            #print 'MOVIE:                             ' + movie_rating
            #print 'USER:                              '+ URI_USER
            #print 'URI FICTICIA USER_RATING_MOVIE:    ' + URI_PROPERTY_USERRATINGMOVIE_RATING_CLASS
            #print 
            
            # build single tripple and append it with the others
            single_triple = []
            #<URI_Property_UserRatingMovie_RATING_1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Rating_UserMovieRating>
            single_triple.append(URI_PROPERTY_USERRATINGMOVIE_RATING_CLASS)
            single_triple.append(propertyURI_TypeOf)
            single_triple.append(propertyURI_TernaryRating_USER_MOVIE_RATING)
            all_triples.append(single_triple)
            
            
            single_triple = []
            # <URI_Property_UserRatingMovie_RATING1> <https://schema.org/Rating> <Predator_(1989)_MOVIELENS_source>
            URI_PROPERTY_USERRATINGMOVIE_RATING_CLASS
            film_URI = movie_rating.replace(' ', '_') # URI crashes in SPARQL is blanks exist
        
            #print movie_rating[0:3]
            #print film_URI
                        
            film_URI += '_' + in_source['source_name']
            #print film_URI
            single_triple.append(URI_PROPERTY_USERRATINGMOVIE_RATING_CLASS)
            single_triple.append(property_URI_RatingMovie)
            single_triple.append(film_URI)
            all_triples.append(single_triple)
            
            #<URI_Property_UserRatingMovie_RATING1> <https://schema.org/Rating> 4.5                
            single_triple = []
            single_triple.append(URI_PROPERTY_USERRATINGMOVIE_RATING_CLASS)
            single_triple.append(property_URI_RatingLiteral)
            single_triple.append(user_ALL_movie_vs_ratings[movie_rating])
            all_triples.append(single_triple)
            
            #<URI_Property_UserRatingMovie_RATING1> <https://schema.org/Rating> <URI_MOVIELENS_USER_JAIME>
            single_triple = []
            single_triple.append(URI_PROPERTY_USERRATINGMOVIE_RATING_CLASS)
            single_triple.append(property_URI_RatingUser)
            single_triple.append(URI_USER)
            all_triples.append(single_triple)
            
            last_RatingIDNUM_in_System+=1
    #print all_triples
    print 
        
    '''
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

        '''
    #print 'PRINTING ALL RESULTING TRIPLES'
    #print all_triples                           
    #print all_triples[1]
    #for i in range(0,len(all_triples)): 
    #    print all_triples[i][0] + ' - ' + all_triples[i][1] + ' - ' + all_triples[i][2]


    # if movie was not found, return empty list    
    return all_triples
    
def storeInVirtuoso_UserRatingsOnFilms(in_user, in_movie, in_rating, in_RDFgraph, in_metadata_mappings):    
    # initialize virtuoso
    metadata_SemanticDB_Virtuoso_settings = readMetadata_SemanticDB(METADATA_FILE)
    virtuoso_conn = virtuoso_connector(metadata_SemanticDB_Virtuoso_settings['http_location'], metadata_SemanticDB_Virtuoso_settings['port'], metadata_SemanticDB_Virtuoso_settings['folder_end_point'])

    
    # harcoded name of keys to look in the metadata because Movielens delivers a data file, no JSON or similar
    Reification_RatingMovie_relation = 'USER_RATING_MOVIE_Relation_RatingMovie'
    Reification_RatingUser_relation = 'USER_RATING_MOVIE_Relation_RatingUser'
    Reification_RatingNumber_relation = 'USER_RATING_MOVIE_Relation_RatingNumber'
    #property_URI_RatingMovie   = 'https://schema.org/RatingMovie' 
    #property_URI_RatingUser    = 'https://schema.org/RatingUser' 
    #property_URI_RatingLiteral = 'https://schema.org/Rating' 
    # harcoded name of keys to look in the metadata because Movielens delivers a data file, no JSON or similar
    property_URI_RatingMovie   = in_metadata_mappings[Reification_RatingMovie_relation]   #'https://schema.org/RatingMovie' # harcoded because Movielens delivers a data file, no JSON or similar
    property_URI_RatingUser    = in_metadata_mappings[Reification_RatingUser_relation]   #'https://schema.org/RatingUser' 
    property_URI_RatingLiteral = in_metadata_mappings[Reification_RatingNumber_relation]   #'https://schema.org/Rating' 
    
    propertyURI_TypeOf = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
    propertyURI_TernaryRating_USER_MOVIE_RATING = 'https://schema.org/Rating_UserMovieRating'
    
    URI_USER = 'URI_MOVIELENS_USER_' + in_user
    global last_RatingIDNUM_in_System
    URI_PROPERTY_USERRATINGMOVIE_RATING_CLASS = 'URI_Property_UserRatingMovie_RATING_' + str(last_RatingIDNUM_in_System)

    # build single tripple and append it with the others
    single_triple = []
    all_triples = []
    #<URI_Property_UserRatingMovie_RATING_1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Rating_UserMovieRating>
    single_triple.append(URI_PROPERTY_USERRATINGMOVIE_RATING_CLASS)
    single_triple.append(propertyURI_TypeOf)
    single_triple.append(propertyURI_TernaryRating_USER_MOVIE_RATING)
    all_triples.append(single_triple)
    
    
    single_triple = []
    # <URI_Property_UserRatingMovie_RATING1> <https://schema.org/Rating> <Predator_(1989)_MOVIELENS_source>
    URI_PROPERTY_USERRATINGMOVIE_RATING_CLASS
    film_URI = in_movie.replace(' ', '_') # URI crashes in SPARQL is blanks exist
    film_URI += '_MOVIELENS_source' # URI crashes in SPARQL is blanks exist


    single_triple.append(URI_PROPERTY_USERRATINGMOVIE_RATING_CLASS)
    single_triple.append(property_URI_RatingMovie)
    single_triple.append(film_URI)
    all_triples.append(single_triple)
    
    #<URI_Property_UserRatingMovie_RATING1> <https://schema.org/Rating> 4.5                
    single_triple = []
    single_triple.append(URI_PROPERTY_USERRATINGMOVIE_RATING_CLASS)
    single_triple.append(property_URI_RatingLiteral)
    single_triple.append(in_rating)
    all_triples.append(single_triple)
    
    #<URI_Property_UserRatingMovie_RATING1> <https://schema.org/Rating> <URI_MOVIELENS_USER_JAIME>
    single_triple = []
    single_triple.append(URI_PROPERTY_USERRATINGMOVIE_RATING_CLASS)
    single_triple.append(property_URI_RatingUser)
    single_triple.append(URI_USER)
    all_triples.append(single_triple)

    print 'storeInVirtuoso_UserRatingsOnFilms -----> '
    print all_triples
    #virtuoso_conn.insert_triples_user_rating_movie(all_triples, in_RDFgraph, '')
    virtuoso_conn.insert(all_triples, in_RDFgraph)

    last_RatingIDNUM_in_System += 1                       
    print 'ADDING RATING -> (USER: '+ in_user + ') - (MOVIE: ' + in_movie + ' ) - (RATING: ' + str(in_rating) + ')'


def store_ODStudents_ratings (in_RDFgraph, in_metadata_mappings):
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Star Trek III: The Search for Spock (1984)', 4.5, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Star Trek IV: The Voyage Home (1986)', 4.6, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Star Trek V: The Final Frontier (1989)', 4.7, in_RDFgraph, in_metadata_mappings)
    #'''
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Star Trek VI: The Undiscovered Country (1991)', 4.8, in_RDFgraph, in_metadata_mappings)
    
    '''storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Torrente', 5.3, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'El dia de la Bestia', 6.1, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Superman Returns', 5.2, in_RDFgraph, in_metadata_mappings)
    '''
    #'''
    
    
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Twelve Monkeys (a.k.a. 12 Monkeys) (1995)',4.5 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Stargate (1994)', 4, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Alexander (2004)', 4, in_RDFgraph, in_metadata_mappings)    
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Body Snatchers (1993)',4.5 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Jurassic Park (1993)',4.5 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Hellraiser',4.5 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Independence Day (a.k.a. ID4) (1996)', 4.5, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Night of the Living Dead (1968)',5 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Manhattan Project, The (1986)', 4.5, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Stealth (2005)', 4.5, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Children of Men (2006)', 4.5, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Relic, The (1997)', 4.5, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Fog, The (1980)',4.5 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Alien (1979)',5 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Psycho (1960)', 5, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Dagon (2001)', 3, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Starship Troopers 2: Hero of the Federation (2004)',2 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Constantine (2005)', 4.5, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Skeleton Key, The (2005)', 3, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Naked in New York (1994)', 3, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Vicky Cristina Barcelona (2008)',4 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Hamlet 2 (2008)',3 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Pocahontas (1995)',3 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Garfield: The Movie (2004)',3 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Incredibles, The (2004)', 4.5, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'SpongeBob SquarePants Movie, The (2004)',2 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Animatrix, The (2003)',5 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Beowulf (2007)',2 , in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('Jaime', 'Star Wars: The Clone Wars (2008)', 4.5, in_RDFgraph, in_metadata_mappings)
    
    '''
    storeInVirtuoso_UserRatingsOnFilms('JesulinUbrique', 'Toros 1', 1, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('JesulinUbrique', 'Toros 2', 2, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('JesulinUbrique', 'Toros 3', 3, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('JesulinUbrique', 'Toros 4', 4, in_RDFgraph, in_metadata_mappings)
    '''
    
    '''
    storeInVirtuoso_UserRatingsOnFilms('JaimePrimo', 'Torrente', 4.25, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('JaimePrimo', 'El dia de la Bestia', 4.7, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('JaimePrimo', 'Superman Returns', 4.5, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('JaimePrimo', 'Toros 2', 2, in_RDFgraph, in_metadata_mappings)
    storeInVirtuoso_UserRatingsOnFilms('JaimePrimo', 'Toros 4', 2.1, in_RDFgraph, in_metadata_mappings)
    '''
    '''
    print 'MOVILENS RATING USER JAIME' + json.dumps(movieLens_prefs_UserRatings['Jaime'], indent=2)
    print 'MOVILENS RATING USER CARLOS' + json.dumps(movieLens_prefs_UserRatings['Carlos'], indent=2)
    print 'MOVILENS RATING USER JESULINUBRIQUE' + json.dumps(movieLens_prefs_UserRatings['1Jesulin Ubrique'], indent=2)
    '''


def MOVIELENS_SOURCE_Load_from_Memory_TO_VIRTUOSO_in_pieces(in_film_all_triples, in_virtuoso_conn, in_RDF_GRAPH_RECOMMENDER, in_source):
    # LOAD ALL THE TRIPLES INTO VIRTUOSO IN PIECES OF 1400 TRIPLES IN EACH CALL (OTHERWISE VIRTUOSO WILL NOT ACCEPT ALL TRIPPLES IN 1 SINGLE CALL)   
    
    # 2. LOAD ALL THE TRIPLES INTO VIRTUOSO IN PIECES OF SIZE OF 1400 TRIPLES IN EACH CALL
    film_all_triples_ByChunks = []
    max_tripples = len(in_film_all_triples)
    print 'MAX NUM TRIPPLES: ' + str(max_tripples)
    
    in_virtuoso_conn.insert(in_film_all_triples, in_RDF_GRAPH_RECOMMENDER)
    
    '''
    j = 0
    while j <= max_tripples:
        film_all_triples_ByChunks = in_film_all_triples[j: (j+1400)]
        if len(film_all_triples_ByChunks) > 0:
           # load into virtuoso
           #in_virtuoso_conn.insert_triples_user_rating_movie(film_all_triples_ByChunks, in_RDF_GRAPH_RECOMMENDER, in_source)
           in_virtuoso_conn.insert(film_all_triples_ByChunks, in_RDF_GRAPH_RECOMMENDER)
        j = j+1400
        print 'J             ==========' + str(j)
        print '(TRIPLES      ==========' + str(len(in_film_all_triples)) + ')' + ' LONGITUD RATINGS ALL: ' + str(len(in_film_all_triples)/4)
    '''


def MOVIELENS_SOURCE_recommendations_IN_triple_format (in_recommendation_For_OD_System_User, in_source_type_to_check_real_name, in_metadata_mappings, in_metadata_content):
            
    # stores the User Based Collaboratory Filtering 
    aux_UB_CF_recommendation_all_triples = []        
    
    # in_source_type_to_check_real_name receives the source 'HTTP_API_request' to search for the real name of the recommendation based on the names of Movielens
    
    # NOW CONVERT EACH RECOMMENDATION COMING FROM THE DATA FROM MOVIELENS INTO A TRIPLE of the format:   
    # <URI_MOVIE_MOVIELENS> <https://schema.org/name> "literal name of movie"
    
    # 1. FIRST GET A SOURCE TO QUERY FOR THE REAL NAME OF THE FILM. IN this case we try with HTTP_API_REQUEST
    source_to_search = {} # choose one one of the sources to search the real name of the recommendation
    for source2 in in_metadata_content:
        if source2['query_type'] == in_source_type_to_check_real_name :      # search in OMDB the real name of the ficticiuos URI
            source_to_search = source2

    # 2. NOW FOR EACH RECOMMENDATION, GO TO THE SOURCE AND GET THE INFORMATION FOR THAT NAME WE HAD IN THE FICTICIOUS URI FROM MOVIELENS
    # go to that source and look for the film in OMDB and get the real name to put it on a triple to return as recommendation                                
    for i in range(0,len(in_recommendation_For_OD_System_User)):
        (rating_movielens, film_movielens_name) = in_recommendation_For_OD_System_User[i]
        URI_FILM_MOVIELENS = '<' + film_movielens_name + '>'
        property_URI_name = in_metadata_mappings['movie_name']  # we need to do this by hand because MOVIELENS has no properties, we read it from the file in disk
        # MOVIELENS URI was in this format:  Snow_White_and_the_Seven_Dwarfs_(1937)_MOVIELENS_source
        film_movielens_name = film_movielens_name.split("_(", 1)[0].replace ("_", " ").replace (", The", "")
        
        # get the OMDB info from the film
        film_all_triples = []
        film_all_triples = HTTP_API_SOURCE_request_film_info(film_movielens_name, source_to_search, in_metadata_mappings, in_metadata_content)                
        real_film_name_from_source = ""
        if len(film_all_triples) > 0:
            #print 'GOT INFO FROM OMDB: ' + str(film_all_triples)
            # 3. GET THE LIST CONTAINING ONLY THE name: [u'Seven_Samurai_OMBD_source', u'https://schema.org/name', u'Seven Samurai']
            # AND IGNORE THE OTHERS TRIPLES
            triple_with_real_film_name = []
            for i in range(0,len(film_all_triples)):
                (source_film_uri, source_property_uri, source_property_value) = film_all_triples[i]
                #print source_film_uri + ' ' + source_property_uri + ' ' + source_property_value
                if source_property_uri == property_URI_name:  
                    real_film_name_from_source = source_property_value
        else:
            print_film_not_found(film_movielens_name, source_to_search)

        #print URI_FILM_MOVIELENS
        property_URI_name = '<' + in_metadata_mappings['movie_name'] +'>' # we need to do this by hand because MOVIELENS has no properties, we read it from the file in disk
        #print property_URI_name  
        #print real_film_name_from_source
        single_triple = []
        single_triple.append(URI_FILM_MOVIELENS)
        single_triple.append(property_URI_name)
        single_triple.append(real_film_name_from_source)
        aux_UB_CF_recommendation_all_triples.append(single_triple)
           
    return aux_UB_CF_recommendation_all_triples
    

    
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
 
  
def MOVILENS_initialization():
    # read metadata
    metadata_content = readMetadata_sources(METADATA_FILE)
    metadata_mappings = readMetadata_mappings(METADATA_FILE)

    # initialize virtuoso
    metadata_SemanticDB_Virtuoso_settings = readMetadata_SemanticDB(METADATA_FILE)    
    virtuoso_conn = virtuoso_connector(metadata_SemanticDB_Virtuoso_settings['http_location'], metadata_SemanticDB_Virtuoso_settings['port'], metadata_SemanticDB_Virtuoso_settings['folder_end_point'])
    
    # Get the source
    for source in metadata_content:
        if source['source_name'] == "MOVIELENS_source":
            in_source = source;
    
    # 1. LOAD ALL TRIPLES INTO MEMORY from USER-RATING-MOVIES in source
    film_all_triples = []
    film_all_triples = MOVIELENS_SOURCE_Load_from_Disk_TO_Memory_in_triples_format(in_source, metadata_mappings, metadata_content)
    print 'LONGITUD RATINGS ALL: ' + str(len(film_all_triples)/4) + ' (TRIPLES: ' + str(len(film_all_triples)) + ')'
            
    # 2. LOAD ALL THE TRIPLES INTO VIRTUOSO IN PIECES OF 1400 TRIPLES IN EACH CALL (OTHERWISE VIRTUOSO WILL NOT ACCEPT ALL TRIPPLES IN 1 SINGLE CALL) 
    # MOVIELENS_SOURCE_Load_from_Memory_TO_VIRTUOSO_in_pieces(film_all_triples, virtuoso_conn, RDF_GRAPH_RECOMMENDER, in_source['source_name'])
    
            
    # 3. STORE SOME USER RATINGS FROM THE USER OF OUR OD SYSTEM (CARLOS & JAIME)           
    store_ODStudents_ratings (RDF_GRAPH_RECOMMENDER, metadata_mappings)
    return

def MOVILENS_get_user_based_recommendations():
    # read metadata
    metadata_content = readMetadata_sources(METADATA_FILE)
    metadata_mappings = readMetadata_mappings(METADATA_FILE)

    # initialize virtuoso
    metadata_SemanticDB_Virtuoso_settings = readMetadata_SemanticDB(METADATA_FILE)
    virtuoso_conn = virtuoso_connector(metadata_SemanticDB_Virtuoso_settings['http_location'], metadata_SemanticDB_Virtuoso_settings['port'], metadata_SemanticDB_Virtuoso_settings['folder_end_point'])
    CF_UB_recommender = collaboratory_filtering_UserBased()
    
    # initialize virtuoso
    virtuoso_conn = virtuoso_connector(metadata_SemanticDB_Virtuoso_settings['http_location'], metadata_SemanticDB_Virtuoso_settings['port'], metadata_SemanticDB_Virtuoso_settings['folder_end_point'])
    CF_UB_recommender = collaboratory_filtering_UserBased()
    
    # Get the source
    for source in metadata_content:
        if source['source_name'] == "MOVIELENS_source":
            in_source = source;
    
    # 4. READ FROM VIRTUOSO ALL THE USERS AND THEIR RATINGS 
    critics = {}
    critics = virtuoso_conn.get_USERS_and_RATINGS(RDF_GRAPH_RECOMMENDER, in_source['source_name'])
    print 'GOT FROM VIRTUOSO ---> USERS len(critics): ' + str(len(critics))
           
    # 5. GET THE 10 FIRST RECOMMENDATIONS USING USER COLLABORATORY FILTERING
    recommendation_For_OD_System_User = CF_UB_recommender.GET_RECOMMENDATIONS (critics, 'URI_MOVIELENS_USER_Jaime')[0:10]
    recommendation_For_OD_System_User = recommendation_For_OD_System_User[1:11]  
                        
    # 6. NOW CONVERT EACH RECOMMENDATION INTO A TRIPLE with the format:   
    # <URI_MOVIE_MOVIELENS> <https://schema.org/name> "literal name of movie"
    print str(len(recommendation_For_OD_System_User))
    UB_CF_recommendation_all_triples = []
    source_type_to_check_real_name = 'HTTP_API_request'
    UB_CF_recommendation_all_triples = MOVIELENS_SOURCE_recommendations_IN_triple_format (recommendation_For_OD_System_User, source_type_to_check_real_name, metadata_mappings, metadata_content)
            
    print '\n' + '*'*40
    print 'MOVILENS RECOMMENDATIONS:'
    print '*'*40
    print str(UB_CF_recommendation_all_triples)
    
    return UB_CF_recommendation_all_triples