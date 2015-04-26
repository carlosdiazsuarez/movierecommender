'''
Created on 14/04/2015

@author: jandres
'''

import sys
import os
workingDir=os.getcwd()
#sys.path.append( workingDir+"\\RunningPython")
import re
import csv
import codecs
from copy import deepcopy
import urllib2
import json    # JSON (JavaScript Object Notation) is a compact, text based format for computers to exchange data and is once loaded into Python just like a dictionary. 


class http_api_request_connector(object):
    
    workingDir=os.getcwd() +'\\'
    MOVIES_TO_BE_LOAD_INITIAL_LOAD = 'OD_CK1_InitialSetMovieNames_from_DBpedia.txt'
    f_movie_1stload_name = workingDir + MOVIES_TO_BE_LOAD_INITIAL_LOAD
    
    p_movie_type = 'movie'
    p_year = ""
    p_plot = 'full'
    p_tomatoes = False
    not_exist_movie_name = 'NOT EXISTING FILM'


    def __init__(self):
        self.workingDir=os.getcwd() +'\\'
        self.MOVIES_TO_BE_LOAD_INITIAL_LOAD = 'OD_CK1_InitialSetMovieNames_from_DBpedia.txt'
        self.f_movie_1stload_name = workingDir + self.MOVIES_TO_BE_LOAD_INITIAL_LOAD
        self.p_movie_type = 'movie'
        self.p_year = ""
        self.p_plot = 'full'
        self.p_tomatoes = False
        self.not_exist_movie_name = 'NOT EXISTING FILM'

        
    ##functions
    def parse_movieName(self, in_title):
        #print 'PARSING...' + in_title
        #print 'PARSED...' + in_title.replace (" ", "+")
        #print 'PARSED...' + re.sub(' \(film\)$', '', in_title).replace (" ", "+")
        #print '-'*20
        #print '-'*20
        #return in_title.replace (" ", "+")
        return re.sub(' \(film\)$', '', in_title).replace (" ", "+")

    def build_ombdQuery_byTitle(self, in_title, in_year):
        #print 'OMBd QUERY BUILDING...'
        #print 'OMBd QUERY BUILT...' + 'http://www.omdbapi.com/?t=' + in_title + '&y=' + in_year + '&plot=full' +  '&r=json'
        #print '-'*20
        #print '-'*20
        return 'http://www.omdbapi.com/?t=' + in_title + '&y=' + in_year + '&plot=full' +  '&r=json'            #+ '&type=movie'

    def fire_http_api_Query(self, in_http_api_Request):
        #print 'FIRING'
        #print in_http_api_Request
        json_result = json.load(urllib2.urlopen(in_http_api_Request))
        #print 'RECEIVED'
        #print '-'*20
        #print '-'*20
        return json_result


    def print_movie(self, in_json):
        print 'PIECES OF RESULT (function)'
        print 'TITLE        - ' + in_json['Title']
        print 'YEAR         - ' + in_json['Year']
        print 'RATED        - ' + in_json['Rated']
        print 'RUNTIME      - ' + in_json['Runtime']
        print 'GENRE        - ' + in_json['Genre']
        print 'DIRECTOR     - ' + in_json['Director']
        print 'WRITER       - ' + in_json['Writer']
        print 'ACTORS       - ' + in_json['Actors']
        print 'PLOT         - ' + in_json['Plot']
        print 'LANGUAGE     - ' + in_json['Language']
        print 'COUNTRY      - ' + in_json['Country']
        print 'AWARDS       - ' + in_json['Awards']
        print 'POSTER       - ' + in_json['Poster']
        print 'imdbRATING   - ' + in_json['imdbRating']
        print 'imdbVOTES    - ' + in_json['imdbVotes']
        print 'imdbID       - ' + in_json['imdbID']
        print '-'*20
        print '-'*20


    def print_Basic_movie(self, in_json):
        print 'TITLE        - ' + in_json['Title']
        print json.dumps(in_json, indent=2)


    def load_Initial_Set_Movies(self):
        
        # get non existing film result
        p_movie_name_not_Exist_parsed = self.parse_movieName(self.not_exist_movie_name)
        ombd_query_NotExist = self.build_ombdQuery_byTitle(p_movie_name_not_Exist_parsed, self.p_year)
        json_movie_notExist = self.fire_ombdQuery(ombd_query_NotExist)
        #print json_movie_notExist

        f_1stload_movie = codecs.open(self.f_movie_1stload_name, "r", "utf-8")
        movie_name_to_search = f_1stload_movie.readline()

        while movie_name_to_search != '':    

            movie_name_to_search = movie_name_to_search[0: (len(movie_name_to_search) -2)] # get rid of \r\n

            # look for film
            p_movie_name_parsed = self.parse_movieName(movie_name_to_search)
            ombd_query = self.build_ombdQuery_byTitle(p_movie_name_parsed, self.p_year)
            json_movie_result = self.fire_ombdQuery(ombd_query)
            #print json_movie_result
            print
            if json_movie_result == json_movie_notExist:
                print 'Movie to search: ' + movie_name_to_search
                print json_movie_result
                #print 'DID NOT EXIST \n\n\n\n'
                print 'DID NOT                                                            !!!!!!!!!'
    
            else:
                print 'Movie to search: ' + movie_name_to_search
                #self.print_movieOMDb(json_movie_result)
                self.print_Basic_movie(json_movie_result)
                #print 'EXISTED \n\n\n\n'
                print 'EXISTED'
                

            movie_name_to_search = f_1stload_movie.readline()

        print
        f_1stload_movie.close()
    
    
    def getMovie_by_Name(self, movie_name_to_search):
        p_movie_name_parsed = self.parse_movieName(movie_name_to_search)
        ombd_query = self.build_ombdQuery_byTitle(p_movie_name_parsed, self.p_year)
        json_movie_result = self.fire_ombdQuery(ombd_query)
        return json_movie_result
    
    def getMovie_Information(self, in_http_api_request):
        json_movie_result = {}
        try:        
            json_movie_result = self.fire_http_api_Query(in_http_api_request)
        except Exception:
            print 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX - ERROR!! - ([http_api_request_connector] getMovie_Information)'
            print 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX - ERROR!!   \n(query: ' + in_http_api_request +')' 
            pass
            
        return json_movie_result
        
