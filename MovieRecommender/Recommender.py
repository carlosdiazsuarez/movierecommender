'''
Created on 14/04/2015

@author: cdiaz jandres
'''

import re, math
import json
from collections import Counter

from connectors.omdb.omdb_connector import omdb_connector

from connectors.twitter.twitter_patternPkg_connector import twitter_patternPkg_connector
from connectors.twitter.twitter_streaming_connector import twitter_streaming_connector




def main():


    ################################################## JAIME START
    # HEY CARLOS i ADDED THIS STUFF SO YOU SEE HOW MY OMDB AND TWEETERS STUFF CAN BE INVOKED FROM THE MAIN FUNCITON
    # NOTE THAT THE TWEETER STUFF IS RETURNING DATA AS LIST LISTS (THE JSONS I HAD TO TREAT THEM WITHIN THE CLASSES IN ORDER TO PROCESS THEM
    # THAT SHOULD NOT BE A PROBLEM SINCE THE TWEETER STUFF IS FOR ENRICHMENT OPERATIONS
    
    film_to_search = 'Obama'
    print 'TOPIC TO SEARCH: ' + film_to_search
    print 
    
    
    # SOURCE 1 OMDb Movie Details
    print '\n' + '*'*40
    print 'PROCEEDING TO OMBD search:'
    print '*'*40
    omdb_conn = omdb_connector()
    #omdb_conn.load_Initial_Set_Movies()
    json_movie_result = omdb_conn.getMovie_by_Name(film_to_search)
    print 'OMDb LOAD - PRINTING JSON file:'
    print json_movie_result
    print json.dumps(json_movie_result, indent=2)
    
    
    # SOURCE 4 A TWITTER SECURE LOAD
    print '\n' + '*'*40
    print 'PROCEEDING TO TWITTER SECURE SEARCH:'
    print '*'*40
    twitter_secureLoad_conn = twitter_patternPkg_connector ()
    tweets_initialDump_List = twitter_secureLoad_conn.getTweetSecureLoad(film_to_search)
    print 'TWEETER SECURE LOAD - PRINTING LIST OF TWEETS (LIST OF [tweet.id, tweet.date, oneSubject, tweet.text]):'
    print tweets_initialDump_List
   
   
    # SOURCE 4 B TWITTER STREAMING LOAD
    print '\n' + '*'*40
    print 'PROCEEDING TO TWITTER STREAMING SEARCH:'
    print '*'*40
    twitter_StreamingLive_conn = twitter_streaming_connector()
    tweets_Streaming_List = twitter_StreamingLive_conn.getTweetStreamingLive(film_to_search)
    print 'TWEETER STEAMING LOAD - PRINTING LIST OF TWEETS (LIST OF [tweet.id, tweet.date, oneSubject, tweet.text]):'
    print tweets_Streaming_List


    print 'FINISHED ALL SEARCHES: ' + film_to_search
    

    ################################################## JAIME END 

if __name__ == '__main__':
    main()
