'''
Created on Apr 15, 2015

@author: Jaime
'''

from tweepy.streaming import StreamListener
import json, time, sys

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream

import os
import re
import csv
import codecs
from copy import deepcopy
import urllib2

class twitter_streaming_connector(object):
    '''
    classdocs
    '''

    class SListener(StreamListener):

        def __init__(self, topic2search, father, fprefix = 'streamer'):
            # self.api = api or API()
            self.counter = 0
            self.fprefix = fprefix
            #self.output  = open(fprefix + '.json', 'w')
            #self.delout  = open('delete.txt', 'a')      
            self.topic2search = topic2search
            self.list_tweets_to_fill = []
            self.father = father

        def on_data(self, data):
            if  'in_reply_to_status' in data:
                self.on_status(data)
            elif 'delete' in data:
                delete = json.loads(data)['delete']['status']
                if self.on_delete(delete['id'], delete['user_id']) is False:
                    return False
            elif 'limit' in data:
                if self.on_limit(json.loads(data)['limit']['track']) is False:
                    return False
            elif 'warning' in data:
                warning = json.loads(data)['warnings']
                print warning['message']
                return False

        def on_status(self, status):
            
            #self.output.write(status + "\n")
            self.counter += 1
            '''
            print '------- TWEET: ' + str(self.counter)
            print status   # this is a JSON
            print '\n - ID - \n' + json.loads(status)['id_str']
            print '\n - CREATED AT - \n' + json.loads(status)['created_at']
            print '\n - LOCATION - \n' + json.loads(status)['user']['location']
            print '\n - USER - \n' + json.loads(status)['user']['id_str']
            print '\n - TEXT - \n' + json.loads(status)['text']
            print 
            print
            #print json.dumps(status, indent=4)
            '''
            
            this_tweet = [json.loads(status)['id_str'], json.loads(status)['created_at'], self.topic2search, json.loads(status)['text']]
            #print this_tweet
            self.list_tweets_to_fill.append(this_tweet)                    

            if self.counter >= 5:
                #self.output.close()
                #print "File Closed."
                # print "Creating new file..."
                #self.output = open(self.fprefix + '.' + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
                self.counter = 0

                #print len(self.list_tweets_to_fill)
                #print self.list_tweets_to_fill
                self.father.set_twitter_streamed(self.list_tweets_to_fill)
             
                print 'shutting down listener itself...'
                self.shutdown()
             
            return

        def on_delete(self, status_id, user_id):
            #print "writing in delete file..."
            #self.delout.write( str(status_id) + "\n")
            return

        def on_limit(self, track):
            sys.stderr.write(track + "\n")
            return

        def on_error(self, status_code):
            sys.stderr.write('Error: ' + str(status_code) + "\n")
            return False

        def on_timeout(self):
            sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
            print "Timeout, sleeping for 60 seconds...\n"
            time.sleep(60)
            return 


    def __init__(self):
        self.workingDir=os.getcwd() +'\\'     
        self.FILE_STORAGE = "OD_CK1_Source4_Tweeter_StreamingLive"
    
        ## authentication
        self.ckey= 'ajy85rNFgAuMmTcovsQC8RuKx'
        self.csecret = 'GbeqKPH2sov1t7woCKVqqdxewpuMVPsvnbNuhoGJHaTG7Io0IN'
        self.atoken = '3160092849-SNhidCsQBgqD5PRta1kQkSEvLPw4ujx7iaEUfMR'
        self.asecret= '3JXCjD4MMN9fjgsCTsAV9Q3CvrONmQDoYH241UIzSxOwS'
        self.tweets_list_streamed = []
        
        
    def getTweetStreamingLive(self, topic_to_search):
        print '\n\nCLASS (twitter_streaming_connector) - Twitter Streaming Load - Topic: ' + topic_to_search

        auth = OAuthHandler(self.ckey, self.csecret)
        auth.set_access_token(self.atoken, self.asecret)

        track = [topic_to_search]
        #track = ['Obama', 'Castro']           # Obama OR Castro  
        #track = ['The Lord of the Rings']     # The AND Lord AND of AND the AND Rings
        
        print 'SEARCHING TWEET WITH RELATED TO: '+ str(track)
    
        #listen = self.SListener(topic_to_search, self.FILE_STORAGE)
        listen = self.SListener(topic_to_search, self, self.FILE_STORAGE)    
        stream = tweepy.Stream(auth, listen)

        print "Streaming started..."

        try: 
            stream.filter(languages=["en"], track = track)
        except Exception, e:
            #print 'EXCEPTION!!!: '+ str(e)
            print 'Finishing Stream Listener for topic: ' +  str(track)  + '!'
            stream.disconnect()

            #print 'FINAL OUTPUT!!!!!'
            # print len(self.tweets_list_streamed)
            # print self.tweets_list_streamed
            print 'Sending Tweeters back (in the form of a list)'            
            print 'CLASS (twitter_streaming_connector) - Total Streming Load: ' + str(len(self.tweets_list_streamed)) + '\n'        
            return self.tweets_list_streamed


    def set_twitter_streamed(self, twitters_arrived):
        self.tweets_list_streamed = twitters_arrived
        
        
        
