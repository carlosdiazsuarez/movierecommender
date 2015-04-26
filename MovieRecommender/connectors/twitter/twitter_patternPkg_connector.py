'''
Created on Apr 14, 2015

@author: Jaime
'''
from pattern.web import Twitter, hashtags
from pattern.db  import Datasheet, pprint, pd
import codecs
import json
import string

class twitter_patternPkg_connector(object):
    '''
    classdocs
    '''

    def __init__(self):
        self.FILE_STORAGE = "OD_CK1_Source4_Tweeter_InitialLoad.csv"
        self.search_topic = ''

    def formatData2Json(self, tweet_id, tweet_date, tweet_subject, tweet_text):
        data = {}
        data['tweet_id'] = tweet_id
        data['created_at'] = tweet_date
        data['topic'] = tweet_subject         #data['movie_name'] = tweet_subject
        data['text'] = tweet_text
        #json_data = json.dumps(data)
        json_data = data
        #print json_data
        # print json.dumps(json_data, indent= 2, separators=(',',':'))
        return json_data


    def getTweetSecureLoad(self, topic):
        # This example retrieves tweets containing given keywords from Twitter.

        self.search_topic = topic
        print 'CLASS (Twitter_PatternPKG) - Twitter Secure Initial Load - Topic: ' + self.search_topic
        self.search_topic = topic + ' film'
        try: 
            # We'll store tweets in a Datasheet.
            # A Datasheet is a table of rows and columns that can be exported as a CSV-file.
            # In the first column, we'll store a unique id for each tweet.
            # We only want to add the latest tweets, i.e., those we haven't seen yet.
            # With an index on the first column we can quickly check if an id already exists.
            # The pd() function returns the parent directory of this script + any given path.

            table = Datasheet.load(pd(self.FILE_STORAGE))
            # index = set(table.columns[0])
            index = set(table.columns[4])   # on the text
            
        except:
            table = Datasheet()
            index = set()

        engine = Twitter(language="en")

        # With Twitter.search(cached=False), a "live" request is sent to Twitter:
        # we get the most recent results instead of those in the local cache.
        # Keeping a local cache can also be useful (e.g., while testing)
        # because a query is instant when it is executed the second time.
        prev = None

        #searchThisSubjects = search_topic

        # put headers
        table.append(["tweet_id", "tweet_date", "InputSubject", "Tweet_text"])

        #for oneSubject in searchThisSubjects:
        oneSubject = self.search_topic
        # oneSubject

        tweet_list_Json = []  # list of JSons
        tweet_list = []
        try:
            for i in range(1):
                for tweet in engine.search(oneSubject, start=prev, count=8, cached=False):
                    if 'http' in tweet.text:
                        posi = tweet.text.index('http')
                        tweet.text = tweet.text[0:posi-1]
                                
                    # Only add the tweet to the table if it doesn't already exists.
                    if len(table) == 0 or tweet.text not in index :
                        table.append([tweet.id, tweet.date, oneSubject, tweet.text])
                        index.add(tweet.text)
                        
                        tweet_list.append([tweet.id, tweet.date, oneSubject, tweet.text])
                        #tweetJson = self.formatData2Json(tweet.id, tweet.date, oneSubject, tweet.text)
                        #tweetJson = self.formatData2Json(tweet.id, tweet.date, oneSubject.replace(' film', ''), tweet.text)
                        tweet.text = filter(lambda x: x in string.printable, tweet.text) # remove weird stuff
                        tweet.text = tweet.text.replace('"', '') # remove weird stuff
                        tweet.text = tweet.text.replace('\n', '') # remove weird stuff
                        tweetJson = self.formatData2Json(tweet.id, tweet.date, oneSubject.replace(' film', ''), tweet.text) # remove artificiall film 
                        
                        tweet_list_Json.append(tweetJson)
                        #print tweetJson  
                        
                        # BUILD A JSON
                        #http://stackoverflow.com/questions/14547916/how-can-i-loop-over-entries-in-json
                        #BUILD A LIST OF DICTIONARIES                    
                        #http://stackoverflow.com/questions/2733813/iterating-through-a-json-object
                        
                        
                    # Continue mining older tweets in next iteration.
                    prev = tweet.text
    
        except Exception:
            print 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX - ERROR!! - ([twitter_patternPkg_connector] getTweetSecureLoad)'
            print 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX - ERROR!!   (film: ' + oneSubject +')' 
            pass
        
        # Create a .csv in pattern/examples/01-web/
        # table.save(pd("OD_CK1_Source4_Tweeter_InitialLoad.csv"))
        print "CLASS (Twitter_PatternPKG) - Total Secure Twitter Load: " +  str(len(table)) + '\n'
        #print json.dumps(tweet_list)
        
        # return tweet_list
        return tweet_list_Json
