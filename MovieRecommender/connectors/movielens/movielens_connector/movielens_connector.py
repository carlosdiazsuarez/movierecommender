'''
Created on Apr 14, 2015

@author: Jaime
'''

import codecs                   # codecs.open("test", "r", "utf-8")

class movielens_connector(object):
    '''
    classdocs
    '''
    global FILE_RATINGS
    global FILE_MOVIES

    def __init__(self, in_location_folder):
        self.FILE_RATINGS = in_location_folder + '/ratings.csv'
        self.FILE_MOVIES = in_location_folder + '/movies.csv'
        print 'FILE_RATINGS: ' + self.FILE_RATINGS
        print 'FILE_MOVIES: '+ self.FILE_MOVIES

    def find_char_in_str(self, s, ch):
        return [i for i, ltr in enumerate(s) if ltr == ch]

    def load_parse_MovieLens2015_UserRatings(self, path):
        # Get movie titles
        print path
        movies={}
        #for line in open(path+'/movies.csv'):
        for line in codecs.open(path+'/movies.csv', "r", "utf-8"):
            if ( len(line.split(',')) == 4 and ( ' The (' in line or ' A (' in line ) ):
                pos = line.index(',')
                id = line[0: pos]
                pos_comillas = []
                pos_comillas = self.find_char_in_str(line, '"')
                title = line[pos+2:pos_comillas[1]]        
                movies[id]=title
            if len(line.split(',')) == 3 :
                (id,title)=line.split(',')[0:2]
                movies[id]=title
        #print movies['79057']
        #print movies['4993'] + '\n'
        # Load data
        prefs={}
        
        firsLine=True
        for line in open(path+'/ratings.csv'):
            if firsLine:
                firsLine = False
                continue
    
            (user,movieid,rating,ts)=line.split(',')
            prefs.setdefault(user,{})
            if movieid in movies:
                prefs[user][movies[movieid]]=float(rating)
        
        
        # SMALLER VERSION - USERS WITH MAX 30 RATINGS, otherwise Virtuoso dies!
        prefs_MAX30ratings_per_user = {}
        for user_key in prefs:
            #print 'USER (' + user_key + ') RATINGS (' + str(len(prefs[user_key])) + ')'
            if len(prefs[user_key]) < 50:
                prefs_MAX30ratings_per_user[user_key] = prefs[user_key]
                #print 'METIENDO ESTE!!!!!!!!!!!!!!!!!!!!'
        
        #print '################################################################3 prefs_MAX30ratings_per_user'
        #for user_key2 in prefs_MAX30ratings_per_user:
            #print 'USER (' + user_key2 + ') RATINGS (' + str(len(prefs_MAX30ratings_per_user[user_key2])) + ')'
        
        
        print '\n' + '*'*40
        print 'MOVIELENS load_parse_MovieLens2015_UserRatings SUCCESSFULLY: (folder to search: ' + path +')'
        print '*'*40
        
        #return prefs
        return prefs_MAX30ratings_per_user
