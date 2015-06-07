'''
Created on May 10, 2015

@author: Jaime
'''

from math import sqrt
import json

class collaboratory_filtering_UserBased(object):
    '''
    classdocs
    '''

    def __init__(self, ):
       print 
    
    # Returns the Pearson correlation coefficient for p1 and p2
    def sim_pearson(self, prefs,p1,p2):
    # Get the list of mutually rated items
        si={}
        for item in prefs[p1]:
            if item in prefs[p2]: si[item]=1
        # Find the number of elements
        n=len(si)
    
        # if they are no ratings in common, return 0
        if n==0: return 0
    
        # Add up all the preferences
        sum1=sum([prefs[p1][it] for it in si])
        sum2=sum([prefs[p2][it] for it in si])
    
        # Sum up the squares
        sum1Sq=sum([pow(prefs[p1][it],2) for it in si])
        sum2Sq=sum([pow(prefs[p2][it],2) for it in si])
    
        # Sum up the products
        pSum=sum([prefs[p1][it]*prefs[p2][it] for it in si])
    
        # Calculate Pearson score
        num=pSum-(sum1*sum2/n)
        den=sqrt((sum1Sq-pow(sum1,2)/n)*(sum2Sq-pow(sum2,2)/n))
        if den==0: return 0
    
        r=num/den
    
        return r

    def COMPARING_PEOPLE (self, in_critics, person1, person2):
        print '\n' + '*'*40
        print 'person1: ' + person1 + json.dumps(in_critics[person1], indent=2)
        print 'person2: ' + person2 + json.dumps(in_critics[person2], indent=2)
        print 'SIM PEARSON: ' + str(self.sim_pearson(in_critics, person1,person2))
        print '*'*40       
        #'URI_MOVIELENS_USER_JesulinUbrique', 'URI_MOVIELENS_USER_JaimePrimo'

       
    # compare 1 person against everyone else and find the closest match
    # Returns the best matches for person from the prefs dictionary.
    # Number of results and similarity function are optional params.
    def topMatches(self, prefs,person,n=5):
        scores=[(self.sim_pearson(prefs,person,other),other) for other in prefs if other!=person]
        # Sort the list so the highest scores appear at the top
        scores.sort( )
        scores.reverse( )
        print scores
        return scores[0:n]
   
    
    def TOP_MATCHES(self, in_critics, person):     

        print '\n' + '*'*40
        print 'MOVILENS RATING USER: '+  person + json.dumps(in_critics[person], indent=2)
        print '\n' + '*'*40
        print 'TOP MATCHES ----> ' + person + ': ' + str(self.topMatches(in_critics, person, n=3))
        
        #'URI_MOVIELENS_USER_JaimePrimo'
        #'URI_MOVIELENS_USER_JesulinUbrique'
        #'URI_MOVIELENS_USER_296'
            
        print '*'*40


    # Gets recommendations for a person by using a weighted average
    # of every other user's rankings
    def getRecommendations(self, prefs,person):
        totals={}
        simSums={}
        for other in prefs:
            # don't compare me to myself
            if other==person: continue
            sim=self.sim_pearson(prefs,person,other)
    
            # ignore scores of zero or lower
            if sim<=0: continue
            for item in prefs[other]:
                # only score movies I haven't seen yet
                if item not in prefs[person] or prefs[person][item]==0:
                    # Similarity * Score
                    totals.setdefault(item,0)
                    totals[item]+=prefs[other][item]*sim
                    # Sum of similarities
                    simSums.setdefault(item,0)
                    simSums[item]+=sim
    
        # Create the normalized list. Scores are normalized by dividing each of the total movie scores by similarity sum
        rankings=[(total/simSums[item],item) for item,total in totals.items( )]
    
        # Return the sorted list
        rankings.sort( )
        rankings.reverse( )
        return rankings

   
    def GET_RECOMMENDATIONS (self, in_critics, person):
        print '\n' + '*'*40
        print 'GET RECOMMENDATIONS (USER BASED) ----> ' + person + ': ' + str(self.getRecommendations(in_critics, person))
        
        print 'GET RECOMMENDATIONS (USER BASED) ----> ' + 'URI_MOVIELENS_USER_JaimePrimo' + ': ' + str(self.getRecommendations(in_critics, 'URI_MOVIELENS_USER_JaimePrimo'))
        print 'GET RECOMMENDATIONS (USER BASED) ----> ' + 'URI_MOVIELENS_USER_JesulinUbrique' + ': ' + str(self.getRecommendations(in_critics, 'URI_MOVIELENS_USER_JesulinUbrique'))
        print 'GET RECOMMENDATIONS (USER BASED) ----> ' + 'URI_MOVIELENS_USER_170' + ': ' + str(self.getRecommendations(in_critics, 'URI_MOVIELENS_USER_170'))                    
        print '*'*40
        return self.getRecommendations(in_critics, person)
        
        
        
        