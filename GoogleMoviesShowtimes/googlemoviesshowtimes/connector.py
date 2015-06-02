'''
Created on 13/04/2015

@author: cdiaz
'''
from googlemoviesshowtimes.parser import GoogleMovieShowtimes

from geoip import geolite2

if __name__ == '__main__':
    
    match = geolite2.lookup_mine()
    
    print match
        
    parser = GoogleMovieShowtimes(match.location)
    
    resp = parser.parse()
    
    print resp
        
    
    