'''
Created on 13/04/2015

@author: cdiaz
'''
from googlemoviesshowtimes.parser import GoogleMovieShowtimes

if __name__ == '__main__':
    parser = GoogleMovieShowtimes("barcelona")
    
    resp = parser.parse()
    
    print resp
    
    parser = GoogleMovieShowtimes("madrid")
    
    resp = parser.parse()
    
    print resp
        
    
    