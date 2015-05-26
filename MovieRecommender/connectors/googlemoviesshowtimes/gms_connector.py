'''
Google Movie Showtimes parser class for Python.

This script provides a Python class that can be used to parse Google Movie
Showtimes (www.google.com/movies) pages into dictionary objects.

@author Vaidik Kapoor
@version 0.1
'''

import httplib, urllib, BeautifulSoup, re
from copy import deepcopy
from BeautifulSoup import BeautifulSoup

'''
GoogleMovieShowtimes class
    This class is used for getting response from www.google.com/movies
'''
class GoogleMovieShowtimes:
    '''
    Constructor for GoogleMovieShowtimes class.

    Parameters:
        near      - (string) valid name of the location (city)
    '''
    def __init__(self, near):
        self.params = {'near': near}

        params = deepcopy(self.params)
        for key, val in params.iteritems():
            if val == '':
                self.params.pop(key)
        params = urllib.urlencode(self.params)

        conn = httplib.HTTPConnection('www.google.com')
        conn.request("GET", "/movies?" + params, "")

        response = conn.getresponse()
        self.response_code = response.status
        self.response = response.getheaders
     
        self.response_body = ""
        while 1:
            data = response.read()
            if not data:
                break
            self.response_body += data

        if (self.response_code == 200):
            self.html = BeautifulSoup(self.response_body)

    '''
    Function for parsing the response and getting all the important data
    as a huge dictionary object.

    No parameters are required.
    '''
    def parse(self):
        
        resp = {'theater': []}
        theaters = self.html.findAll('div', attrs={'class': 'theater'})
        for div in theaters:
            resp['theater'].append({})

            index = resp['theater'].index({})

            theater = []
            theater.append(('name', div.div.h2.a.contents[0]))
            theater.append(('info', div.div.div.contents[0]))
            theater.append(('movies', []))

            resp['theater'][index] = dict(theater)

            movies = div.findAll('div', {'class': 'movie'})
            for div_movie in movies:
                resp['theater'][index]['movies'].append({})

                index_m = resp['theater'][index]['movies'].index({})

                movie = []                 
                movie.append(('movieId', div_movie.div.a.attrs[0][1].split("=")[-1]))                
                movie.append(('movieName', div_movie.div.a.contents[0]))
                movie.append(('movieInfo', div_movie.span.contents[0]))
                movie.append(('times', []))

                resp['theater'][index]['movies'][index_m] = dict(movie)

                times = div_movie.find('div', {'class': 'times'})
                times = times.findAll('span')
                for div_time in times:
                    if len(div_time.contents) == 3:
                        time_val = div_time.contents[2]
                        time_val = re.search('(\d\d:\d\d)', time_val.string)
                        resp['theater'][index]['movies'][index_m]['times'].append(time_val.group(0))

        return resp