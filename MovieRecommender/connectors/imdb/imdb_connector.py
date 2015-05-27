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
class IMDB:
    
    def __init__(self):
        return
    
    def getIDs(self, name):
        self.params = {'q': name.encode("latin_1")}

        params = deepcopy(self.params)
        for key, val in params.iteritems():
            if val == '':
                self.params.pop(key)
        params = urllib.urlencode(self.params)

        conn = httplib.HTTPConnection('www.imdb.com')
        conn.request("GET", "/find?" + params, "")

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
            
        results = self.html.findAll('td', attrs={'class': 'result_text'})
        self.ids = []
        for td in results:
          self.ids.append(td.a.attrs[0][1]) 

        return self.ids
    
    def getTitleExtra(self, title):

        conn = httplib.HTTPConnection('www.imdb.com')
        conn.request("GET", title, "")

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
                    
        results = self.html.findAll('span', attrs={'class': 'title-extra'})
        self.titles = []
        for span in results:
          title = span.contents[0]
          title = title.replace('\n', ' ')
          title = title.replace('"', ' ')
          title = title.strip()     
          self.titles.append(title) 

        return self.titles    