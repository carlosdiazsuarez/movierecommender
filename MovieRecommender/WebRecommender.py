'''
Created on 25/05/2015

@author: cdiaz
'''

import web
from connectors.googlemoviesshowtimes.parser import GoogleMovieShowtimes

render = web.template.render('templates/')

urls = (
    '/', 'index',
    '/coldstart', 'coldstart'
)
     
class index:
    def GET(self):
        return render.index()
    
class coldstart:
    def GET(self):
        input = web.input(city=None)
        parser = GoogleMovieShowtimes("barcelona")    
        gms_resp = parser.parse()
        
        return render.coldstart(input.city, gms_resp)                                      

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()