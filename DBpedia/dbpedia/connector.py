'''
Created on 13/04/2015

@author: cdiaz
'''

import sys, getopt

from SPARQLWrapper import SPARQLWrapper, JSON

def getObjectDatatypeProperty(uri, property):
    sparql = SPARQLWrapper("http://dbpedia.org/sparql")   
    sparql.setQuery("""
        PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>
        SELECT ?""" + property + 
        """ WHERE {
           <""" + uri + """> dbpedia-owl:""" + property +  """ ?""" + property + """ .
        }
        """)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
 
    return results

def getProperty(uri, property):
    sparql = SPARQLWrapper("http://dbpedia.org/sparql")   
    sparql.setQuery("""
        PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>
        SELECT ?""" + property + 
        """ WHERE {
           <""" + uri + """> dbpprop:""" + property +  """ ?""" + property + """ .
        }
        """)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
 
    return results

def main(argv):
   uri = ''
   try:
      opts, args = getopt.getopt(argv,"hu:",["uri="])
   except getopt.GetoptError:
      print 'test.py -u <uri>'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'test.py -u <uri>'
         sys.exit()
      elif opt in ("-u", "--uri"):
         uri = arg
   
   print 'Movie uri is:\r', uri
   
   results = getObjectDatatypeProperty(uri, "director")
   print '\rDirectors are: \r'
   for result in results["results"]["bindings"]:
       print(result["director"]["value"])
       
   results = getProperty(uri, "name")
   print '\rName is: \r'
   for result in results["results"]["bindings"]:
       print(result["name"]["value"])

   results = getObjectDatatypeProperty(uri, "abstract")
   print '\rAbstract is: \r'
   for result in results["results"]["bindings"]:
       print(result["abstract"]["value"])

   results = getObjectDatatypeProperty(uri, "writer")
   print '\rWriters are: \r'
   for result in results["results"]["bindings"]:
       print(result["writer"]["value"])
                   
   results = getObjectDatatypeProperty(uri, "starring")
   print '\rStarring is: \r'
   for result in results["results"]["bindings"]:
       print(result["starring"]["value"])
             
   results = getObjectDatatypeProperty(uri, "runtime")
   print '\rRuntime is: \r'
   for result in results["results"]["bindings"]:
       print(result["runtime"]["value"])

   results = getProperty(uri, "studio")
   print '\rStudio is: \r'
   for result in results["results"]["bindings"]:
       print(result["studio"]["value"])

   results = getProperty(uri, "language")
   print '\rLanguage is: \r'
   for result in results["results"]["bindings"]:
       print(result["language"]["value"])

   results = getProperty(uri, "country")
   print '\rCountry is: \r'
   for result in results["results"]["bindings"]:
       print(result["country"]["value"])

   results = getProperty(uri, "budget")
   print '\rBudget is: \r'
   for result in results["results"]["bindings"]:
       print(result["budget"]["value"])
                  
if __name__ == "__main__":
   main(sys.argv[1:])
    
 
