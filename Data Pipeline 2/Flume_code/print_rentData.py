#print_rentData.py starts here 
import urllib2
response = urllib2.urlopen('<url for api call>')
html = response.read()
print(html) 
#print_rentData.py ends here 

