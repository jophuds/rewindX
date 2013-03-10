# -*- coding: utf-8 -*-
"""
Created on Sat Jan 29 17:39:52 2013
Retrieves chart data from musichartarchives website

@author: user
"""
import urllib2
import sys
import re
from BeautifulSoup import BeautifulSoup 
import MySQLdb
import pandas.io.sql as psql

def integrity_check():
    
    date1 = "1986-01-01"
    date2 = "2013-01-01"
    
    db = MySQLdb.connect(user = 'root', passwd = 'password', db = 'db')      
    
    query = ("""SELECT DISTINCT artist,title FROM album WHERE chart_date BETWEEN 
             '{0}' AND '{1}';""".format(date1, date2))
    
    df = psql.frame_query(query, con=db)
    
    db.close()
    
    for tup in df.itertuples():

        
        if isinstance(tup[1], basestring):
            pass
        else:
            print "Entry {0} -- Artist -- {1} -- Not a string".format(tup[0], tup[1])
    
    
        if isinstance(tup[2], basestring):
            pass
        else:
            print "Entry {0} -- Title -- {1} -- Not a string".format(tup[0], tup[1])
    
    return True

def return_chart_data(url):
    #Given a url - returns h1 and table data
    #Matches[0] identifies if Album or Single
    #Matches[3,4,5] = YYYY-MM-DD
    
    print url
    page = urllib2.urlopen(url)
    soup = BeautifulSoup(page.read())
    title = soup.find('h1')
    reg = re.compile(r'(\w+)')
    matches = reg.findall(title.string)
    #Get table with chart data and extract
    table = soup.find('table')
    rows = table.findAll('tr')
    #[Num, Title, Artist] * num of entries
    chartdata = []
    data = []
    for tr in rows:
        cols = tr.findAll('td')
        for td in cols:
            #If this empty or None then need to replace it with ''
            #this is required to ensure string.methods can be used later down the line.
            entry = td.find(text = True)
            
            if entry is None:
                chartdata.append('') 
            else:
                chartdata.append(entry.lstrip())
        
        data.append(chartdata)
        chartdata = []
        
    print data    
    return matches, data

        
def scrape_url_data(url):
    #Given url - gets all other urls in table 
    
    page = urllib2.urlopen(url)
    
    soup = BeautifulSoup(page.read())
    #Get metadata
    #[0] 1980s [1] Album
    title = soup.find('h1')
    reg = re.compile(r'(\w+)')
    matches = reg.findall(title.string)
    #Get links for charts
    table = soup.find('table')
    rows = table.findAll('tr')
    links = []
    for tr in rows:
        breaks = tr.findAll('a', href=True)
        for br in breaks:
            links.append(br['href'])
    return matches, links
     
def main():
    
    
    
    entry_urls_album = ["http://musicchartsarchive.com/album-chart/1980s",
                        "http://musicchartsarchive.com/album-chart/1990s",
                        "http://musicchartsarchive.com/album-chart/2000s",
                        "http://musicchartsarchive.com/album-chart/2010s"]
                        
    entry_urls_singles = ["http://musicchartsarchive.com/singles-chart/1980s",
                          "http://musicchartsarchive.com/singles-chart/1990s",
                          "http://musicchartsarchive.com/singles-chart/2000s",
                          "http://musicchartsarchive.com/singles-chart/2010s"]
                  
    #Get links for each link
    album_links = []
    album_links_metadata = []
    single_links = []
    single_links_metadata = []
    temp1 = temp2  = []
    
    for decade in entry_urls_album:
        print decade
        [temp1,temp2] = scrape_url_data(decade)
        album_links.append(temp2)
        album_links_metadata.append(temp1)
    
    for decade in entry_urls_singles:
        [temp1,temp2] = scrape_url_data(decade)
        single_links.append(temp2)
        single_links_metadata.append(temp1)
        
    f = open("album_links.txt", "wb")
    for link in album_links:
        #FUTURE: Check if link is valid
        f.write("%s\n" % link)
    f.close()
    
    f = open("single_links.txt", "wb")
    for link in single_links:
        #FUTURE: Check if link is valid
        f.write("%s\n" % link)
        print link
    f.close()
    
    #Get chart data
    album_data = []
    f = open("single_data.txt", "wb")
    
    for decade in single_links:
            for week in decade:
                print week
                [metadata,chartdata] = return_chart_data(week)
                album_data.append(chartdata)
                for position in chartdata:                  
                    entry = (u"{0}-{1}-{2} \t{3} \t{4} \t{5} \t{6}\n".
                            format(metadata[3], metadata[4], metadata[5], 
                            metadata[0], position[0], position[1].
                            encode('utf8'), position[2].encode('utf8')))
                    f.write(u"{0}".format(entry))
    f.close()
   

    
if __name__ == '__main__':
    main()
