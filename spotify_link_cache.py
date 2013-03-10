'''
Created on Feb 5, 2013

@author: user
'''
import json
import urllib2
import MySQLdb
import requests
import time
import pandas.io.sql as psql


def albums_in_span(db, date1, date2, chart):
    '''Returns all artist,titles in span from specific chart
       Chart specifies if album or single'''
    
    query = ("""SELECT DISTINCT artist,title FROM {0}
                WHERE chart_date BETWEEN 
                '{1}' AND '{2}';""".format(chart,date1, date2))
    
    df = psql.frame_query(query, con=db)
    
    return df

def main():

    date1 = "1986-01-01"
    date2 = "2013-01-01"
    
    db = MySQLdb.connect(user = 'root', passwd = 'password', db = 'db')      
    
    album_df = albums_in_span(db, date1, date2, 'album')
        
    db.close()

    '''Open Spotify db'''
    db = MySQLdb.connect(user = 'root', passwd = 'password', db = 'db_new')      
    c = db.cursor()

    s = t = 5267
    
    f = open("not_found.txt", "wb")
    f1 = open("spot_links.txt", "wb")
    
    for entry in album_df[t:].itertuples():
     
        f.write("\n")
        print "Search#{0} - for {1} -- {2}".format(t, entry[1], entry[2])
        t += 1
                
        url = ("http://ws.spotify.com/search/1/album.json?q={0}%20{1}".
              format(urllib2.quote(entry[1]), urllib2.quote(entry[2].strip())))
              
        print url
        time.sleep(1) 
        
        resp = requests.get(url)
                
        if resp.status_code != 200:
            # THROW APPROPRIATE ERROR
            print "No link found for {0} -- {1}".format(entry[1], entry[2])
             
        else:
                        
            d = json.loads(resp.text)            
            
            if d['info']['num_results'] > 0: 
                         
                spotify_album = d['albums'][0]
                link = spotify_album['href']

                print ("Link found for {0} -- {1} -- {2}".
                       format(entry[1], entry[2], spotify_album['href']))
                
                if unicode(spotify_album['artists'][0]['name']) == unicode(entry[1].strip()):
                    
                    print "Artists match...."
                            
                    if unicode(spotify_album['name']) == unicode(entry[2].strip()):
                            
                        print "Album titles match..."
                        
                        '''Can write this to db - get echo_id'''
                             
                        query = ("""SELECT echo_id FROM artist 
                                WHERE name = "{0}";""".format(entry[1]))
                        c.execute(query)
                        result = c.fetchall();
                        
                        if len(result) > 0:
                            '''Then echo_id is present and can write all to db'''
                            echo_id = result[0][0]    
                            link = spotify_album['href']
                            query = ("""INSERT INTO spot_link VALUES 
                                    ("{0}","{1}","{2}","{3}", "{4}");""".
                                    format(entry[1].strip(), 
                                           entry[2].strip(), 
                                           echo_id, link, 
                                           spotify_album['popularity']))   
                        
                            print query
                
                            try:
                                c.execute(query)
                                print "Written to db #{0}".format(s)
                                s += 1
                            except MySQLdb.Error, e:
                                print repr(e)
                        
                        else:
                            print "No Echo ID for artist -- not written to db"
                            f.write("Echo ID for artist not present, {0}\n".
                                    format(entry[1], entry[2]))
                        
                    else:
                        print "Albums don't match"
                        f.write("Albums don't match\n")
                        
                        try:
                            f.write("Spotify, {0}, My db, {1}\n".
                                    format(spotify_album['name'], entry[2]))
                            
                            f.write("""Spot Link, {0}, 
                                    Popularity, {1}, 
                                    Title: {2}\n""".
                                    format(link, spotify_album['popularity']
                                           , spotify_album['name']))
                            
                        except UnicodeError:
                            pass
                        
                        f1.write("{0}\n".format(link))
                else:
                    print "Artist names don't match"
                    f.write( "Artist names don't match\n")
                    
                    try:    
                        f.write("Spotify, {0}, My db, {1}\n".
                                format(spotify_album['artists'][0]['name']
                                       , entry[1].strip()))
                        
                        f.write("Spot Link, {0}, Popularity, {1}, Title: {2}\n".
                                format(link, spotify_album['popularity'], 
                                       spotify_album['name']))
                        
                    except UnicodeError:
                        pass
                        
                    f1.write("{0}\n".format(link))
                
            else:
                print "Spotify Album Call good but 0 search results\n"
                f.write("No search results for {0}, {1}\n".
                        format(entry[1], entry[2]))
                pass        
 
    f.close()   
    f1.close()
    c.close()
    db.close()
    return 1;

if __name__ == '__main__':
    main()
