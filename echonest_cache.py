'''
Created on Feb 2, 2013

@author: Joe G.

Cycles through every entry in chart db
and searches echonest for that artist.

If echonest top return is an exact
match for chart entry, metadata collected 
and inserted into mySQL db.

All metadata also pickled and written to local file.

'''

from pyechonest import artist, config
import MySQLdb
import sys
import time
import pickle
from matplotlib.cbook import Null


def artists_in_span(db, date1, date2, chart):
    '''Returns all artist,titles in span from specific chart
       Chart specifies if album or single'''
    c = db.cursor()
    
    query = ("""SELECT DISTINCT artist FROM {0}
                WHERE chart_date BETWEEN 
                '{1}' AND '{2}';""".format(chart,date1, date2))
    
    c.execute(query)
    result = c.fetchall();
    c.close()
    return result

def echonest_artist(artist_name, key):
    #Takes a string varable of artist name
    #queries the echonest api to get closest match.
    #Uses ID of this to ensure correct artist object'''
     
    results = artist.search(artist_name)
    
    if len(results) > 0:
        pos_artist = results[0]
        echo_id = pos_artist.id
        mb_id = pos_artist.get_foreign_id()
        terms = pos_artist.get_terms()
        time.sleep(10)
        print artist_name
        
        return 1, terms, mb_id, artist_name, echo_id            
    else:
        return 0,False,False,False,False

def main():
    
    '''Gets and caches all echonest data required for 
    Insight Data Project'''
    
    '''1. Get all artists between Date1 and Date2'''
    
    date1 = "1986-01-01"
    date2 = "2013-01-01"
    
    db_name = MySQLdb.connect(user = 'root',
                        passwd = 'xxxxxxxxx',
                        db = 'db')  
    
    config.ECHO_NEST_API_KEY = "XXXXXXXXXXXX"    
    
    artists = artists_in_span(db_name, date1, date2, 'album')
    artist_list = []
    name_cursor = db_name.cursor()
    
    for tup in artists:
        artist_list.append(tup[0])
        
    artist_object_list = []
    artist_name_list = []
    artist_id_list = []
    
    
    name_cursor.close()
    db_name.close()
    
    db = MySQLdb.connect(user = 'root',
                        passwd = 'xxxxxxxx',
                        db = 'db_new')  
        
    c = db.cursor()

    
    '''2. Get echonest artist details for each artist
    Function returns a dict which will be stored in
    a list of dicts'''
    
    
    '''s and t track and set off cache at diff points in array'''
    s = t = 1452
    output = open('singles_artist_dict.pkl', 'wb')
    
            
    for a in artist_list[t:]:
        
        '''check if it is in the db already'''
        query = ("""SELECT DISTINCT name FROM artist 
                WHERE name = "{0}";""".format(a))
                    
        try:
            c.execute(query)
            result = c.fetchall()
                        
            if len(result) == 0:
                flow = 1
            else:
                flow = 0
            
        except MySQLdb.Error, e:
            
            print repr(e)
            flow = 0
        
        
        if flow > 0:
        
            print s
            s += 1
            [a, genres, mb_id, name, echo_id] = echonest_artist(a, config.ECHO_NEST_API_KEY)
        
            if a > 0:
            
                artist_name_list.append(name)
                artist_object_list.append(genres)
                artist_id_list.append(echo_id)
        
                artist_dict = {'names':artist_name_list,
                                'cache':artist_object_list,
                                'ids':artist_id_list}
           
                pickle.dump(artist_dict, output)
            
                '''Write to SQL db as we go'''
                query = ("""INSERT INTO artist VALUES("{0}","{1}","{2}");""".
                         format(name, mb_id, echo_id))
                
                try:
                    c.execute(query)
                    inner_flow = 1
                    print query
                except MySQLdb.Error, e:
                    print repr(e)
                    inner_flow = 0    
            
                if inner_flow > 0:
            
                    for genre in genres:
                
                        query = ("""INSERT INTO genre 
                                VALUES("{0}","{1}","{2}","{3}", NULL);""".
                                format(genre['name'], genre['weight'], 
                                       genre['frequency'], echo_id))
                        
                        try:
                            c.execute(query)
                            print query
                        except MySQLdb.Error, e:
                            print repr(e)
        
                else:    
                    pass
        
    output.close()
    c.close()
    db.close()
    
    '''
    # read python dict back from the file
    pkl_file = open('myfile.pkl', 'rb')
    mydict2 = pickle.load(pkl_file)
    pkl_file.close()
    '''

if __name__ == '__main__':
    main()
