#!/usr/bin/env python

import os
import MySQLdb
import json

from flask import Flask, request, render_template, app 
from pandas import DataFrame, Series
import pandas as pd
from numpy.ma.core import floor, abs
from numpy.core.numeric import nan
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt


glob_seed_artist = ""
glob_seed_album = ""

app = Flask(__name__)

@app.route('/', methods = ['POST', 'GET'])
def index():
    
    if request.method == "POST":
        
        seed_artist = request.form["artist"]
        seed_album = request.form["album"]
        
        print seed_artist
        print seed_album
        
        [testx, seed_link, seed_df, rec_list, edge_df] = driver(seed_album, 
                                                                seed_artist)
        
        if testx == -1:   
            print "Sending back {0}".format("Unknown")
            return render_template('splash.html', 
                                   var1 = "Unknown... enter artist", 
                                   var2 = "Unknown.... enter album")      
        else:

            seed_data = [seed_link, seed_df.title[0], seed_df.artist[0]] 
            print rec_list
            
            return render_template('test_new.html',  var1 = seed_data, 
                                   var2 = seed_link, var3 = rec_list, 
                                   var4 = range(0,10))
                 
    else:
        
        '''Get list of available artists for inclusion'''
        pos_artist_list = get_pos_artist_list()
        return render_template('splash.html', var1 = "Enter Artist...", 
                               var2 = "Enter album... pick artist first", 
                               artist_list = pos_artist_list, album_list="")
        




@app.route('/album', methods = ['POST', 'GET'])
def album():
    
    if request.method == "POST":
        
        seed_artist = glob_seed_artist    
        seed_album = request.form["album"]
        global glob_seed_album
        glob_seed_album = seed_album
        
        print seed_album, seed_artist
        
        [testx, seed_link, seed_df, rec_list, edge_df] = driver(seed_album, 
                                                                seed_artist)
        
        if testx == -1:   
            
            print "Sending back {0}".format("Unknown")
            return render_template('splash.html', 
                                   var1 = "Unknown... enter artist", 
                                   var2 = "Unknown.... enter album")
              
        else:

            seed_data = [seed_link, seed_df.title[0], seed_df.artist[0]] 
            print rec_list    
            return render_template('test_new.html',  var1 = seed_data, 
                                   var2 = seed_link, var3 = rec_list, 
                                   var4 = range(0,10))
                 
    else:
        pass



@app.route('/artist', methods = ['POST', 'GET'])
def artist():

    if request.method == "POST":
        
        seed_artist = request.form["artist"]
        print seed_artist
        global glob_seed_artist      
        glob_seed_artist = seed_artist
        
        '''Get possible albums users can pick from and supply'''
        pos_alb_list = get_pos_album_list(seed_artist)
        return render_template('splash.html', var1 = seed_artist, 
                               var2 = "", artist_list = "", 
                               album_list = pos_alb_list)
                 
    else:
        
        '''Get list of available artists for inclusion'''
        pos_artist_list = get_pos_album_list()
        return render_template('splash.html', var1 = "Enter Artist...", 
                               var2 = "Enter album... pick artist first", 
                               artist_list = pos_artist_list)



@app.route('/about', methods = ['POST', 'GET'])
def about():    
    return render_template("about.html")


@app.route('/feedback', methods = ['POST', 'GET'])
def feedback():
    
    '''Runs when user clicks for next album'''
    b = request.form["b"]
    print "Received b:{0}".format(b)
    return b
    


def driver(seed_album, seed_artist):
    
    '''Called from app route when artist/album both present
       Orchestrates recommendations and returns rec list'''


    db = MySQLdb.connect(user = 'root', passwd = 'xxxxxxxxx', db = 'db')  
    
    db_new = MySQLdb.connect(user = 'root', passwd = 'xxxxxxxx', db = 'db_new')
    
    
    tables = ["billboard", "album", "single"]
    
    '''1. Get artist of album'''
    seed_artist_test = get_artist(db, seed_album)
    
    if seed_artist_test == -1:
        print "Album not found in db"
        return (-1,-1,-1,-1,-1)
    else:
        pass
    
    '''1.1 Make sure artist is the same as entered artist'''
    if seed_artist != seed_artist_test:
        print "Artist name from db does not match user input"
        return (-1,-1,-1,-1,-1)
    else:
        seed_artist = seed_artist_test
        pass
    
    '''2. Get date of this album release'''
    seed_instance = first_title_entry(db, seed_artist, seed_album, "album")
        
    '''2.1 Get seed artist genres from mysql db_new '''
    seed_id = get_echo_id(db_new, seed_artist)
    
    [flow, seed_genre] = get_genre_df(seed_artist, 
                                      seed_album, seed_id, 1, db_new)
    
    print("Got genre")
    
    seed_link = get_spotify_link(seed_genre.echo_id[0], 
                                 seed_artist, seed_album)
    
    print("Got seed link")
    
    if seed_id == NaN:
        print "Artist does not have echo_id"
        return (-1,-1,-1,-1,-1)
    else:
        pass 
    
    if flow == 0:
        print "Artist does not have genre data"
        return(-1,-1,-1,-1,-1)
    else:
        pass
    
    if len(seed_link) == 0:
        print "Artist does not have genre data"
        return(-1,-1,-1,-1,-1)
    else:
        pass

    
    '''3. Get artist,title of all entrys in this span'''
    days_in_span = 250
    album_span = releases_in_span(db, seed_instance, days_in_span, tables[1])
    print ("Got releases in span")
    
    '''4. Get every [album,title] chart performance'''
    entry_performance = []
    artist_list = []
    title_list = []
    
    for entry in album_span:
        #create dataframe to store, artist, title and performance
        artist_list.append(entry[0])
        title_list.append(entry[1])
        entry_performance.append(release_perf_in_span(db, seed_instance, 
                                                      entry[0], entry[1]))
    
    
    '''5. Quantify chart performance of each title'''
    '''5.1 Place all data in dataframe'''
    span_perf = []    
    for performance in entry_performance:
        span_perf.append(release_quantification(performance))
           
    perf = np.array(span_perf)
    
    
    #This include the seed artist, title and performance    
    df = DataFrame({'artist': Series(artist_list), 
                    'title': Series(title_list), 
                    'perf':perf})
    
    '''5.2 Store seed artist/title data but set perf dead low
    so it'll at the end of df when sorted'''
    df.perf[df.artist == seed_artist] = 0.00001
    
    '''6. Perform first pass filtering of titles'''
    '''6.1 Remove low familiarity/poor chart performers'''
    perc = 40
    df_analyse =  span_remove_percentile(df, perc)
        
    '''7. Get genre details for df_analyse'''
    '''7. 1 Pull all distinct echoIDs froms db_new:genre'''    
    pos_artist_id = []
      
    for artist_name in df_analyse.artist: 
            
        pos_id = get_echo_id(db_new, artist_name)
        pos_artist_id.append(pos_id)
    
    df_analyse['echo_id'] = pos_artist_id
    
    '''7.2 Drop anything that does not have echo_id -- will equal nan'''
    df_analyse = df_analyse.dropna(axis = 0)
    df_genre = DataFrame()
    df = DataFrame()    
    
    '''7.3. Remove albums/artists that do not have a spotify link'''
    link_list = []    
    for tup in df_analyse.itertuples():
        
        link = get_spotify_link(tup[4], tup[1], tup[3])
        
        if len(link) > 0:
            link_list.append(link)
        else:
            link_list.append(NaN) 
    
    df_analyse['link'] = link_list
    df_analyse = df_analyse.dropna(axis = 0)
    
    '''Remove duplicate entries from the same artists here'''
    df_analyse = df_analyse.drop_duplicates('artist')
    
    '''For each artist pull genre name,weighting,frequencey'''
    for tup in df_analyse.itertuples():
        
        [d, df] =  get_genre_df(tup[1], tup[3], tup[4], tup[2], db_new)
        
        if  d > 0:
            df_genre = df_genre.append(df)              
        else:
            pass
                    
    '''8. Perform analysis based on genre filtering'''
    df_edge_weights = network_analysis(seed_genre, db_new, 
                                       df_genre, df_analyse)
    
    
    '''10. Manipulate and return data to caller for display'''
    rec_list = list(df_edge_weights.link[0:10])
   
    '''10.1 Make Recommend String''' 
    db.close()
    db_new.close()
    
    return 1, seed_link, seed_genre, rec_list, df_edge_weights      



def get_pos_album_list(seed_artist):

    db = MySQLdb.connect(user = 'root', passwd = 'xxxxxxxx', db = 'db_new')  
    c = db.cursor()
    
    query = """SELECT DISTINCT title FROM spot_link 
            WHERE artist ="{0}";""".format(seed_artist)
            
    c.execute(query)
    results = c.fetchall()
    
    c.close()
    db.close()
    alb_list = []
    
    for tup in results:

        new_string = tup[0]
        
        if new_string.find("'") == -1: 
            alb_list.append(new_string)
        else:
            pass
        
    alb_list = json.dumps(alb_list)
    
    return alb_list



def get_pos_artist_list():
    
    db = MySQLdb.connect(user = 'root', passwd = 'xxxxxxxx', db = 'db_new')  
    c = db.cursor()
    query = """SELECT DISTINCT artist.name FROM artist, genre 
            WHERE artist.echo_id = genre.echo_id;"""
    c.execute(query)
    results = c.fetchall()
    c.close()
    db.close()
    
    alb_list = []
    for tup in results:
        
        new_string = tup[0]
        
        if new_string.find("'") == -1: 
            alb_list.append(new_string)
        else:
            pass
        
    
    alb_list = json.dumps(alb_list)
    return alb_list



def get_spotify_link(echo_id, artist_name, title):
    
    db = MySQLdb.connect(user = 'root', passwd = 'xxxxxxxxx', db = 'db_new')      
    c = db.cursor()

    query = """SELECT link FROM spot_link 
            WHERE title = "{0}" 
            AND echo_id = "{1}";""".format(title, echo_id)
             
    sp_url = ""
    try:
        c.execute(query)
        results = c.fetchall()
                
        if len(results) > 0:
            sp_base = "https://embed.spotify.com/?uri="
            sp_url = "{0}{1}&view=coverart".format(sp_base,results[0][0])
        else:
            sp_url = ""
            print "Could not find {0} -- {1}".format(artist_name, title)
                
    except MySQLdb.Error, e:
        print repr(e)
             
    c.close()
    db.close()
    return sp_url 



def conv_to_list(result):
    
    result = map(list, result)
    result = sum(result, [])
    return result

def first_title_entry(db, artist, title, chart):
    
    '''Returns first entry date for artist title''' 
    c = db.cursor()
    
    query = ("""SELECT MIN(chart_date) FROM album 
            WHERE artist = "{0}" 
            AND title = "{1}";""".format(artist, title))
     
    c.execute(query)
    result = c.fetchall();
    c.close()
    return result[0][0]


def artist_first_album(db, artist_name, chart):
    '''Returns first artist album entry'''
    '''chart_date,title in tuple'''
      
    c = db.cursor()    
    query = ("""SELECT min(chart_date),title   
            FROM {0} WHERE artist = "{1}";""".format(chart, artist_name))
    
    c.execute(query)
    result = c.fetchall();
    c.close()
    return conv_to_list(result)
    
    
def release_performance(db, artist_name, title, chart):
    '''Returns release performance for artist,title'''
    
    c = db.cursor()
    query = ("""SELECT chart_date,position 
            FROM {0} WHERE artist = "{1}" 
            AND title = "{2}";""".format(chart,artist_name, title))
    c.execute(query)
    result = c.fetchall();
    c.close()
    '''Do not convert to list as tuple is ideal for this data'''
    return result

def releases_in_span(db, date, days, chart):
    '''Returns all artist,titles in span from specific chart
       Chart specifies if album or single'''
    
    c = db.cursor()
    
    query = ("""SELECT DISTINCT artist,title FROM {0}
                WHERE chart_date BETWEEN 
                DATE_SUB('{1}', INTERVAL {2} DAY) AND 
                DATE_ADD('{1}', INTERVAL {3} DAY)
                GROUP BY title;""".format(chart, date, days, (days*2)))
    c.execute(query)
    
    print(query)
    result = c.fetchall();
    c.close()
    return result
     

def all_albums(db, artist_names):
    '''Returns all albums for an artist'''
    '''**Alter to make it album or single'''
    
    c = db.cursor()
    query = ("""SELECT title FROM album WHERE artist = "{0}" 
            GROUP BY title;""".format(artist_names))
    c.execute(query)
    result = c.fetchall();
    c.close()
    '''don't convert data easier to use a list'''
    return conv_to_list(result)
    
def echonest_artist(artist_name, key):
    '''Returns top matching echonest artist obj'''     
    results = artist.search(artist_name)
    return results[0]
        
def span_artist(album_span, key):
    '''Returns a list of artist objects from echonest'''
    
    span_a = []
    for artist in album_span:
        a = echonest_artist(artist[1], key)
        span_a.append(a)
    return(span_a)


def get_echo_id(db, seed_artist):
    '''Get echo_id of seed_artist'''
    
    c = db.cursor()
    query = ("""SELECT echo_id FROM artist 
            WHERE name  = "{0}";""".format(seed_artist))
        
    try:
        c.execute(query)
        result = c.fetchall()
    
    except MySQLdb.Error, e:    
        result = "" 

    if len(result) == 0:        
        return nan
    elif len(result[0]) == 0:
        return nan
    else:
        echo_id = result[0][0]

    return echo_id 


def release_perf_in_span(db, date, span_artist, span_release):
    '''Returns the performance of a specific release
    in a specific timespan'''
    
    c = db.cursor()
    query = ("""SELECT chart_date,position FROM album 
            WHERE artist = "{0}"AND title = "{1}";""".
            format(span_artist, span_release))
    
    try:       
        c.execute(query)
        result = c.fetchall()
        
    except MySQLdb.Error, e:
        result = "" 
    
    return result

def span_remove_percentile(df, perc):
    '''Takes df of artists, title and chart performance 
    and removes the botton and/or top percentile (perc)'''
    
    new_df = df.sort('perf', ascending = 0)
    
    if perc > 0:
        
        y = np.floor(len(new_df)/100 * perc)
        new_df = new_df[0:(len(new_df) - y)]
        new_df = new_df.sort('perf', ascending = 0)
        return new_df
    
    else:
        return new_df

def release_quantification(performance):
    '''Quantifies the peformance of a series of chart dates and position'''
    
    perf = 0.0
    count = 0.0
    
    if len(performance) > 0:
     
        for entry in performance:
            position = entry[1].strip()
            position = float(position)
        
            if position == 0:
                perf = 0
            elif position < 10:    
                perf += 1/0.9
                count += 1
            elif position > 9 and position < 20:    
                perf += 1/0.8
                count += 1
            elif position > 19 and position < 30:    
                perf += 1/0.7
                count += 1    
            elif position > 29 and position < 40:    
                perf += 1/0.6
                count += 1
            elif position > 39 and position < 50:    
                perf += 1/0.5
                count += 1         
    else:
        perf = 0
    
    if perf > 0:        
        return perf*count
    else:
        return 0
        
        
def get_artist(db, album_title):
    '''Returns the artist of a specific release in album db'''
    
    c = db.cursor()
    query = ("""SELECT DISTINCT artist 
            FROM album WHERE title = "{0}";""".format(album_title))
    
    try:
        c.execute(query)
        results = c.fetchall()
        print "bd:artist queried for album:{0}".format(album_title)
    except MySQLdb.Error, e:
        print repr(e)
        
    if len(results) == 0:        
        return -1
    elif len(results[0]) == 0:
        return -1
    else:
        return results[0][0]
    

def top_genre_comp(test_df, seed_df, g1, g2):
    '''Compare the top genres in two diff genre dfs'''
            
    sum_edge_weight = 1

    for genre in seed_df.genre[g1: g2]:                
        sum_edge_weight = 0
            
        if any(test_df.genre == genre):
                                
            seed_wf = seed_df.weight[seed_df.genre == genre]
            seed_wf = seed_wf.values[0]
                    
            test_wf = test_df.weight[test_df.genre ==  genre] 
            test_wf = test_wf.values[0]
            
            diff = seed_wf/test_wf
            m1 = abs(1 - diff)
            
            if m1 == 0:
                m1 = 1
            else:
                pass
                                    
        else:   
            m1 = 1
                        
        sum_edge_weight += m1

    this_edge_weight = 1/sum_edge_weight    
    return this_edge_weight



def get_g_num(seed_df, start_index):
    '''Find number of genres making 70% cumsum from start point'''
        
    val = seed_df[start_index:].wf.cumsum()
    summing = list(val/seed_df[start_index:].wf.sum())
        
    for t in range(0, len(summing)):
                    
        if summing[t] >= 0.7:
            return t
        else:
            pass


def network_analysis(seed_df, db_new, df, df_link):
    '''Returns DF with weight column of nearest artists'''
                   
    distinct_id = list(np.unique(df.echo_id))
    
    '''Should not need to remove seed id, 
    as it should have been removed before this'''
    #distinct_id.remove(seed_df.echo_id)
    
    G = nx.Graph()
    
    dis_edge_df = DataFrame()
    seed_df = seed_df.sort('wf', ascending = False)

    artist_sum_list = []
    echo_id_sum_list = []
    title_sum_list = []
    sum_weight_list = []
    sum_link_list = []
    
    '''Need to identify what type of genre profile the 
       seed artist has == sets g1 and g2'''
    start = 0
    g1 = get_g_num(seed_df, start)
    
    if g1 <= 3:
        com_num = g1
    else:
        com_num = g1 -1         
    
    mid_seed_df = seed_df[g1+1:]
    g = get_g_num(mid_seed_df, 0)
    g2 = g + g1
        
    seed_artist = seed_df.artist[0] 
    seed_album = seed_df.title[0]
    
    for dis_id in distinct_id:
        
        '''It is possible that an artist may have more 
        than one album in df_genre dataframe. Need to 
        ensure that each album is considered seperately'''
        
        count = 0    
        test_df_all = df[df.echo_id == dis_id]
        test_df_title_list = list(np.unique(test_df_all.title))
        link = df_link.link[df_link.echo_id == dis_id]
        link = list(link)
        link = link[0]
        
        for title in test_df_title_list:
            
            test_df = test_df_all[test_df_all.title == title]
            test_df = test_df.sort('wf', ascending = False)
            test_df_artist = test_df.artist[0]
        
            flow = 0
        
            if isinstance(test_df_artist, basestring):
            
                print "Genre comp for {0}".format(test_df_artist)
                flow = 1
            
            else:
             
                test_name = np.unique(test_df.artist)
            
                try:
                    test_df_artist = test_name[0]
                    flow = 1    
                except KeyError:
                    print "Artist not stored correctly... cannot be analysed"
                    flow = 0
        
            '''Perform no analysis... simply remove as less 
            than 3 common genres in seed top 5 genres'''
            com = len(np.intersect1d(test_df.genre, seed_df.genre[:g1]))
             
        
            if flow > 0 and com >= com_num:
            
                weight_top_genres = top_genre_comp(test_df, 
                                                   seed_df, 0, g1) * g1
                                                   
                weight_mid_genres = top_genre_comp(test_df, 
                                                   seed_df, g1, 
                                                   len(seed_df)-g2) * g2
                                                    
                weight_bot_genres =  top_genre_comp(test_df, 
                                                   seed_df, 
                                                   g2, len(seed_df))
                
                weight_bot_genres = weight_bot_genres * (len(seed_df)-g2)
                
                ''' Discard top weight 2/16/13 -- skews recs'''
                weight = weight_mid_genres + weight_bot_genres
            
            
                print "Node added - {0} & {1} - {2}".format(seed_artist, 
                                                            test_df_artist, 
                                                            weight)
                
                '''Need to ensure nodes are added between 
                seed_artist album and possible_artist album'''
            
                sum_weight_list.append(weight)
                echo_id_sum_list.append(dis_id)
                artist_sum_list.append(test_df_artist)
                title_sum_list.append(title)
                sum_link_list.append(link)
            
                try:
                    
                    G.add_edge(seed_artist, 
                               test_df_artist, {'weight': (weight)})
                    
                except TypeError:
                    print "Error adding nodes -- Type Error..."
                    pass       
            
        else:
            pass
        
    edge_df = DataFrame({'echo_id':echo_id_sum_list, 
                         'weight':sum_weight_list, 
                         'title':title_sum_list, 
                         'artist':artist_sum_list, 
                         'link':sum_link_list})
    
    edge_df_sorted = edge_df.sort('weight', ascending = False)

    #nx.draw(G)
    #plt.show()
        
    return edge_df_sorted




def get_genre_df(artist_name, title, echo_id, perf, db_new):
    '''Receives artist, title and echo_id'''
    '''Returns genre dataframe'''
        
    c = db_new.cursor()
    query = """SELECT name,weight,freq FROM genre 
            WHERE echo_id = "{0}";""".format(echo_id)
        
    try:
        c.execute(query)
        result = c.fetchall()
        print "db_new:genre acquired for :{0}".format(title)
            
    except MySQLdb.Error, e:
        print repr(e)
        result = ""
    
    c.close()    
    t = len(result)
    
    if t > 0:
        
        title_list = []
        echo_id_list = []
        artist_list_pos = []
        perf_list = [] 
        
        genre_name_list = []
        genre_weight_list = []
        genre_freq_list = []
        
        '''if band has less than 3 genre tags then
        remove this band from the df at this point'''
        
         
        for tup in result:
                
            genre_name_list.append(tup[0])
            genre_weight_list.append(tup[1])
            genre_freq_list.append(tup[2])
            perf_list.append(perf)
            title_list.append(title)
            echo_id_list.append(echo_id)
            artist_list_pos.append(artist_name)             
        
            
        df_artist = DataFrame({'genre': genre_name_list, 
                               'weight': genre_weight_list,
                               'freq': genre_freq_list, 
                               'echo_id': echo_id_list,
                               'title': title_list, 
                               'artist': artist_list_pos, 
                               'perf':perf_list})
        
        df_artist['wf'] = df_artist.weight * df_artist.freq
        df_artist.sort('wf', ascending = False)
            
        if len(df_artist) > 5:         
            return 1, df_artist
        
        else:    
            return 0,0           
    else:
        return 0,0

    
if __name__ == '__main__':
    '''Set debug to False if running on AWS '''
    port = int(os.environ.get('PORT', 8080))
    app.debug = False
    app.run(host='0.0.0.0', port=port)
