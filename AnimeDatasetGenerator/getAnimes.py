'''
This script can be used to download anime dataset from [**Myanimelist**](https://myanimelist.net/) using an unofficial MyAnimeList REST API, [**Jikan**](https://jikan.me/docs).
Column metadata:
animeID: id of anime as in anime url https://myanimelist.net/anime/ID
name: title of anime
premiered: premiered on. default format (season year)
genre: list of genre
type: type of anime (example TV, Movie etc)
episodes: number of episodes
studios: list of studio
source: source of anime (example original, manga, game etc)
scored: score of anime
scoredBy: number of member scored the anime
members: number of member added anime to their list
'''

# importing libraries
import requests
import json
import csv
import sys
import time
from numpy import genfromtxt

dataFile = 'ID_SEARCH_repescagem.csv'

id_array = genfromtxt(dataFile,skip_header=1,dtype = int)

i = 0
k = 7

outputFile = 'AnimeList_' + str(k+1).zfill(2) + '.csv'
errorfile = 'error_' + str(k+1).zfill(2) + '.csv'

# opening output file for writing
w = open(outputFile, 'w', encoding="utf-8")
e = open(errorfile, 'w', encoding="utf-8")

# header
w.write('animeID, request_cached, title, title_english, title_japanese, \
title_synonyms, image_url, type, source, episodes, status, airing, aired, \
duration, rating, score, scored_by, rank, popularity, members, favorites, \
background, premiered, broadcast, related, producer, licensor, studio, \
genre, opening_theme, ending_theme\n')
e.write('animeID, status, type, message, error\n')
# creating csv writer object
writer = csv.writer(w)
error_writer = csv.writer(e)

while i < (len(id_array)-1):
    
    try:

    	apiUrl = 'http://api.jikan.moe/v3/anime/' + str(id_array[i]) # base url for API
    	print(f'Reading anime id {id_array[i]}')
    	# note: for SSL use 'https://api.jikan.me/'. For more go here 'https://jikan.me/docs'
	
    	# API call
    	time.sleep(1)
    	#page = requests.get(apiUrl)
    	page = requests.get(apiUrl, stream = True)

    	

    	# Decoding JSON

    	print('    Fetching JSON...')
    	c = page.content
    	jsonData = json.loads(c)
    	if(page.status_code == 200):

    		print(f'\n    Writing to file {outputFile}...') # Message

    		l = [] # List to store the data to write
    		name = jsonData['title']

    		print(f"        Reading, {name}, animelist...") # printing title to screen

    		l.append(id_array[i]) # anime ID
    		l.append(jsonData['request_cached'])
    		l.append(jsonData['title'])
    		l.append(jsonData['title_english'])
    		l.append(jsonData['title_japanese'])
    		l.append(str(jsonData['title_synonyms']))
    		l.append(jsonData['image_url'])
    		l.append(jsonData['type'])
    		l.append(jsonData['source'])
    		l.append(jsonData['episodes'])
    		l.append(jsonData['status'])
    		l.append(jsonData['airing'])
    		l.append(str(jsonData['aired']))
    		l.append(jsonData['duration'])
    		l.append(jsonData['rating'])
    		l.append(jsonData['score'])
    		l.append(jsonData['scored_by'])
    		l.append(jsonData['rank'])
    		l.append(jsonData['popularity'])
    		l.append(jsonData['members'])
    		l.append(jsonData['favorites'])
    		l.append(str(jsonData['background']))
    		l.append(jsonData['premiered'])
    		l.append(jsonData['broadcast'])
    		l.append(str(jsonData['related']))
    		l.append(str(jsonData['producers']))
    		l.append(str(jsonData['licensors']))
    		l.append(str(jsonData['studios']))
    		l.append(str(jsonData['genres']))
    		l.append(str(jsonData['opening_themes']))
    		l.append(str(jsonData['ending_themes']))
    		l = [l] # help in writing using csv.writer.writerows

    		print('        Writing anime', name)
    		print('        Index of anime to be written:', id_array[i])
    		print(f'        iteration {i+1} of {len(id_array)}\n')
    		writer.writerows(l) # writing one row in the CSV

    	else:
    		print(f"    Id {id_array[i]} had code return {jsonData['status']}")
    		print(f'    iteration {i+1} of {len(id_array)}\n')

    		error_list = []
    		error_list.append(id_array[i]) # anime ID
    		error_list.append(jsonData['status'])
    		error_list.append(jsonData['type'])
    		error_list.append(jsonData['message'])
    		error_list.append(jsonData['error'])
    		error_list = [error_list] # help in writing using csv.writer.writerows

    		error_writer.writerows(error_list) # writing one row
    	i += 1				
    except:
        print(f"    Unexpected error on id {id_array[i]}: {sys.exc_info()[0]}\n")
        w.close() # closing file
        e.close()
        time.sleep(60)
        k += 1
        outputFile = 'AnimeList_' + str(k+1).zfill(2) + '.csv'
        errorfile = 'error_' + str(k+1).zfill(2) + '.csv'

        # opening output file for writing
        w = open(outputFile, 'w', encoding="utf-8")
        e = open(errorfile, 'w', encoding="utf-8")

        # header
        w.write('animeID, request_cached, title, title_english, title_japanese, \
        title_synonyms, image_url, type, source, episodes, status, airing, aired, \
        duration, rating, score, scored_by, rank, popularity, members, favorites, \
        background, premiered, broadcast, related, producer, licensor, studio, \
        genre, opening_theme, ending_theme\n')
        e.write('animeID, status, type, message, error\n')
        # creating csv writer object
        writer = csv.writer(w)
        error_writer = csv.writer(e)
        continue

w.close() # closing file
e.close()

print(f'    No more anime left on extraction {k+1}.\
    \n    Output file:{outputFile}.\
    \n    Total Anime stored in  the session: {count}')
