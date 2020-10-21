"""
This script can be used to download users anime list from [**Myanimelist**](https://myanimelist.net/) 
using an unofficial MyAnimeList REST API, [**Jikan**](https://jikan.me/docs) in .json files.

Using the api URL https://api.jikan.moe/v3/user/'user_name'/animelist/all/'page', it's possibile to get
the animes' list from the user list, with the watch status, user score and many other data anime.
"""

import requests
import json
import time
import os
import pandas as pd
import csv
import sys
from numpy import genfromtxt

user_file = 'users_filtered.csv'

user_array = genfromtxt(user_file,skip_header=1,dtype = 'str' ,encoding='utf-8')

folders = 40
qtt_max_folder = 100
#folders = 1
#qtt_max_folder = 10
i = 23
j = 35
count_user = i+j*qtt_max_folder
k = 1

while j < folders:
    
    try:
        print(f"Folder {j+1} of {folders}")
		
        errorfile = 'user_error_' + str(j) + '.csv'
        e = open(errorfile, 'w', encoding="utf-8")
        e.write('user, status, type, message, error\n')
        error_writer = csv.writer(e)
    
        while i < qtt_max_folder and count_user < len(user_array):
        
            count_user = i+j*qtt_max_folder
            user = user_array[count_user]
            
            page_aux = 1
            print(f"    Reading user '{user}'.\n    User count: {count_user+1} of {len(user_array)}")
            
            while page_aux != 0:
                
                apiUrl = 'https://api.jikan.moe/v3/user/' + user + '/animelist/all/'+ str(k)
                time.sleep(4)
                page = requests.get(apiUrl, stream = True)
                print('            Fetching JSON...')
                c = page.content
                jsonData = json.loads(c)
                
                if(page.status_code == 200):
                    jsonData = json.loads(c)
                    print(f'            Reading page {k}')
                    
                    if len(jsonData['anime']) > 0:
                        file_name = user + '_' + str(k) + '.json'
                        data_path = os.path.abspath(os.getcwd()) + '\\data\\' + str(j+1) + '\\'
                        
                        with open(data_path + file_name, 'w') as json_file:
                            json.dump(jsonData, json_file)
                        print(f'            Page {k} saved\n')
                            
                        k += 1
                        
                    else:
                        page_aux = 0
                        print(f'            No data on page {k}\n')
                        k = 1
                        
                else:
                    print(f"    User {user} had code return {jsonData['status']}\n")
                    
                    error_list = []
                    error_list.append(user) # user name
                    error_list.append(jsonData['status'])
                    error_list.append(jsonData['type'])
                    error_list.append(jsonData['message'])
                    error_list.append(jsonData['error'])
                    error_list = [error_list] # help in writing using csv.writer.writerows
                    
                    error_writer.writerows(error_list) # writing one row
                    
                    page_aux = 0
                    
            i += 1
        j += 1
        i = 0
        
    except:
        print(f"    Unexpected error on user '{user}': {sys.exc_info()[0]}\n")
        continue
e.close()
print("End of process")