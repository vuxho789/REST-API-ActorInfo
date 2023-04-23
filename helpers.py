# -*- coding: utf-8 -*-
# Python 3.8.5
"""

This program was written by Vu Ho
Created on Mon 21 March 2022

"""
import re
import sqlite3
from pandas.io import sql
import requests
from flask import request, send_file
from datetime import datetime, timedelta
from math import ceil
import matplotlib.pyplot as plt
from actors_db import connect_db


# ----- DATABASE HELPER FUNCTIONS ------
def add_actor(actor, showlist, conn):
    try:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO Actors (name, tvmazeId, country, birthday, deathday, gender, lastUpdate) VALUES (?, ?, ?, ?, ?, ?, ?)",\
                    (actor['name'], actor['id'], actor['country'], actor['birthday'],\
                     actor['deathday'], actor['gender'], actor['lastUpdate']))
        actor_db_id = cur.lastrowid
        
        # If the list of shows is not empty
        if showlist:
            for show in showlist:
                cur.execute("INSERT OR IGNORE INTO ActorInShows (actor_id, showName) VALUES (?, ?)",\
                            (actor_db_id, show))
                    
        conn.commit()
        
    except sqlite3.Error as err:
        print(f'Adding record error: {err}') 
        conn().rollback()

    finally:
        cur.close()
    
    return actor_db_id


def check_existed_actor(actor_name, conn):
    cur = conn.cursor()
    
    # Checking if the table is empty
    cur.execute("SELECT EXISTS (SELECT 1 FROM Actors)")
    
    # row = 1 if Actors table is not empty, 0 if empty
    row = cur.fetchall()[0][0]
    
    # Table is not empty
    if row:
        # Checking if actor already exists in table
        cur.execute("SELECT * FROM Actors WHERE lower(name) = ?", (actor_name,))
        
        # Getting list of matching actors
        match = cur.fetchall()
        cur.close()
        
        # No matching actor found
        if not match:
            return False
        
        # Actor exists in the table
        else:
            return True
    else:
        # Table is empty
        return False
    

def get_actor_by_id(actor_id, conn):
    actor = {}
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM Actors WHERE id = ?", (actor_id,))
    row = cur.fetchone()

    # Converting row object to dictionary
    if row:
        actor["id"] = row["id"]
        actor["name"] = row["name"]
        actor["tvmazeId"] = row["tvmazeId"]
        actor["country"] = row["country"]
        actor["birthday"] = row["birthday"]
        actor["deathday"] = row["deathday"]
        actor["gender"] = row["gender"]
        actor["lastUpdate"] = row["lastUpdate"]
    
    cur.close()
    return actor


def get_shows_by_id(actor_id, conn):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM ActorInShows WHERE actor_id = ?", (actor_id,))
    rows = cur.fetchall()

    if rows:
        shows = [row['showName'] for row in rows]  
    else:
        shows = []
    
    cur.close()
    return shows

    
def update_actor_by_id(actor_id, new_info, new_shows, conn):
    try:
        cur = conn.cursor()
        cur.execute("UPDATE Actors SET name = ?, tvmazeId = ?, country = ?, birthday = ?, deathday = ?, gender = ? \
                    WHERE id = ?",\
                    (new_info['name'], new_info['id'], new_info['country'], new_info['birthday'],\
                     new_info['deathday'], new_info['gender'], actor_id,))
        
        # Checking if the shows are changed
        if new_shows.sort() != get_shows_by_id(actor_id, conn).sort():
            # Deleting all current shows associated with the actor
            cur.execute("DELETE FROM ActorInShows WHERE actor_id = ?", (actor_id,))
            
            # If list of new shows is not empty
            if new_shows:
                # Adding the new list of shows
                for show in new_shows:
                    cur.execute("INSERT OR IGNORE INTO ActorInShows (actor_id, showName) VALUES (?, ?)",\
                                (actor_id, show))
                    
        conn.commit()
        
    except sqlite3.Error as err:
        print(f'Updating record error: {err}') 
        conn().rollback()

    finally:
        cur.close()    

    
def get_all_actors(lst_filters, lst_orders, conn):
    modified_filters = [item for item in lst_filters if item != 'shows']
    filter_str = ', '.join(str(item) for item in modified_filters)
    order_str = ', '.join(str(item) for item in lst_orders)
    
    lst_actors = []
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(f"SELECT {filter_str} FROM Actors ORDER BY {order_str}")
        rows = cur.fetchall()
    
        # Converting rows into list of actors
        for row in rows:
            actor = {}
            # Converting row objects to dictionary
            actor = {key:value for key, value in zip(modified_filters, row)}
            
            if 'shows' in lst_filters:
                actor['shows'] = get_shows_by_id(row['id'], conn)
            
            lst_actors.append(actor)

    except:
        lst_actors = []
    cur.close()
    return lst_actors


# ----- API HELPER FUNCTIONS ------
def add_new_actor(input_name):
    # Pre-processing input name
    junk_characters = re.compile(r'[^a-zA-Z]')
    name = re.sub(junk_characters, ' ', input_name).lower()
    
    # Retrieving actor information from TV Maze
    request_url = f'https://api.tvmaze.com/search/people?q={name}'
    tvm_response = requests.get(request_url).json()
    
    if len(tvm_response) == 0:
        return {'message': f'Actor {input_name} does not exist'}, 404
    
    elif tvm_response[0]['person']['name'].lower() != name:
        return {'message': f'Actor {input_name} does not exist'}, 404
    
    else:
        # Checking if actor exists in the db
        actor_rawinfo = tvm_response[0]['person']
        tvm_actorid = actor_rawinfo['id']
        conn = connect_db()
        
        # Checking if actor already exists in the Database
        if check_existed_actor(name, conn):
            return {'message': f'Actor {input_name} already exists'}, 400
        else:
            # Getting a dictionary of record with keys are Actors table schema
            actor_record ={key:value for key, value in actor_rawinfo.items() \
                           if key in {'name', 'id', 'country', 'birthday', 'deathday', 'gender'}}
                
            now = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')   
            actor_record['lastUpdate'] = now
            
            for key in actor_record.keys():
                # Updating None Type values in actor_record to string 'NULL'
                if actor_record[key] == None:
                    actor_record[key] = 'NULL'
                # Updating country values to country_name
                elif actor_record[key] != None and key == 'country':
                    country_name = actor_record['country']['name']
                    actor_record[key] = country_name   
            
            # Getting a list of actor's shows from TV Maze using the tvm_actorid
            show_record = []
            shows_request_url = f'https://api.tvmaze.com/people/{tvm_actorid}/castcredits?embed=show'
            tvm_shows_response = requests.get(shows_request_url).json()
            for show in tvm_shows_response:
                show_name = show['_embedded']['show']['name']
                show_record.append(show_name)
            
            # Adding records into Database
            actor_id = add_actor(actor_record, show_record, conn)
            conn.close()
            
            # Creating self link to actor
            actor_link = {"self": {"href": f"http://{request.host}/actors/{actor_id}"}}
            
            # Creating an API response if adding actor is successful
            api_response = {"id": actor_id,
                            "last-update": now,
                            "_links": actor_link
                            }
            return api_response, 201
        
        
def get_all_actors_paginated(input_order, input_page, input_size, input_filter):
    order_options = ['id', 'name', 'country', 'birthday', 'deathday', 'last-update']
    filter_options = ['id', 'name', 'country', 'birthday', 'deathday', 'last-update', 'shows']
    
    # Checking query parameters
    if input_page < 1:
        return {'message': f'Page number {input_page} is invalid'}, 400
    elif input_size < 1:
        return {'message': f'Size {input_page} is invalid'}, 400
    
    modified_order = []
    for item in input_order:
        if item[0] not in ['+', '-'] or item[1:] not in order_options:
            return {'message': f'Order criteria {item} is invalid'}, 400
        
        if item.startswith('+'):
            modified_item = f'{item[1:]} ASC'
            modified_order.append(modified_item)
            
        if item.startswith('-'):
            modified_item = f'{item[1:]} DESC'
            modified_order.append(modified_item)
        
    for item in input_filter:
        if item not in filter_options:
            return {'message': f'Filtering attribute {item} is invalid'}, 400
        
    conn = connect_db()
    cur = conn.cursor()

    # Checking if the table is empty
    cur.execute("SELECT EXISTS (SELECT 1 FROM Actors)")

    # Return 1 if Actors table is not empty, 0 if empty
    check_db_empty = cur.fetchall()[0][0]
    cur.close()
    
    if not check_db_empty:
        return {'message': 'There is no actor in the database'}, 404
        
    else:
        actors_list = get_all_actors(input_filter, modified_order, conn)
        conn.close()
        
        nb_of_page = ceil(len(actors_list)/int(input_size))
        
        if input_page > nb_of_page:
            return {'message': f'Page {input_page} is out of range (Maximum page number is {nb_of_page})'}, 400
        
        else:
            start_index = (input_page - 1)*input_size
            end_index = (start_index + input_size) if input_page*input_size < len(actors_list) else len(actors_list)
            
            output_actors = actors_list[start_index : end_index]
            output_order = ','.join(input_order)
            output_filter = ','.join(input_filter)
            
            self_link = {"href": f"http://{request.host}/actors?order={output_order}&page={input_page}&size={input_size}&filter={output_filter}"}
            previous_link = {"href": f"http://{request.host}/actors?order={output_order}&page={input_page-1}&size={input_size}&filter={output_filter}"}
            next_link = {"href": f"http://{request.host}/actors?order={output_order}&page={input_page+1}&size={input_size}&filter={output_filter}"}
            
            # Checking if the query page is the only page
            if input_page == 1 and input_page == nb_of_page:
                output_links = {"self": self_link
                                }
            
            # Checking if the query page is the first page
            elif input_page == 1 and input_page < nb_of_page:
                output_links = {"self": self_link,
                                "next": next_link
                                }
                
            # Checking if the query page is the last page
            elif input_page > 1 and input_page == nb_of_page:
                output_links = {"self": self_link,
                                "previous": previous_link
                                }
                
            else:
                output_links = {"self": self_link,
                                "previous": previous_link,
                                "next": next_link
                                }                    
                
            api_response = {"page": input_page,
                            "page-size": input_size,
                            "actors": output_actors,
                            "_links": output_links}
            return api_response, 200
        
        
def get_actor(id):
    conn = connect_db()
    # Getting a dictionary of actor information from DB
    actor_from_db = get_actor_by_id(id, conn)
    # Getting a list of shows from DB
    shows_from_db = get_shows_by_id(id, conn)
    
    # Getting next row (ID) from DB
    # (Note: the next row may not be id+1 if some deletions occurred previously)
    cur = conn.cursor()
    cur.execute("SELECT * FROM Actors WHERE id > ? LIMIT 1", (id,))
    next_row = cur.fetchone()
    
    # Getting previous row (ID) from DB
    # (Note: the previous row may not be id-1 if some deletions occurred previously)
    cur.execute("SELECT * FROM Actors WHERE id < ? ORDER BY id DESC LIMIT 1", (id,))
    previous_row = cur.fetchone()
    cur.close()
    conn.close()
    
    # Checking if actor (ID) not exists in the DB
    if not actor_from_db:
        return {'message': f'Actor with id {id} is not found'}, 404
    else:
        # Checking if the ID is the first row
        if not previous_row:
            next_id = next_row['id']
            links = {"self": {"href": f"http://{request.host}/actors/{id}"},
                     "next": {"href": f"http://{request.host}/actors/{next_id}"}
                     }
            
        # Checking if the ID is the last row
        elif not next_row:
            previous_id = previous_row['id']
            links = {"self": {"href": f"http://{request.host}/actors/{id}"},
                     "previous": {"href": f"http://{request.host}/actors/{previous_id}"}
                     }
            
        else:
            previous_id = previous_row['id']
            next_id = next_row['id']
            links = {"self": {"href": f"http://{request.host}/actors/{id}"},
                     "previous": {"href": f"http://{request.host}/actors/{previous_id}"},
                     "next": {"href": f"http://{request.host}/actors/{next_id}"}
                     }
        
        # Creating an API response if retrieving the actor successfully
        api_response = {"id": id,
                        "last-update": actor_from_db['lastUpdate'],
                        "name": actor_from_db['name'],
                        "country": actor_from_db['country'],
                        "birthday": actor_from_db['birthday'],
                        "deathday": actor_from_db['deathday'],
                        "gender": actor_from_db['gender'],
                        "shows": shows_from_db,
                        "_links": links
                        }
        return api_response, 200
    
    
def delete_actor(id):
    conn = connect_db()
    # Getting a dictionary of actor information from DB
    actor_from_db = get_actor_by_id(id, conn)
    
    # Checking if actor (ID) not exists in the DB
    if not actor_from_db:
        return {'message': f'Actor with id {id} is not found'}, 404
    else:
        cur = conn.cursor()
        cur.execute("DELETE FROM Actors WHERE id = ?", (id,))
        cur.execute("DELETE FROM ActorInShows WHERE actor_id = ?", (id,))
        conn.commit()
        cur.close()
        conn.close()
        api_response = {"message": f"The actor with id {id} was removed from the database!",
                        "id": id
                        }
        return api_response, 200
    
    
def update_actor(id, actor_model):
    # Getting the payload and converting it to a JSON
    actor = request.json
    
    conn = connect_db()
    # Getting a dictionary of actor information from DB
    actor_from_db = get_actor_by_id(id, conn)
    
    # Checking if actor (ID) not exists in the DB
    if not actor_from_db:
        return {'message': f'Actor with id {id} is not found'}, 404
    
    new_actor_record = {key: value for key, value in actor_from_db.items()}
    new_show_record = get_shows_by_id(id, conn)
    for key in actor:
        # Checking if the attribute in the payload is valid
        if key not in actor_model.keys():
            return {"message": f"Attribute {key} is invalid"}, 400
        
        # If the actor's name is changed, the tvmazeid associated with the old name is invalid -> change to NULL
        elif key == 'name':
            new_actor_record[key] = actor[key]
            new_actor_record['tvmazeId'] = 'NULL'
        
        elif key == 'shows':
            new_show_record = actor[key]
            
        else:
            new_actor_record[key] = actor[key]
    
    now = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')   
    new_actor_record['lastUpdate'] = now
    
    # Updating new information to the actor
    update_actor_by_id(id, new_actor_record, new_show_record, conn)
    conn.close()
    
    # Creating self link to actor
    actor_link = {"self": {"href": f"http://{request.host}/actors/{id}"}}
    
    # Creating an API response if adding actor is successful
    api_response = {"id": id,
                    "last-update": now,
                    "_links": actor_link
                    }
    return api_response, 200


def get_stat_summary(input_format, input_attributes):
    attribute_options = ['country', 'birthday', 'gender', 'life_status']
    
    modified_attributes = []
    for item in attribute_options:
        if item != 'life_status':
            modified_attributes.append(item)
        else:
            modified_attributes.append('deathday')
    
    for item in input_attributes:
        if item not in attribute_options:
            return {'message': f'Filtering attribute {item} is invalid'}, 400
        
    conn = connect_db()
    cur = conn.cursor()

    # Checking if the table is empty
    cur.execute("SELECT EXISTS (SELECT 1 FROM Actors)")

    # Return 1 if Actors table is not empty, 0 if empty
    check_db_empty = cur.fetchall()[0][0]

    if not check_db_empty:
        return {'message': 'There is no actor in the database'}, 404
    else:
        # Getting total number of actors
        cur.execute("SELECT COUNT(*) FROM Actors")
        total_actors = cur.fetchone()[0]
        
        # Getting total number of actors updated in the last 24 hours
        last_24 = datetime.now() - timedelta(hours = 24)
        last_24_str = last_24.strftime('%Y-%m-%d-%H:%M:%S')
        cur.execute("SELECT COUNT(*) FROM Actors WHERE lastUpdate >= ?", (last_24_str,))
        updates_last_24 = cur.fetchone()[0]
        
        # Getting data from DB into a DataFrame based on the attributes
        str_attributes = ', '.join(modified_attributes)
        result_df = sql.read_sql(f"SELECT {str_attributes} FROM Actors", conn)
        cur.close()
        conn.close()
        
        output_dict = {}
        if 'country' in input_attributes:
            country_df = result_df[['country']].copy()
            country_df = country_df.groupby('country').size().reset_index(name='count')
            country_df['percent'] = round((country_df['count']/country_df['count'].sum())*100,1)
            country_df.replace('NULL', 'Unknown', inplace = True)
            
            country_df.drop('count', axis = 1, inplace = True)
            country_dict = country_df.set_index('country').T.to_dict('records')
            output_dict['country'] = country_dict[0]
        
        if 'birthday' in input_attributes:
            birthday_df = result_df[['birthday']].copy()
            birthday_df['year'] = birthday_df['birthday'].str.slice(0,4)
            birthday_df = birthday_df.groupby('year').size().reset_index(name='count')
            birthday_df['percent'] = round((birthday_df['count']/birthday_df['count'].sum())*100,1)
            birthday_df.replace('NULL', 'Unknown', inplace = True)
            
            birthday_df.drop('count', axis = 1, inplace = True)
            birthday_dict = birthday_df.set_index('year').T.to_dict('records')
            output_dict['birthday'] = birthday_dict[0]
        
        if 'gender' in input_attributes:
            gender_df = result_df[['gender']].copy()
            gender_df = gender_df.groupby('gender').size().reset_index(name='count')
            gender_df['percent'] = round((gender_df['count']/gender_df['count'].sum())*100,1)
            gender_df.replace('NULL', 'Unknown', inplace = True)
            
            gender_df.drop('count', axis = 1, inplace = True)
            gender_dict = gender_df.set_index('gender').T.to_dict('records')
            output_dict['gender'] = gender_dict[0]
            
        if 'life_status' in input_attributes:
            deathday_df = result_df[['deathday']].copy()        
            deathday_df.loc[(deathday_df['deathday'] != 'NULL'), 'deathday'] = 'Deceased'
            deathday_df.replace('NULL', 'Alive', inplace = True)
            deathday_df = deathday_df[['deathday']].value_counts().reset_index(name='count')
            deathday_df['percent'] = round((deathday_df['count']/deathday_df['count'].sum())*100,1)
            
            deathday_df.drop('count', axis = 1, inplace = True)
            deathday_dict = deathday_df.set_index('deathday').T.to_dict('records')
            output_dict['life_status'] = deathday_dict[0]
        
        if input_format == 'json':
            api_response = {"total": total_actors,
                            "total-updated": updates_last_24}
            for key in input_attributes:
                api_response[f'by-{key}'] = output_dict[key]
                
            return api_response, 200
                
        else:
            plt.figure(figsize = (20, 15))
            nb_of_plots = len(output_dict) + 1
            
            # Plotting actors by update status
            plt.subplot(1, nb_of_plots, 1)
            labels = ['Not updated', 'Updated last 24 hours']
            values = [total_actors - updates_last_24, updates_last_24]
            plt.pie(x = values, labels = labels, colors = plt.cm.Accent.colors, autopct='%.1f%%', startangle = 90)
            # plt.legend(loc='lower left', fontsize=10)#, bbox_to_anchor=(0.5, -0.1))
            plt.title('Actors\nBy Update Status', color = 'b', fontsize = 12, fontweight='bold')
            
            # Plotting actors by input attributes
            plot_count = 1
            for key, value in output_dict.items():
                plt.subplot(1, nb_of_plots, plot_count + 1)
                labels = list(value.keys())
                values = list(value.values())
                plt.pie(x = values, labels = labels, colors = plt.cm.Accent.colors, autopct='%.1f%%', startangle = 90)
                # plt.legend(loc='lower left', fontsize=10)#, bbox_to_anchor=(0.5, -0.1))
                plt.title(f'Actors\nPercentage By {key}', color = 'b', fontsize = 12, fontweight='bold')
                plot_count += 1
                
            plt.tight_layout()
            image_name = 'z5335667_q6.jpg'
            plt.savefig(image_name)
            
            return send_file(image_name, mimetype='image/jpg')