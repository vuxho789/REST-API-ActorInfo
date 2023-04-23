# -*- coding: utf-8 -*-
# Python 3.8.5
"""

This program was written by Vu Ho
Created on Mon 21 March 2022

"""
import sqlite3


# ----- DATABASE SET UP ------
# Establish database connection
def connect_db():
    connection = sqlite3.connect('z5335667.db')
    return connection


# Create tables in the database
def create_tables():
    conn = connect_db()
    
    actors_table = '''CREATE TABLE IF NOT EXISTS Actors (
            id              INTEGER PRIMARY KEY,
            name            TEXT NOT NULL,
            tvmazeId        INTEGER,
            country         TEXT,
            birthday        TEXT,
            deathday        TEXT,
            gender          TEXT,
            lastUpdate      TEXT
            );'''
    
    shows_table = '''CREATE TABLE IF NOT EXISTS ActorInShows (
            actor_id        INTEGER,
            showName        TEXT NOT NULL,
            PRIMARY KEY (actor_id, showName),
            FOREIGN KEY (actor_id) REFERENCES Actors(id)
                ON DELETE CASCADE
                ON UPDATE NO ACTION
            );'''
    
    try:
        conn.execute(actors_table)
        conn.execute(shows_table)
        conn.commit()
        print('Tables are created successfully')
        
    except sqlite3.Error as err:
        print(f'Creating table error: {err}')
        
    finally:
        conn.close()
