# -*- coding: utf-8 -*-
# Python 3.8.5
"""

This program was written by Vu Ho
Created on Mon 21 March 2022

"""
from flask import Flask
from actors_api import api_bp
from actors_db import create_tables


app = Flask(__name__)
app.register_blueprint(api_bp)

if __name__ == '__main__':
    create_tables()
    app.run(debug = True)
