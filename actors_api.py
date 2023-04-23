# -*- coding: utf-8 -*-
# Python 3.8.5
"""

This program was written by Vu Ho
Created on Mon 21 March 2022

"""
from flask import Blueprint
from flask_restx import Resource, Api, fields, reqparse
from helpers import add_new_actor, get_all_actors_paginated, get_actor, delete_actor, update_actor, get_stat_summary


# ----- API IMPLEMENTATION ------
api_bp = Blueprint('api', __name__, url_prefix='/api/v1')

api = Api(api_bp,
          title = 'API for Actors',
          version='1.0',
          description = 'Simple API to provide data service regarding Actors information fetched from TV Maze website')

# Schema for Actors payload
actor_model = api.model('Actor', { \
                        'name': fields.String(example = 'New name'),
                        'country': fields.String(example = 'New country'),
                        'birthday': fields.String(description = 'Date format: YYYY-MM-DD or NULL if unknown', example = '1999-05-20'),
                        'deathday': fields.String(description = 'Date format: YYYY-MM-DD or NULL if unknown', example = 'NULL'),
                        'gender': fields.String(example = 'Male'),
                        'shows': fields.List(fields.String, example = ['New show 1', 'New show 2'])}, strict = True)

# Define parameteres can be obtained from the API queries
parser = reqparse.RequestParser()
parser.add_argument('name')

list_parser = reqparse.RequestParser()
list_parser.add_argument('order', action = 'split', default = ['+id'])
list_parser.add_argument('page', type = int, default = 1)
list_parser.add_argument('size', type = int, default = 10)
list_parser.add_argument('filter', action = 'split',  default = ['id','name'])

stat_parser = reqparse.RequestParser()
stat_parser.add_argument('format', choices = ['json', 'image'], required = True)
stat_parser.add_argument('by', action='split', required = True)


@api.route('/actors')
class ActorsList(Resource):
    # Q1 - Add a new Actor
    @api.response(201, 'Actor Added Successfully')
    @api.response(404, 'Actor Not Found')
    @api.response(400, 'Actor Already Exists')
    @api.doc(description = 'Add a New Actor', params = {'name': 'Name of an Actor'})
    def post(self):
        # Retrieving the query parameters
        args = parser.parse_args()
        input_name = args.get('name')
        return add_new_actor(input_name)


    # Q5 - Retrieve the List of Available Actors
    @api.response(200, 'Successful')
    @api.response(404, 'No Actor Available')
    @api.response(400, 'Parameter Validation Error')
    @api.doc(description = 'Retrieve All Available Actors',\
             params = {'order': 'Criteria to sort the list of actors\n(Criteria: id, name, country, birthday, deathday, last-update)\n(Prefix: + for ascending order, - for descending order)',\
                       'page': 'Page number to display',\
                       'size': 'Number of actors on a page',\
                       'filter': 'Attributes to display for each actor\n(Attributes: id, name, country, birthday, deathday, last-update, shows)'})
    @api.expect(list_parser, validate = True)    
    def get(self):
        # Retrieving the query parameters
        args = list_parser.parse_args()
        input_order = args.get('order')
        input_page = args.get('page')
        input_size = args.get('size')
        input_filter = args.get('filter')
        return get_all_actors_paginated(input_order, input_page, input_size, input_filter)
        
        
@api.route('/actors/<int:id>')
@api.param('id', 'Actor ID')    
class ActorsInfo(Resource):
    # Q2 - Retrieve an Actor
    @api.response(200, 'Successful')
    @api.response(404, 'Actor Not Found')
    @api.response(400, 'Invalid ID')
    @api.doc(description = 'Retrieve an Actor by ID')
    def get(self, id):
        if (id < 1):
            return {'message': f'Actor id {id} is invalid'}, 400
        else:
            return get_actor(id)
        

    # Q3 - Delete an Actor
    @api.response(200, 'Successful')
    @api.response(404, 'Actor Not Found')
    @api.response(400, 'Invalid ID')
    @api.doc(description = 'Delete an Actor by ID')
    def delete(self, id):
        if (id < 1):
            return {'message': f'Actor id {id} is invalid'}, 400
        else:
            return delete_actor(id)
        
        
    # Q4 - Update an Actor
    @api.response(200, 'Successful')
    @api.response(404, 'Actor Not Found')
    @api.response(400, 'Invalid ID')
    @api.doc(description = 'Update an Actor by ID')
    @api.expect(actor_model, validate = True)
    def patch(self, id):
        if (id < 1):
            return {'message': f'Actor id {id} is invalid'}, 400
        else:
            return update_actor(id, actor_model)
        
        
# Q6 - Get the Statistics of the Existing Actors
@api.route('/actors/statistics')
@api.param('format', 'Format (image/json) to display the statistics')
@api.param('by', 'Attributes (country/birthday/gender/life_status) used in the statistics')
class ActorsStat(Resource):
    
    @api.response(200, 'Successful')
    @api.response(404, 'No Actor Available')
    @api.response(400, 'Parameter Validation Error')
    @api.doc(description="Get Statistics of Existing Actors")
    @api.expect(stat_parser, validate = True)
    def get(self):
        # Retrieving the query parameters
        args = stat_parser.parse_args()
        input_format = args.get('format')
        input_attributes = args.get('by')
        return get_stat_summary(input_format, input_attributes)
        
        