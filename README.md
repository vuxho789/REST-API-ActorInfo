# REST-API-ActorInfo
- This is a university project implementing a Flask-Restx data service that allows a client to read and store information about actors/actresses, and allows the consumers to access the data through a REST API.

- The project utilises an external API which which provides a detailed list of TV shows and people. The retrieved information is then stored locally in a SQLite database.

## API Endpoints
![Screenshot 2023-04-23 170208](https://user-images.githubusercontent.com/101780311/233825065-612d7af5-9452-4023-83c2-3824a8007803.png)

## How to run
The Flask app can be run locally by the following command.
```bash
$ python3 app.py
```
Once the app starts, the swagger documentation of the application can be accessed at: http://localhost:5000/api/v1 
