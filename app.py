from flask import Blueprint, render_template
from backend.engine import Engine

import json

from flask import Flask, request
#app = Flask(__name__, template_folder="frontend/templates")
main = Blueprint('app', __name__, template_folder="frontend/templates")

@main.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@main.route("/hello", methods=["GET"])
def hello():
    return render_template("hello.html")

@main.route("/create_index", methods=["GET"])
def create_index():
    engine.create_es_index()
    return "movielens index is created"

@main.route("/save_to_es", methods=["GET"])
def save_to_es():
    engine.save_to_es()
    return "Save"

@main.route("/save_to_es_movies", methods=["GET"])
def save_to_es_movies():
    engine.save_to_es_movies()
    return "Save Movie"

@main.route("/search/<movieName>", methods=["GET"])
def search_movie(movieName):
    result = engine.search_movie_tmdb(movieName)
    return json.dumps(result)

@main.route("/predict/<int:userId>/<int:movieId>", methods=["GET"])
def get_predict_rating(userId, movieId):
    ratings = engine.get_predicted_rating(userId, movieId)
    print(ratings)
    return ratings

@main.route("/predict_file/<file_name>", methods=["GET"])
def get_predict_ratings(file_name):
    ratings = engine.get_predicted_rating_from_file(file_name)
    return json.dumps(ratings)

#TODO Improve topN
@main.route("/topN/<int:userId>/<int:count>", methods=["GET"])
def get_topN(userId,count):
    topN_movies_ratings = engine.top_ratings(userId, count)
    print(topN_movies_ratings)
    movie_list = json.dumps(topN_movies_ratings)
    return render_template('movies.html', movies=movie_list)
    #return json.dumps(topN_movies_ratings)

#TODO start

@main.route("/avg/<int:movieId>", methods=["GET"])
def get_average_rating(movieId):
    rating = engine.get_average_rating(movieId)
    return json.dumps(rating)

@main.route("/similar/<int:movieId>/<int:count>", methods=["GET"])
def get_similar_movie(movieId, count):
    similar_movies = engine.similar_movie(movieId, coount)
    return json.dumps(similar_movies)

@main.route("/similar/<int:userId>/<int:count>", methods=["GET"])
def get_similar_user(userId, count):
    similar_user = engine.similar_user(userId, coount)
    return json.dumps(similar_user)

#TODO end

def create_app(sc, dataset_path, tmdb_key):
    global engine 
    engine = Engine(sc, dataset_path, tmdb_key)
    engine.load_data_from_file()
    app = Flask(__name__)
    app.register_blueprint(main)
    return app 
