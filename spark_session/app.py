from flask import Blueprint, render_template
from engine import Engine

import json

from flask import Flask, request
main = Blueprint('app', __name__)

@main.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@main.route("/topN_movies/", methods=["POST"])
def get_topN_movie():
    userId = int(request.form['userId'])
    count = int(request.form['count'])
    similar_user = engine.recommend_topN_movie_for_user(userId, count)
    print(similar_user)
    return similar_user
    #return json.dumps(similar_user)

def create_app(spark, dataset_path, tmdb_key):
    global engine 
    engine = Engine(spark, dataset_path, tmdb_key)
    engine.load_rating()
    engine.train()
    app = Flask(__name__)
    app.register_blueprint(main)
    return app 
