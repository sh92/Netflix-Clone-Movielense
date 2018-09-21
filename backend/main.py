from pyspark import SparkConf, SparkContext
import os
from app import create_app

def run_server(app):
    app.run(host='0.0.0.0', port=8999)

if __name__ == "__main__":
    conf = SparkConf().setAppName("MovieLense")
    sc = SparkContext(conf=conf)
    tmdb_key = "YOUR_KEY"

    dataset_path = os.path.join('../../../data', 'ml-latest-small')
    app = create_app(sc, dataset_path, tmdb_key)
    run_server(app)
