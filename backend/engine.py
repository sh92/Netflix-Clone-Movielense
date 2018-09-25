import os
import sys
import numpy as np
from pyspark.mllib.recommendation import ALS, Rating
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql import SQLContext

import tmdbsimple as tmdb
from es import ES
import logging
import re
import json
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Engine:

   def __init__(self, sc, data_path, tmdb_key):
      self.sc =  sc
      self.data_path = data_path
      self.es = ES()
      self.sqlContext = SQLContext(self.sc)
      tmdb.API_KEY = tmdb_key
      self.tmdb_key = tmdb_key

   def load_data_from_file(self):
      ratings_file_path = os.path.join(self.data_path, 'ratings.csv')
      ratings_raw_RDD = self.sc.textFile(ratings_file_path)
      ratings_header = ratings_raw_RDD.take(1)[0]
      #ratings_header_list = ratings_header.split(",")
      rating_schema = StructType(\
      [StructField('userId', IntegerType(), True),\
      StructField('movieId', IntegerType(), True),\
      StructField('rating', FloatType(), True)]\
      )
      ratings_RDD = ratings_raw_RDD.filter(lambda line: line!=ratings_header).map(lambda line: line.split(",")).map(lambda x: (int(x[0]), int(x[1]), float(x[2]) )).cache()
      self.ratings_RDD = ratings_RDD
      print(ratings_RDD.take(10))
      rating_df = self.sqlContext.createDataFrame(ratings_RDD, rating_schema)
      rating_df.show(10)
      self.rating_df = rating_df

      ratings_dict_RDD = rating_df.rdd.map(lambda item : (
          item['movieId'], {
             'userId': item['userId'],
             'movieId': item['movieId'],
             'rating': item['rating']
          }))
      print(ratings_dict_RDD.take(10))
      self.ratings_dict_RDD = ratings_dict_RDD

      movie_schema = StructType(\
      [StructField('movieId', IntegerType(), True),\
      StructField('title', StringType(), True),\
      StructField('genres', StringType(), True)]\
      )

      movies_file_path = os.path.join(self.data_path, 'movies.csv')
      movies_raw_RDD = self.sc.textFile(movies_file_path)
      movies_header = movies_raw_RDD.take(1)[0]
      movies_header_list = movies_header.split(",")

      self.movies_RDD = movies_raw_RDD.filter(lambda line: line!=movies_header).map(lambda line: line.split(",")).map(lambda x: (int(x[0]),x[1],x[2])).cache()


      movies_df = self.sqlContext.createDataFrame(self.movies_RDD, movie_schema)
      movies_df.show(10)
      self.movies = movies_df

      movies_dict_RDD = rating_df.rdd.map(lambda item : (
          item['movieId'], {
             'movieId': item['movieId'],
             'title': item['title'],
             'genres': item['genres']
          }))
      self.movies_dict_RDD = movies_dict_RDD
            
      self.rank = 3
      self.iterations = 10
      self.train()

   def get_predicted_rating(self, userId, movieId):
      predicted_rating_RDD = self.model.predict(userId,movieId)
      print(predicted_rating_RDD)
      return redicted_rating_RDD

   def get_predicted_rating_from_file(self, file_name):
      data = self.sc.textFile(file_name)
      ratings = data.map(lambda l: l.split(',')).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

      testdata = ratings.map(lambda p: (p[0], p[1]))
      predictions = self.model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
      ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
      RMSE = np.sqrt(ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
      print("RMSE = " + str(RMSE))
      print(predictions.collect())
      return predictions.collect()

   def get_rdd_from_es(self, userId, movieId):
      q ={
          "query": {
                "bool": {
                   "must" : {
                       "match_all": {}
                   },

                   "filter": {
                      "term": {
                         "userId": userId,
                         "movieId": movieId
                      }
                    }
                }
          }
      }
      es_read_conf = {
        "es.nodes" : "localhost",
        "es.port" : "9200",
        "es.resource" : "movielens/ratings",
        "es.query" : q
      }
      es_read_rdd = self.sc.newAPIHadoopRDD(
      inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
      keyClass="org.apache.hadoop.io.NullWritable", 
      valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
      conf=es_read_conf)
      es_df = self.sqlContext.createDataFrame(es_read_rdd)
      es_df.show(10)
      es_ratings_RDD = self.transform(es_read_rdd)
      return es_ratings_RDD.collect()

   def search_userRatings_from_es(self, userId):
      q ={
          "query": {
                "bool": {
                   "must" : {
                       "match_all": {}
                   },

                   "filter": {
                      "term": {
                         "userId": userId
                      }
                    }
                }
          }
      }

      es_read_conf = {
        "es.nodes" : "localhost",
        "es.port" : "9200",
        "es.resource" : "movielens/ratings",
        "es.query" : q
      }
      es_read_rdd = self.sc.newAPIHadoopRDD(
      inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
      keyClass="org.apache.hadoop.io.NullWritable",
      valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
      conf=es_read_conf)
      es_df = self.sqlContext.createDataFrame(es_read_rdd)
      es_df.show(10)
      result = self.transform(es_read_rdd)
      print(result.collect())
      return result.collect()
      

   def load_data_from_es(self):
      es_read_conf = {
        "es.nodes" : "localhost",
        "es.port" : "9200",
        "es.resource" : "movielens/ratings"
      }
      es_read_rdd = self.sc.newAPIHadoopRDD(
      inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
      keyClass="org.apache.hadoop.io.NullWritable", 
      valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
      conf=es_read_conf)
      self.es_df = self.sqlContext.createDataFrame(es_read_rdd)
      self.es_df.show(10)
      result = self.transform_from_es(es_read_rdd)
      self.es_ratings_RDD = result
      return result.take(10)

   def transform_from_es(self, es_read_rdd):
      return es_read_rdd.map(lambda x: (x[1]['userId'],x[1]['movieId'],x[1][u'rating']))
      
   def search_movie(self, movie_name):
      search = tmdb.Search()
      response = search.movie(query=movie_name)
      print(response)
      data_list = []
      for s in search.results:
         data = { 'title': s['title'], 'date':s['date'], 'popularity':s['popularity'], 'id': s['id']}
         print(data)
         data_list.append(data)
      result = {'response': response, 'data':data_list}
      return result

   def create_es_index(self):
      self.es.create_index("movielens")

   def save_to_es(self):
      self.rating_df.write.format("es").save("movielens/ratings")
      self.movies_df.write.format("es").save("movielens/movies")

   def save_to_es_hadoop(self):
      es_write_conf = {
      "es.nodes" : 'localhost',
      "es.port" : '9200',
      "es.resource" : 'movielens/ratings',
      #"es.input.json" : "yes"
      "es.mapping.id": "movieId"
      }
      self.ratings_dict_RDD.saveAsNewAPIHadoopFile(
      path='-',
      outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
      keyClass="org.apache.hadoop.io.NullWritable",
      valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
      conf=es_write_conf)
      return true

   def train(self):
      self.model = ALS.train(self.ratings_RDD, self.rank, self.iterations, 0.01)
      logger.info("ALS model")
    
   def top_ratings(self, user_id, count):
      unrated = self.ratings_RDD.filter(lambda rating: not rating[0] == user_id).map(lambda x: (user_id, x[1])).distinct()
      predicted_RDD = self.model.predictAll(unrated)
      predicted_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
      ratings = predicted_RDD.takeOrdered(count, key=lambda x: -x[1])
      return ratings
