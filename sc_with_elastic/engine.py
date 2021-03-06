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


def get_product_avgRating_count(groupBy_product_rating_RDD):
   product_id = groupBy_product_rating_RDD[0]
   count = len(groupBy_product_rating_RDD[1])
   rating_sum = 0.000
   for x in groupBy_product_rating_RDD[1]:
      rating_sum += x
   return product_id, (float(rating_sum/count), count)

class Engine:

   def __init__(self, sc, data_path, tmdb_key):
      self.sc =  sc
      self.sqlContext = SQLContext(self.sc)
      self.data_path = data_path
      self.es = ES(self.sc, self.sqlContext)
      tmdb.API_KEY = tmdb_key
      self.tmdb_key = tmdb_key

   def load_data_from_file(self):
      ratings_file_path = os.path.join(self.data_path, 'ratings.csv')
      ratings_raw_RDD = self.sc.textFile(ratings_file_path)
      ratings_header = ratings_raw_RDD.take(1)[0]
      rating_schema = StructType(\
      [StructField('userId', IntegerType(), True),\
      StructField('movieId', IntegerType(), True),\
      StructField('rating', FloatType(), True)]\
      )
      ratings_RDD = ratings_raw_RDD.filter(lambda line: line!=ratings_header).map(lambda line: line.split(",")).map(lambda x: (int(x[0]), int(x[1]), float(x[2]) )).cache()
      self.ratings_RDD = ratings_RDD
      rating_df = self.sqlContext.createDataFrame(ratings_RDD, rating_schema)
      self.rating_df = rating_df

      ratings_dict_RDD = rating_df.rdd.map(lambda item : (
          item['movieId'], {
             'userId': item['userId'],
             'movieId': item['movieId'],
             'rating': item['rating']
          }))
      #logger.info(ratings_dict_RDD.take(10))
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
      self.movies_df = movies_df

      movies_dict_RDD = rating_df.rdd.map(lambda item : (
          item['movieId'], {
             'movieId': item['movieId'],
             'title': item['title'],
             'genres': item['genres']
          }))
      self.movies_dict_RDD = movies_dict_RDD
            
      self.rank = 10
      self.iterations = 10
      self.train()

      #TODO Get the image by using tmdb API and links.csv
      links_schema = StructType(\
      [StructField('movieId', IntegerType(), True),\
      StructField('imdbId', IntegerType(), True),\
      StructField('tmdbId', IntegerType(), True)]\
      )

      links_file_path = os.path.join(self.data_path, 'links.csv')
      links_raw_RDD = self.sc.textFile(links_file_path)
      links_header = links_raw_RDD.take(1)[0]
      links_header_list = links_header.split(",")

      self.links_RDD = links_raw_RDD.filter(lambda line: line!=links_header).map(lambda line: line.split(",")).map(lambda x: (int(x[0]),x[1],x[2])).cache()

      links_df = self.sqlContext.createDataFrame(self.links_RDD, links_schema)
      self.links_df = links_df

      links_dict_RDD = links_df.rdd.map(lambda item : (
          item['movieId'], {
             'movieId': item['movieId'],
             'imdbId': item['imdbId'],
             'tmdbId': item['tmdbId']
          }))

   def get_predicted_rating(self, userId, movieId):
      predicted_rating_RDD = self.model.predict(userId,movieId)
      logger.info(predicted_rating_RDD)
      return redicted_rating_RDD

   def get_predicted_rating_from_file(self, file_name):
      data = self.sc.textFile(file_name)
      ratings = data.map(lambda l: l.split(',')).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

      testdata = ratings.map(lambda p: (p[0], p[1]))
      predictions = self.model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
      ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
      RMSE = math.sqrt(ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
      logger.info("RMSE = " + str(RMSE))
      #logger.info(predictions.collect())
      return predictions.collect()

   def get_es_ratingRDD(self):
       ratings = self.es.get_ratingRDD()
       return ratings

   def get_es_ratingRDD_by_userId(self, userId):
       ratings = self.es.get_ratingRDD_by_userId(userId)
       return ratings

   def get_es_ratingRDD_by_movieId(self, userId):
       ratings = self.es.get_ratingRDD_by_movieId(movieId)
       return ratings

   def get_es_ratingRDD_by_user_movie(self, userId, movieId):
       ratings = self.es.get_ratingRDD_by_user_movie(userId, movieId)
       return ratings

   def search_movie_tmdb(self, movie_name):
      search = tmdb.Search()
      response = search.movie(query=movie_name)
      #logger.info(response)
      data_list = []
      for s in search.results:
         data = { 'title': s['title'], 'date':s['date'], 'popularity':s['popularity'], 'id': s['id']}
         #logger.info(data)
         data_list.append(data)
      result = {'response': response, 'data':data_list}
      return result

   def create_es_index(self):
      self.es.create_index("movielens")

   def save_to_es(self):
      self.rating_df.write.format("es").save("movielens/ratings")
      self.movies_df.write.format("es").save("movielens/movies")
      self.links_df.write.format("es").save("movielens/links")

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

   def topN_ratings_unrated_movies(self, user_id, count):
      unrated = self.ratings_RDD.filter(lambda x: not x[0] == user_id).map(lambda x: (user_id, x[1])).distinct()
      predicted_RDD =  self.model.predictAll(unrated)
      total_RDD = self.ratings_RDD.union(predicted_RDD)
      list_predict_movie = predicted_RDD.map(lambda x : x.product).distinct().collect()
      predicted_movie_RDD = total_RDD.filter(lambda x: x[1] in list_predict_movie)

      predicted_groupby_product_rating_RDD = predicted_movie_RDD.map(lambda x: (x[1], x[2])).groupByKey()
      product_avgRating_count_RDD = predicted_groupby_product_rating_RDD.map(get_product_avgRating_count)

      filtered_RDD = product_avgRating_count_RDD.filter(lambda x: x[1][0] > 3 and x[1][1] > 30)
      ratings_list = filtered_RDD.takeOrdered(count, key=lambda x: -x[1][0])
      ratings_movie_id = [x[0] for x in ratings_list]
      movie_title_dict = self.es.get_movieTitleByMovieId(ratings_movie_id)
      result_list = [ (x[0], movie_title_dict[x[0]], x[1][0], x[1][1]) for x in ratings_list]
      result = []
      for x in result_list:
         y = {
               "movieId": x[0],
               "title": x[1],
               "rating": x[2],
               "count": x[3]
            }
         result.append(y)
      return result
