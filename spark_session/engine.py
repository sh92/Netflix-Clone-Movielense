import os
import sys
import numpy as np
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.sql.types import *

import tmdbsimple as tmdb
import logging
import re
import json
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Engine:

   def __init__(self, spark, data_path, tmdb_key):
      self.spark = spark
      self.data_path = data_path
      tmdb.API_KEY = tmdb_key
      self.tmdb_key = tmdb_key

   def load_rating(self):
      ratings_file_path = os.path.join(self.data_path, 'ratings.csv')
      lines = self.spark.read.text(ratings_file_path).rdd
      rating_header = lines.take(1)[0]
      lines = lines.filter(lambda line: line!=rating_header)
      parts = lines.map(lambda row: row.value.split(","))
      ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]), rating=float(p[2]), timestamp=long(p[3])))
      ratings = self.spark.createDataFrame(ratingsRDD)
      (self.training, self.test) = ratings.randomSplit([0.8, 0.2])

   def train(self):
      als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",coldStartStrategy="drop")
      self.model= als.fit(self.training)

   def rmse(self):
      predictions = self.model.transform(test)
      evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                      predictionCol="prediction")
      rmse = evaluator.evaluate(predictions)
      print("Root-mean-square error = " + str(rmse))

   def recommend_topN_movie_for_user(self, userId, count):
      userRecs = self.model.recommendForAllUsers(count)
      result = userRecs[userRecs.userId == userId]
      x = result.toJSON()
      return x.collect()[0]

   def recommend_topN_user_for_for_movie(self, movieId, count):
      movieRecs = self.model.recommendForAllItems(count)
      return movieRecs
