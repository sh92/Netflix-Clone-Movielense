from elasticsearch import Elasticsearch
import json
import sys

class ES:

    def __init__(self, sc, sqlContext):
        self.es = Elasticsearch()
        self.sc = sc
        self.sqlContext = sqlContext

    def create_index(self, index_name):
        with open("index/"+index_name+'.json') as json_data:
             index = json.load(json_data)
        self.es.indices.create(index=index_name, body=index)

    def transform_movie_type(self, es_read_rdd):
        return es_read_rdd.map(lambda x: (x[1][u'movieId'],x[1][u'title'],x[1][u'genres']))

    def transform_rating_type(self, es_read_rdd):
        return es_read_rdd.map(lambda x: (x[1][u'userId'],x[1][u'movieId'],x[1][u'rating']))

    def get_movieTitleByMovieId(self, movieId):
        q = {
           "query": {
                "bool": {
                   "must" : {
                       "match_all": {}
                   },
                   "filter": {
                      "terms": {
                         "movieId": movieId
                      }
                    }
                }
           }
        }
        res = self.es.search(index="movielens", doc_type="movies",  body=q)
        movie_id_title_dict = {}
        for x in res[u'hits'][u'hits']:
           y = x[u'_source']
           movie_id_title_dict[int(y[u'movieId'])] = str(y[u'title']).replace("\"","")
        return movie_id_title_dict

    def get_ratingRDD(self):
        q = {
           "query": {
               "bool": {
                  "must" : {
                      "match_all": {}
                   }
               }
           }
        }
        return self.get_RDD_by_query(q, "ratings")

    def get_RDD_by_query(self, q, index_name):
        es_read_conf = {
           "es.nodes" : "localhost",
           "es.port" : "9200",
           "es.resource" : "movielens/"+index_name,
           "es.query" : q
        }
        es_read_rdd = self.sc.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_read_conf)
        #self.es_df = self.sqlContext.createDataFrame(es_read_rdd)
        #self.es_df.show(10)
        if index_name == "ratings":
            result = self.transform_rating_type(es_read_rdd)
        elif index_name == "movies":
            result = self.transform_movie_type(es_read_rdd)
        return result

    def get_ratingRDD_by_user_movie(self, userId, movieId):
        q = {
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
        return self.get_RDD_by_query(q, "ratings")

    def get_ratingRDD_by_movieId(self, movieId):
        q = {
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
        return self.get_RDD_by_query(q, "ratings")

    def get_ratingRDD_by_userId(self, userId):
        q = {
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
        return self.get_RDD_by_query(q, "ratings")
