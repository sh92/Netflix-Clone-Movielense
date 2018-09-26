from elasticsearch import Elasticsearch
import json


class ES:

    def __init__(self, sc, sqlContext):
        self.es = Elasticsearch()
        self.sc = sc
        self.sqlContext = sqlContext

    def create_index(self, index_name):
        with open("index/"+index_name+'.json') as json_data:
             index = json.load(json_data)
        self.es.indices.create(index=index_name, body=index)

    def transform_from_es(self, es_read_rdd):
        return es_read_rdd.map(lambda x: (x[1]['userId'],x[1]['movieId'],x[1][u'rating']))

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
        return self.get_ratingRDD_by_query(q)

    def get_ratingRDD_by_query(self,q):
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
        #self.es_df = self.sqlContext.createDataFrame(es_read_rdd)
        #self.es_df.show(10)
        result = self.transform_from_es(es_read_rdd)
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
        return self.get_ratingRDD_by_query(q)

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
        return self.get_ratingRDD_by_query(q)

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
        return self.get_ratingRDD_by_query(q)
