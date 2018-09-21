from elasticsearch import Elasticsearch
import json


class ES:

    def __init__(self):
        self.es = Elasticsearch()

    def create_index(self, index_name):
        with open("index/"+index_name+'.json') as json_data:
             index = json.load(json_data)
        self.es.indices.create(index=index_name, body=index)

