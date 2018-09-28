# Netflix-Clone-Movielense

This project is a sideProject to clone Netflix with MovieLense Data and TMDB API
* Top N unrated movie recommender for user

### Enviroment
* Elasticsearch 5.3.0  
* Spark-2.3.1-bin-hadoop2.7  
* ElasticSearch-hadoop-5.3.0  

### Execution
1. Install Spark, ElasticSearch and configure enviroment.
2. Download the data ml-latest from grouplens site
3. pip install -r requirements
4. cd backend
5. Configure server.py, start.sh
6. ./start.sh

### Execution with ElasticSearch
1. create index
2. save data to ElasticSearch
3. search data from ElasticSearch

### Acknowledgement
* [GroupLens, MovieLense](https://grouplens.org/datasets/movielens/)  
* [TMDB API](https://www.themoviedb.org/documentation/api?language=en-US)
