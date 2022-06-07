# Movie Recommendation System Based on Spark

## Description
This project uses the "collaborative filtering (CF)" method to make movie recommendations. 
It has two major components: the offline module and the online module.

The offline module makes recommendations based upon statistical analysis of movie rankings and the Latent Factor Model (LFM) built using user rating data.
I used Spark-SQL for the movie statistics, and the Alternate Least Squares (ALS) of Spark-ML for training the LFM.
The output of the LFM contains the feature matrix for all the movies. I computed the cosine-similarity for every pair of feature vectors in the feature matrix.

The online module makes recommendations as soon as the user provides the rating for a particular movie through interacting with the website (website source code not included here, similar to https://movielens.org/).
The recommendations are based upon the most recent ratings of the user and the cosine-similarities between the movies computed and stored in the offline module. 
This method is called "Item-CF". I used Spark-Streaming to implement this module.

To solve the "cold-start" problem, new users are asked to pick the movie genres they might be interested in during the registration process.
The cosine-similarities between movies can also be computed using the genres of the movies based on the Term-Frequency-Inverse-Document-Frequency (TF-IDF) method.
The "ContentRecommender" module can make recommendations based upon the genres picked by the user without using user rating data.

## Built-With
- Spark-Core
- Spark-SQL  
- Spark-MLLib
- Spark-Streaming
- MongoDB
- Redis
- ElasticSearch
- Kafka
