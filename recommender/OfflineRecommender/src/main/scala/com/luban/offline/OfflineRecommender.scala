package com.luban.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

// LFM

case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

case class Recommendation(mid: Int, score: Double)

case class UserRecs(uid: Int, recs: Seq[Recommendation])

case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object OfflineRecommender {
  val MONGODB_RATING_COLLECTION = "Rating"
  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"
  val USER_MAX_RECOMMENDATION = 20

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRec")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score)) // convert to rdd and remove timestamp
      .cache()

    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = ratingRDD.map(_._2).distinct()

    // train the LFM
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    val (rank, iterations, lambda) = (50, 5, 0.01)
    val model = ALS.train(trainData, rank, iterations, lambda)

    // make predictions of ratings
    val predRatings = model.predict(userRDD.cartesian(movieRDD))
    val userRecs = predRatings
      .filter(_.rating > 0)
      .map(item => (item.user, (item.product, item.rating)))
      .groupByKey()
      .map {
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)).take(USER_MAX_RECOMMENDATION))
      }
      .toDF()

    storeDFInMongoDB(userRecs, USER_RECS)

    // calculate movie similarities
    val movieFeatures = model.productFeatures
      .map {
        case (mid, features) => (mid, new DoubleMatrix(features))
      }

    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter { case (a, b) => a._1 != b._1 } // remove self-recommendation
      .map { case (a, b) => (a._1, (b._1, cosineSim(a._2, b._2))) }
      .filter(_._2._2 > 0.6) // remove similarity smaller than 0.6
      .groupByKey()
      .map {
        case (mid, recs) => MovieRecs(mid, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    storeDFInMongoDB(movieRecs, MOVIE_RECS)

    spark.stop()
  }

  def cosineSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

}
