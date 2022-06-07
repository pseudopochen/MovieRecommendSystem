package com.luban.offline

import com.luban.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.math.sqrt

object ALSTrainer {
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
      .map(rating => Rating(rating.uid, rating.mid, rating.score)) // convert to rdd and remove timestamp
      .cache()

    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testRDD = splits(1)

    adjustALSParam(trainingRDD, testRDD)

    spark.close()
  }

  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {

    val res = for (rank <- Array(20, 50, 100, 200, 300); lambda <- Array(0.01, 0.1, 1))
      yield {
        val model = ALS.train(trainData, rank, 5, lambda)
        val rmse : Double = getRMSE(model, testData)
        (rank, lambda, rmse)
      }
    println(res.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]) : Double = {
    val userProduct = data.map(r => (r.user, r.product))
    val predRatings = model.predict(userProduct)
    val pred = predRatings.map(item => ((item.user, item.product), item.rating))
    val obs = data.map(item => ((item.user, item.product), item.rating))

    sqrt(obs.join(pred).map {
      case (((uid, mid), (r_obs, r_pred))) => {
        val err = r_obs - r_pred
        err * err
      }
    }.mean())
  }


}
