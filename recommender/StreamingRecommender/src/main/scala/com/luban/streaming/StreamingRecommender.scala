package com.luban.streaming

import com.mongodb.casbah.Imports.MongoClientURI
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.{log, log10} // redis output java collections, which do not support ops like "map", import implicit conversions here

case class MongoConfig(uri: String, db: String)

// 定义一个基准推荐对象
case class Recommendation(mid: Int, score: Double)

// 定义基于预测评分的用户推荐列表
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// 定义基于LFM电影特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object ConnHelper extends Serializable {
  lazy val jedis: Jedis = new Jedis("localhost")
  lazy val mongoClient: MongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}

object StreamingRecommender {
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  def getUserRecentRatings(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    jedis.lrange("uid:" + uid, 0, num - 1).map(item => { // redis stores items in list in reverse-time order, just like a stack, so the first num elements are the most recent num elements
      val attrs = item.split(":")
      (attrs(0).trim.toInt, attrs(1).trim.toDouble)
    }).toArray
  }

  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                     (implicit mongoConfig: MongoConfig): Array[Int] = {
    val allSimMovies = simMovies(mid).toArray

    val ratedMovies = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid))
      .toArray
      .map(item => item.get("mid").toString.toInt)

    allSimMovies.filter(x => !ratedMovies.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(num)
      .map(_._1)
  }

  def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double = {
    simMovies.get(mid1) match {
      case Some(x) => x.get(mid2) match {
        case Some(v) => v
        case None => 0.0
      }
      case None => 0.0
    }
  }

  def computeMovieScores(candidateMovies: Array[Int], userRecentRatings: Array[(Int, Double)],
                         simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] = {
    val scores = ArrayBuffer[(Int, Double)]()
    val increMap = mutable.HashMap[Int, Int]()
    val decreMap = mutable.HashMap[Int, Int]()
    for (candidateMovie <- candidateMovies; userRecentRating <- userRecentRatings) {
      val simScore = getMoviesSimScore(candidateMovie, userRecentRating._1, simMovies)
      if (simScore > 0.7) {
        scores.append((candidateMovie, simScore * userRecentRating._2))
        if (userRecentRating._2 > 3) {
          increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
        } else {
          decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
        }
      }
    }
    scores.groupBy(_._1).map {
      case (mid, scoreList) => (mid, scoreList.map(_._2).sum / scoreList.length + log10(increMap.getOrDefault(mid, 1)) - log10(decreMap.getOrDefault(mid, 1)))
    }.toArray
  }

  def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    streamRecsCollection.insert(MongoDBObject("uid" -> uid,
      "recs" -> streamRecs.map(x => MongoDBObject("mid" -> x._1, "score" -> x._2))))
  }

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    val conf : SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRec")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    import spark.implicits._

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val simMovieMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map(movieRecs => (movieRecs.mid, movieRecs.recs.map(x => (x.mid, x.score)).toMap)
      ).collectAsMap() // collected into nested map for fast query

    val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix) // broadcast to executors

    val kafkaParam = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam)
    )

    // incoming raw data: UID|MID|SCORE|TIMESTAMP
    val ratingStream = kafkaStream.map(item => {
      val attrs = item.value().split("\\|")
      (attrs(0).toInt, attrs(1).toInt, attrs(2).toDouble, attrs(3).toInt)
    })

    ratingStream.foreachRDD(
      rdds => {
        rdds.foreach {
          case (uid, mid, score, timestamp) => {
            // 1. retrieve from redis the latest k ratings of uid
            val userRecentRatings = getUserRecentRatings(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

            // 2. retrieve from simMoviesMatrix N movies most similar to mid
            val candidateMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

            // 3. compute score for the movies obtained in 2
            val streamRecs = computeMovieScores(candidateMovies, userRecentRatings, simMovieMatrixBroadCast.value)

            // 4. write output to mongodb
            saveDataToMongoDB(uid, streamRecs)
          }
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }
}
