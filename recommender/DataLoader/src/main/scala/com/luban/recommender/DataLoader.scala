package com.luban.recommender

import com.mongodb.casbah.Imports.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, sql}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import java.net.InetAddress

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

case class MongoConfig(uri: String, db: String)

case class EsConfig(httpHosts: String, transportHosts: String, index: String, clusterName: String)

object DataLoader {

  val MOVIE_DATA_PATH = "/mnt/gv0/brick/MovieRecommendSystem/recommender/DataLoader/src/main/resources/movies.csv"
  val RATING_DATA_PATH = "/mnt/gv0/brick/MovieRecommendSystem/recommender/DataLoader/src/main/resources/ratings.csv"
  val TAG_DATA_PATH = "/mnt/gv0/brick/MovieRecommendSystem/recommender/DataLoader/src/main/resources/tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"


  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "localhost:9200",
      "es.transportHosts" -> "localhost:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"

    )
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF = movieRDD.map(item => {
      val attr = item.split("\\^")
      Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    import org.apache.spark.sql.functions._
    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid"), "left")

    implicit val esConfig : EsConfig = EsConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))
    storeDataInES(movieWithTagsDF)

    spark.stop()
  }

  def storeDataInMongoDB(movieDF: sql.DataFrame, ratingDF: sql.DataFrame, tagDF: sql.DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // create index for collections
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient.close()
  }

  //  def storeDataInES(movieDF: DataFrame)(implicit esConfig: EsConfig): Unit = {
  //    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clusterName).build()
  //    val esClient = new PreBuiltTransportClient(settings)
  //    val REGEX_HOST_PORT = "(.+):(\\d+)".r
  //    esConfig.transportHosts.split(",").foreach {
  //      case REGEX_HOST_PORT(host: String, port: String) => {
  //        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
  //      }
  //    }
  //
  //    if (esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists) {
  //      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
  //    }
  //    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))
  //    movieDF.write
  //      .option("es.nodes", esConfig.httpHosts)
  //      .option("es.http.timeout", "100m")
  //      .option("es.mapping.id", "mid")
  //      .mode("overwrite")
  //      .format("org.elasticsearch.spark.sql")
  //      .save(esConfig.index + "/" + ES_MOVIE_INDEX)
  //  }

  def storeDataInES(movieDF: DataFrame)(implicit eSConfig: EsConfig): Unit = {
    // 新建es配置
    val settings: Settings = Settings.builder().put("cluster.name", eSConfig.clusterName).build()

    // 新建一个es客户端
    val esClient = new PreBuiltTransportClient(settings)

    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) => {
        println("DEBUG-------host: " + host + ", port: " + port)
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }

    // 先清理遗留的数据
    if (esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index))
      .actionGet()
      .isExists
    ) {
      esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }

    esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

    movieDF.write
      .option("es.nodes", eSConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index + "/" + ES_MOVIE_INDEX)
  }


}
