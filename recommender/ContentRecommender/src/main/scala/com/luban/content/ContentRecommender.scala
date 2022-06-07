package com.luban.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

// 需要的数据源是电影内容信息
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class MongoConfig(uri: String, db: String)

// 定义一个基准推荐对象
case class Recommendation(mid: Int, score: Double)

// 定义电影内容信息提取出的特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object ContentRecommender {
  // 定义表名和常量
  val MONGODB_MOVIE_COLLECTION = "Movie"

  val CONTENT_MOVIE_RECS = "ContentMovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val conf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRec")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val movieTagsDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .map(movie => (movie.mid, movie.name, movie.genres.map(c => if (c == '|') ' ' else c))) // replace '|' with space
      .toDF("mid", "name", "genres")
      .cache()

    // use TF-IDF model to get movie features
    val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words") // default using space as separator
    val wordsData = tokenizer.transform(movieTagsDF)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
    val featurizedData = hashingTF.transform(wordsData)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    val movieFeatures = rescaledData.map(
      row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
    )
      .rdd
      .map(
        x => (x._1, new DoubleMatrix(x._2))
      )
    //movieFeatures.collect().foreach(println)

    // from movie features compute movie similarities (same as in OfflineRecommender)
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter { case (a, b) => a._1 != b._1 } // remove self-recommendation
      .map { case (a, b) => (a._1, (b._1, cosineSim(a._2, b._2))) }
      .filter(_._2._2 > 0.6) // remove similarity smaller than 0.6
      .groupByKey()
      .map {
        case (mid, recs) => MovieRecs(mid, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    storeDFInMongoDB(movieRecs, CONTENT_MOVIE_RECS)

    spark.stop()
  }

  def cosineSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
