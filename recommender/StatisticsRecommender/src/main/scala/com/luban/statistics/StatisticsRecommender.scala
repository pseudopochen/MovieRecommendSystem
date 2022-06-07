package com.luban.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

case class Recommendation(mid: Int, score: Double)

case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

object StatisticsRecommender {

  val MOVIE_DATA_PATH = "/mnt/gv0/brick/MovieRecommendSystem/recommender/DataLoader/src/main/resources/movies.csv"
  val RATING_DATA_PATH = "/mnt/gv0/brick/MovieRecommendSystem/recommender/DataLoader/src/main/resources/ratings.csv"
  // Historic most popular
  val RATE_MORE_MOVIES = "RateMoreMovies"
  // Recent most popular
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  // Average ratings
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

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
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("Statistics")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    ratingDF.createOrReplaceTempView("ratings")

    // 1. Historic most popular
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid")
    storeDFInMongoDB(rateMoreMoviesDF, RATE_MORE_MOVIES)

    // 2. Recent most popular
    val sdf = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate", (x: Int) => sdf.format(new Date(x * 1000L)).toInt)
    val ratingofYearMonthDF = spark.sql("select mid, changeDate(timestamp) as yearmonth from ratings")
    ratingofYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyMoviesDF = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth, mid order by yearmonth desc, count desc")
    storeDFInMongoDB(rateMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES)

    // 3. Average scores
    val averageMovieDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")
    storeDFInMongoDB(averageMovieDF, AVERAGE_MOVIES)

    // 4. Most popular for each genre
    val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery", "Romance", "Science", "Tv", "Thriller", "War", "Western")
    val movieWithScoreDF = movieDF.join(averageMovieDF, "mid")
    val genresRDD = spark.sparkContext.makeRDD(genres)

    val genresTopMoviesDF = genresRDD.cartesian(movieWithScoreDF.rdd)
      .filter {
        case (genre, row) => row.getAs[String]("genres").toLowerCase().contains(genre.toLowerCase())
      }
      .map {
        case (genre, row) => (genre, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
      }
      .groupByKey()
      .map {
        case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
      }
      .toDF()
    storeDFInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)

    spark.stop()
  }

}
