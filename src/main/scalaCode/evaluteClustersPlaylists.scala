package scalaCode

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.log4j.{Level, LogManager, Logger}
import breeze.linalg.{Vector, squaredDistance}
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import KMeansUtils.getSongsWithListsDF
import org.apache.spark.sql.functions.{lit, regexp_replace}

object evaluateClustersPlaylists {

  def main(args: Array[String]) {

    // set up spark environment
    val spark = SparkSession.builder()
      .appName("Spotify KMeans Parallel K")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    // set up logging
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Rename arguments to make them readable & cast expected types to avoid type errors in later use
    val inputSongs: String = "songs_lists_output"
    val inputClusters: String = "local-output-spark-K5000"


    // read in input
    val songs = getSongsWithListsDF(spark, inputSongs).select("title", "artist", "playlists")
    val clusters = spark.read.format("csv").option("header", "false").load(inputClusters)
      .toDF("cluster", "title", "artist")
      .withColumn("cluster", regexp_replace($"cluster", "\\(", ""))
      .withColumn("title", regexp_replace($"title", "\\[", ""))
      .withColumn("artist", regexp_replace($"artist", "\\]\\)", ""))

    println(songs.count())
    //val join = songs.join(clusters, (songs("artist") === clusters("artist")) && (songs("title") === clusters("title")))

    //songs.filter($"title"==="xxx").count()

  }
}