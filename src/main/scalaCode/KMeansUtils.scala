package scalaCode

import breeze.linalg.Vector
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.linalg.{Vector => MlVector}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * Utility class for KMeans.
 */
object KMeansUtils {

  /**
   * Useful when mapping from file to DF to handle multiple delimiters and ensures the proper data types.
   * Does not expect trailing playlists.
   */
  case class Song(
                   id: String,
                   title: String,
                   artist: String,
                   duration: Double,
                   key: Double,
                   modality: Double,
                   signature: Double,
                   acousticness: Double,
                   danceability: Double,
                   energy: Double,
                   instrumentalness: Double,
                   liveness: Double,
                   loudness: Double,
                   speechiness: Double,
                   valence: Double,
                   tempo: Double
                 )

  /**
   * Useful when mapping from file to DF to handle multiple delimiters and ensures the proper data types.
   * Expects trailing playlists.
   */
  case class SongWithList(
                           id: String,
                           title: String,
                           artist: String,
                           duration: Double,
                           key: Double,
                           modality: Double,
                           signature: Double,
                           acousticness: Double,
                           danceability: Double,
                           energy: Double,
                           instrumentalness: Double,
                           liveness: Double,
                           loudness: Double,
                           speechiness: Double,
                           valence: Double,
                           tempo: Double,
                           playlists: Array[Int]
                         )

  /**
   * Get a DF with song features (not expecting trailing playlists)
   */
  def getSongsDF(spark: SparkSession, inputDir: String): DataFrame = {
    import spark.implicits._
    val songs = spark.sparkContext
      .textFile(inputDir)
      .map(line => line.replace("\t", ",").split(","))
      .map {
        case Array(id,title,artist,duration,key,modality,signature,acousticness,danceability,energy,instrumentalness,
        liveliness,loudness,speechiness,valence,tempo)
        =>
          Song(id,title,artist,duration.toDouble,key.toDouble,modality.toDouble,signature.toDouble,acousticness.toDouble,
            danceability.toDouble,energy.toDouble,instrumentalness.toDouble,liveliness.toDouble,loudness.toDouble,
            speechiness.toDouble,valence.toDouble,tempo.toDouble)
      }
      .toDF()
    return songs
  }

  /**
   * Get a DF with song features (expecting trailing playlists)
   *   Working on songs with playlists appended to normal line as: ,[pid;pid;pid;...]
   *   Handles empty playlists, but that case shouldn't occur from the MPD.
   */
  def getSongsWithListsDF(spark: SparkSession, inputDir: String): DataFrame = {
    import spark.implicits._
    val songs = spark.sparkContext
      .textFile(inputDir)
      .map(line => line.replace("\t", ",").split(","))
      .map {
        // Whole block below: just takes all values expected from case class above & maps with correct data type
        case Array(id,title,artist,duration,key,modality,signature,acousticness,danceability,energy,instrumentalness,
        liveliness,loudness,speechiness,valence,tempo,playlist)
        =>
          SongWithList(id,title,artist,duration.toDouble,key.toDouble,modality.toDouble,signature.toDouble,
            acousticness.toDouble, danceability.toDouble,energy.toDouble,instrumentalness.toDouble,liveliness.toDouble,
            loudness.toDouble, speechiness.toDouble,valence.toDouble,tempo.toDouble,
            // Handles case when playlists is empty (although that should not happen for songs from MPD...)
            if (playlist.length > 2)
              ((playlist.replace("[","").replace("]",""))
                .split(";"))
                .map(str => str.toInt)
            else Array()
          )
      }
      .toDF()
    return songs
  }

  /**
   * Get an array of the scaled features.
   * @param songs DataFrame of the songs file data
   * @return Array(Vector(Double)) of scaled song features
   */
  def getScaledFeatures(songs: DataFrame): RDD[Vector[Double]] = {

    // assemble song features into a features vector
    val assembler = new VectorAssembler()
      .setInputCols(Array("duration", "key", "modality", "signature", "acousticness",
        "danceability", "energy", "instrumentalness", "liveness", "loudness", "speechiness", "valence", "tempo"))
      .setOutputCol("features")
    val transformVector = assembler.transform(songs)

    // scale such that mean = 0 and standard deviation = 1
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)
    val scalerModel = scaler.fit(transformVector)
    val scaledData = scalerModel.transform(transformVector)

    // return only scaled features as an RDD[Vector]
    val scaledFeatures = scaledData
      .select("scaledFeatures")
      .rdd
      .map(r => r.getAs[MlVector](0).toArray)
      .map(a => Vector(a))

    return scaledFeatures
  }

  def getScaledFeaturesWithData(songs: DataFrame): RDD[(String,String,Vector[Double])] = {
    // assemble song features into a features vector
    val assembler = new VectorAssembler()
      .setInputCols(Array("duration", "key", "modality", "signature", "acousticness",
        "danceability", "energy", "instrumentalness", "liveness", "loudness", "speechiness", "valence", "tempo"))
      .setOutputCol("features")
    val transformVector = assembler.transform(songs)

    // scale such that mean = 0 and standard deviation = 1
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)
    val scalerModel = scaler.fit(transformVector)
    val scaledData = scalerModel.transform(transformVector)

    // Keep data together, returning as (String, String, Vector[Double])
    val scaledFeatures = scaledData.select("title","artist", "scaledFeatures")
      .rdd
      .map(row => row.toSeq)
      .map(row => (row(0).toString, row(1).toString, Vector(row(2).toString.replace("[", "").replace("]","").split(",").map(str => str.toDouble))))

    return scaledFeatures
  }
}