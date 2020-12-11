package scalaCode

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.log4j.{Level, LogManager, Logger}
import breeze.linalg.{Vector, squaredDistance}
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.runtime.ScalaRunTime.stringOf

import KMeansUtils.{getScaledFeatures, getSongsDF}

object KMeansParallelK {

  // if user didn't enter correct number of arguments, log error and cancel program
  def numArgs(args: Array[String], logger: Logger): Unit = {
    if (args.length != 8) {
      logger.error("Usage: Provide 8 Arguments for scalaCode.KMeansParallelK as described below:" +
        "\nscalaCode.KMeansParallelK <input dir: String> <output-dir: String> <wSoS-dir: String> <K-list: Comma Separated String>" +
        " <converge-dist: Double> <max-iters: Int> <seed: Int> <algorithm: String>")
      System.exit(1)
    }
  }

  // find closest center to each point (song)
  def findClosestCenter(p: Vector[Double], centers: scala.collection.Map[Int, Vector[Double]]): Int = {
    var bestKey = 0
    var closest = Double.PositiveInfinity

    // loop through centers and find closest to the inputted point
    for (k <- centers.keys) {
      val tempDist = squaredDistance(p, centers(k))
      if (tempDist < closest) {
        closest = tempDist
        bestKey = k
      }
    }
    bestKey
  }

  // find medoid cluster center: best center out of available points
  def findCenterMedoid(points: List[Vector[Double]]): Vector[Double] = {
    var bestCenter = points(0)
    var bestDist = Double.PositiveInfinity
    // consider each point as a potential center
    for (c <- points.indices) {
      val currentCenter = points(c)
      var currentDist = 0.0
      // sum distance from currentCenter to each point in its cluster
      for (p <- points.indices) {
        currentDist += squaredDistance(points(p), currentCenter)
      }
      // keep track of best center out of available points
      if (currentDist < bestDist) {
        bestDist = currentDist
        bestCenter = currentCenter
      }
    }
    bestCenter
  }

  // find mean cluster center: average of each dimension for points in cluster
  def findCenterMean(closest: RDD[(Int, (Vector[Double], Int))]): scala.collection.Map[Int, Vector[Double]] = {
    // create pair RDD where key = cluster number and value = ([sum of each dimension], number of points in cluster)
    // number of rows = number of clusters
    // Note reduceByKey combines before shuffling
    val pointStats = closest.reduceByKey { case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2) }
    // calculate average of points (average of each dimension) in each cluster to find new centers
    // collect as map object
    val newCenters = pointStats.map { pair =>
      (pair._1, pair._2._1 * (1.0 / pair._2._2))
    }.collectAsMap()

    newCenters
  }

  def clusterAssignmentMedoids(scaledFeatures: RDD[Vector[Double]],
                               centers: Broadcast[scala.collection.Map[Int, Vector[Double]]]):
  (RDD[(Int, (Vector[Double], Int))], scala.collection.Map[Int, Vector[Double]]) = {

    // find closest center to each point (song)
    // results in pair RDD where key = cluster number and value = point. Number of rows = number of songs
    val closest = scaledFeatures.map(p => (findClosestCenter(p, centers.value), (p, 1)))
    // collapse such that key = cluster number and value of = list of all points. Number of rows = number of clusters
    val closestList = closest
      .mapValues(value => value._1) // drop 1 in value. Needed for consistent format with clusterAssignmentMean
      .aggregateByKey(List[Vector[Double]]())(
        (acc, x) => x :: acc,
        (acc1, acc2) => acc1 ::: acc2
      )
    // find best center for each cluster out of points in that cluster
    val newCenters = closestList.mapValues(points => findCenterMedoid(points))
      .collectAsMap()

    (closest, newCenters)
  }

  // assign points to closest center and calculate new centers
  def clusterAssignmentMean(scaledFeatures: RDD[Vector[Double]],
                            centers: Broadcast[scala.collection.Map[Int, Vector[Double]]]):
  (RDD[(Int, (Vector[Double], Int))], scala.collection.Map[Int, Vector[Double]]) = {

    // find closest center to each point (song)
    // Results in pair RDD where key = cluster number and value = (point, 1). 1 so can count number of points in cluster
    // number of rows = number of songs
    val closest = scaledFeatures.map(p => (findClosestCenter(p, centers.value), (p, 1)))

    // find mean cluster center: average of each dimension for points in cluster
    val newCenters = findCenterMean(closest)

    (closest, newCenters)
  }

  // find if converged: how much have centers changed?
  def checkConvergence(centers: Broadcast[scala.collection.Map[Int, Vector[Double]]],
                       newCenters: Broadcast[scala.collection.Map[Int, Vector[Double]]]): Double = {
    // check that centers and newCenters have the same set of keys
    // it is possible for a cluster to be assigned no closest points
    val diff = (centers.value.keySet diff newCenters.value.keySet).size
    var tempDist = 0.0

    // if have the same keys, calculate the total distance between centers and newCenters
    if (diff == 0) {
      for (i <- newCenters.value.keys) {
        tempDist += squaredDistance(centers.value(i), newCenters.value(i))
      }
    }
    // if have different keys, then return infinity -> if number of clusters has changed, then has not converged
    else {
      tempDist = Double.PositiveInfinity
    }
    tempDist
  }

  // format cluster assignments for output:
  // zip cluster assignments for each point (song) with their title and artist and sort on cluster assignment
  def outputFormat(songs: DataFrame, closest: RDD[(Int, (Vector[Double], Int))]): RDD[(Int, Row)] = {
    songs
      .select("title", "artist")
      .rdd
      .zip(closest.keys)
      // switch keys and values so key = cluster assignment
      .map(pair => (pair._2, pair._1))
      // sort by cluster assignment
      .sortByKey()
  }

  def main(args: Array[String]) {

    // set up spark environment
    val spark = SparkSession.builder()
      .appName("Spotify KMeans Parallel K")
      //.master("local")
      .getOrCreate()

    // set up logging
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    // check correct number of arguments given
    numArgs(args, logger)

    // Rename arguments to make them readable & cast expected types to avoid type errors in later use
    val inputDir: String = args(0)
    val outputDir: String = args(1)
    val wSoSDir: String = args(2)
    val KArray: Array[Int] = args(3).split(',').map(_.toInt) // list of k values to try. Convert to array
    val convergeDist: Double = args(4).toDouble
    val maxIters: Int = args(5).toInt
    val seed: Int = args(6).toInt
    val algorithm: String = args(7)

    // check that user inputted an available algorithm
    if ((algorithm != "kmeans") && (algorithm != "kmedoids")) {
      logger.error("Incorrect type of algorithm entered. Use either kmeans or kmedoids")
      System.exit(1)
    }
/*
    // Delete output directory(s), only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val deletePaths = hdfs.globStatus(new Path(outputDir + "*")).map(_.getPath)
    try {
      deletePaths.foreach { path => hdfs.delete(path, true) }
    } catch {
      case _: Throwable =>
    }
    val deletePaths2 = hdfs.globStatus(new Path(wSoSDir + "*")).map(_.getPath)
    try {
      deletePaths2.foreach { path => hdfs.delete(path, true) }
    } catch {
      case _: Throwable =>
    }
    // ================
*/
    // Load song data from file & scale features to prepare data for clustering
    // cache so that partitions are kept for each "map" task between iterations without recomputing
    val songs = getSongsDF(spark, inputDir)
    val scaledFeatures = getScaledFeatures(songs).cache() // scaledFeatures: RDD[Vector[Double]]

    // initialize list of (k, average within sum of squares)
    var wSoSIterations = new ListBuffer[Array[Double]]()

    // create clusterings for given values of K and find average within sum of squares error (wSoS) for each clustering
    for (k <- KArray) {

      // broadcast k to all executors rather than all tasks
      val K = spark.sparkContext.broadcast(k)

      // get K random points as initial centers, collect as Map where key = cluster number, and broadcast
      // must use map in case some clusters do not always receive points in every iteration. Cannot rely on index.
      val centersSample = spark.sparkContext
        // get K random points as initial centers. Array[Vector[Double]], length = K
        .parallelize(scaledFeatures.takeSample(withReplacement = false, K.value, seed))
        // join with index and make index key. Interpret as key = cluster number
        .zipWithIndex()
        .map(pair => (pair._2.toInt, pair._1))
        // collect as map where key = cluster number, value = center
        .collectAsMap()
      // broadcast to all executors
      var centers = spark.sparkContext.broadcast(centersSample)

      // iterate until change in distance from old to new centers is less than convergence distance
      // OR reach max iterations (user input)
      var tempDist = Double.PositiveInfinity
      var iter = 0
      while ((tempDist > convergeDist) && (iter < maxIters)) {

        // assign points to clusters and recalculate cluster centers
        val clusterResult = {
          if (algorithm == "kmeans") {
            clusterAssignmentMean(scaledFeatures, centers)
          } else {
            clusterAssignmentMedoids(scaledFeatures, centers)
          }
        }
        // new cluster assignments
        val closest = clusterResult._1
        // broadcast new cluster centers: Map object, all workers need access
        val newCenters = spark.sparkContext.broadcast(clusterResult._2)

        // check convergence between old and new centers
        tempDist = checkConvergence(centers, newCenters)

        // replace old centers with new centers
        centers = newCenters

        iter += 1

        println(s"Finished iteration (delta = $tempDist)")

        // if last iteration, evaluate clusters and save clustering for this K
        if ((tempDist <= convergeDist) || (iter >= maxIters)) {
          // cluster evaluation: within sum of squared errors = average distance between point and cluster center
          // for the radio application, care about clusters being cohesive more than being distinct from each other
          // wSoS will decrease with increasing K; use findBestClustering.py to plot relationship and find elbow -> optimal K
          wSoSIterations += Array(k, closest.map(pair => squaredDistance(newCenters.value(pair._1), pair._2._1)).mean())

          // zip cluster assignments for each point (song) with their title and artist
          val clusterOutput = outputFormat(songs, closest)

          // write to output: specific file for this K clustering
          clusterOutput.saveAsTextFile(outputDir + K.value)
        }
      }

      println(s"Converged in $iter iterations with delta = $tempDist")

    }

    // write out list of wSoS for each K to file
    val wSoSIterationsRDD = spark.sparkContext.parallelize(wSoSIterations.toList)
    wSoSIterationsRDD
      // stringOf so readable
      .map(row => stringOf(row))
      // coalesce to 1 partition so 1 output file
      .coalesce(1)
      .saveAsTextFile(wSoSDir)
  }
}