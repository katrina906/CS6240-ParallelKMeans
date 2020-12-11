package scalaCode

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import scalaCode.KMeansUtils.{getScaledFeaturesWithData, getSongsDF}
import breeze.linalg.{DenseVector, Vector, squaredDistance}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.Random

object KMeansMultiK {

  /**
   * Check that the correct number of args is given.
   */
  def numArgs(args: Array[String], logger: Logger): Unit = {
    if (args.length != 7) {
      logger.error("Usage: Provide 7 Arguments:<input-data-dir> <setup-dir> <output-dir> \n" +
                   "                           <algo (kmeans or kmedoids)> <balance (bool)> \n" +
                   "                           <num-machines> <name-centers (bool)>")
      System.exit(1)
    }
  }

  /**
   * Pick K random indices from range 0 - end, without replacement.
   */
  def randomCenters(K: Int, end: Int, seed: Int): ListBuffer[Int] = {
    val random = new Random(seed)
    var centers = new ListBuffer[Int]
    for (i <- 0 until K) {
      var index = random.nextInt(end)
      while (centers.contains(index)) {
        index = random.nextInt(end)
      }
      centers += index
    }
    return centers
  }

  /**
   * Initialize a list of K lists to hold song assignments to clusters, (cluster number implied by index).
   */
  def initAssignments(K: Int): ListBuffer[ListBuffer[Int]] = {
    var assignments = new ListBuffer[ListBuffer[Int]]
    for (i <- 0 until K) {
      assignments += new ListBuffer[Int]
    }
    return assignments
  }

  /**
   * Takes a list of ids, looks up their vectors in features, and returns an array of their vectors in the given order.
   */
  def vectorize(idList: ListBuffer[Int], features: Array[Vector[Double]]): Array[Vector[Double]] = {
    var vectors = new ListBuffer[Vector[Double]]
    for (id <- idList) {
      vectors += features(id)
    }
    return vectors.toArray
  }

  /**
   * Finds the closest index. Taken from Example code for CS 6240 KMeans.
   */
  def closestPoint(song: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      // TODO replace with Euclidean (& maybe Euclidean with Jaccard inside for pids list)
      val tempDist = squaredDistance(song, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }

  /**
   * Assigns all songs in the features array to the closest cluster by adding its index to the appropriate list.
   */
  def reassign(features: Array[Vector[Double]], centers: Array[Vector[Double]]): ListBuffer[ListBuffer[Int]] = {
    var assignments = initAssignments(centers.length)
    for (songID <- 0 until features.length) {
      assignments(closestPoint(features(songID), centers)) += songID
    }
    return assignments
  }

  /**
   * Gets the difference in distance from the old centroids to the new centroids, i.e. the delta of this iteration.
   */
  def getDelta(prevCentroids: Array[Vector[Double]], newCentroids: Array[Vector[Double]]): Double = {
    var delta: Double = 0.0
    // Accumulate the change in distance for all clusters from the old centroids to the new centroids
    for (cluster <- 0 until prevCentroids.length) {
      delta += squaredDistance(prevCentroids(cluster), newCentroids(cluster))
    }
    return delta
  }

  /**
   * Recomputes the most central point for a cluster and returns a list of the new centroids (cluster implied by index).
   * The new central point chosen is a real song that exists in the input data (and within the cluster).
   */
  def recomputeMedoids(features: Array[Vector[Double]], assignments: ListBuffer[ListBuffer[Int]]): ListBuffer[Int] = {
    var newCentroids = new ListBuffer[Int]
    // For each cluster
    for (cluster <- 0 until assignments.length) {
      // Consider every song as a possible center
      var minDistance = Double.MaxValue
      var bestCandidate = 0
      for (candidate <- 0 until assignments(cluster).length) {
        var candidateDistance: Double = 0
        // Accumulate the distance to every other song in the same cluster
        for (song <- 0 until assignments(cluster).length) {
          // TODO again, replace with our own distance function
          candidateDistance += squaredDistance(features(candidate), features(song))
        }
        // Check if this candidate is the new best
        if (minDistance > candidateDistance) {
          minDistance = candidateDistance
          bestCandidate = candidate
        }
      }
      // Pick the cluster winner and record its id in the new centroids list
      newCentroids += bestCandidate
    }
    return newCentroids
  }

  /**
   * Recomputes the most central point for a cluster and returns a list of the new centroids (cluster implied by index).
   * The new central point chosen is a synthetic average across all dimensions of the songs within the cluster.
   */
  def recomputeMeans(features: Array[Vector[Double]], assignments: ListBuffer[ListBuffer[Int]]): Array[Vector[Double]] = {
    val K = assignments.length
    val D = features(0).length
    var newCentroids = new Array[Vector[Double]](K)
    // For each cluster
    for (cluster <- 0 until K) {
      // Create a synthetic data point by finding the average along all dimensions
      var sum: Vector[Double] = DenseVector.zeros(D)
      for (songID <- assignments(cluster)) {
        sum += features(songID)
      }
      // Get average by doing element-wise division with a vector where all elements = num songs in this cluster
      newCentroids(cluster) = sum /:/ DenseVector.fill(D){assignments(cluster).length.toDouble}
    }
    return newCentroids
  }

  /**
   * Converts an array of vectors into an array of arrays. Used to make final centroids readable in the output file.
   */
  def readableVectors(vectors: Array[Vector[Double]]): ListBuffer[ListBuffer[Double]] = {
    var readables: ListBuffer[ListBuffer[Double]] = new ListBuffer[ListBuffer[Double]]
    for (vector <- vectors) {
      var elements: ListBuffer[Double] = new ListBuffer[Double]
      for (value <- vector) {
        elements += value
      }
      readables += elements
    }
    return readables
  }

  /**
   * Runs an instance of KMedoids on one machine. Use inside of an RDD function, like .map()
   * The medoids returned as centers are real songs from the input data.
   */
  def KMedoidsLocal(features: Array[Vector[Double]], setup: Setup): RawClustering = {
    // Pick initial centers by random assignment
    var medoids: ListBuffer[Int] = randomCenters(setup.K, features.length, setup.seed)
    var assignments: ListBuffer[ListBuffer[Int]] = initAssignments(setup.K)

    // Run KMedoids until convergence defined by epsilon or until iterations limit is reached
    var delta: Double = Double.MaxValue
    var counter: Int = 0
    while (delta > setup.epsilon && counter < setup.maxIters) {
      // Assign all songs to a cluster based on centroid distance measure
      assignments = reassign(features, vectorize(medoids, features))
      // Calculate new centroids based on reassignments
      var newMedoids = recomputeMedoids(features, assignments)
      // Get the delta between the old centroids and the new centroids
      delta = getDelta(vectorize(medoids, features), vectorize(newMedoids, features))
      // Reassign centroids for the next iteration
      medoids = newMedoids
      counter += 1
    }
    val centroids: Array[Vector[Double]] = vectorize(medoids, features)
    return RawClustering(counter, getWsos(assignments, centroids, features), assignments, readableVectors(centroids))
  }

  /**
   * Runs an instance of KMeans on one machine. Use inside of an RDD function, like .map()
   * The centroids returned as centers are synthetic averages of the songs within the cluster.
   */
  def KMeansLocal(features: Array[Vector[Double]], setup: Setup): RawClustering = {
    // Pick initial centers by random assignment
    var centroids: Array[Vector[Double]] = vectorize(randomCenters(setup.K, features.length, setup.seed), features)
    var assignments: ListBuffer[ListBuffer[Int]] = initAssignments(setup.K)

    // Run KMeans until convergence defined by epsilon or until iterations limit is reached
    var delta: Double = Double.MaxValue
    var counter: Int = 0
    while (delta > setup.epsilon && counter < setup.maxIters) {
      // Assign all songs to a cluster based on centroid distance measure
      assignments = reassign(features, centroids)
      // Calculate new centroids based on the reassignments
      var newCentroids = recomputeMeans(features, assignments)
      // Get the delta between the old centroids and the new centroids
      delta = getDelta(centroids, newCentroids)
      // Reassign the centroids for the next iteration
      centroids = newCentroids
      counter += 1
    }
    return RawClustering(counter, getWsos(assignments, centroids, features), assignments, readableVectors(centroids))
  }

  /**
   * Gets the within sum of squares error.
   */
  def getWsos(assignments: ListBuffer[ListBuffer[Int]], centroids: Array[Vector[Double]], features: Array[Vector[Double]]): Double = {
    var count: Double = 0.0
    var error: Double = 0.0
    for (cluster <- 0 until assignments.length) {
      // Accumulate the total number of songs
      count += assignments(cluster).length
      var center: Vector[Double] = centroids(cluster)
      for (song <- assignments(cluster)) {
        // Accumulate the total distance of each song from its assigned center
        error += squaredDistance(center, features(song))
      }
    }
    if (count == 0) {
      return 0.0
    }
    // Output the average of all song-center errors for this KMeans run (a single decimal number)
    return error / count
  }

  /**
   * Estimates the relative load of a run based on its setup parameters.
   * The heuristic is inspired by the runtime of KMeans, O(N * K * I * D). We ignore N and D, since the input size and
   * number of dimensions is constant for all runs.
   */
  def estimateWork(setup: Setup): Long = {
    return (setup.K * setup.maxIters).toLong
  }

  /**
   * Gets the sum of work units for setup runs assigned to each machine, machine # implied by array index.
   */
  def machineSums(assignments: Array[ListBuffer[(Long, Setup)]]): Array[Long] = {
    var sums: Array[Long] = new Array[Long](assignments.length)
    for (i <- 0 until assignments.length) {
      sums(i) = assignments(i).map(data => data._1).sum
    }
    return sums
  }

  /**
   * Get a list containing the element for each machine at this column.
   */
  def getWindow(assignments: Array[ListBuffer[(Long, Setup)]], column: Int): Array[(Long,Setup)] = {
    var window: Array[(Long,Setup)] = new Array[(Long, Setup)](assignments.length)
    for (i <- 0 until assignments.length) {
      window(i) = assignments(i)(column)
    }
    return window
  }

  /**
   * Exchanges elements during pairwise sliding window exchange process.
   *
   * @param column Int, the column of the matrix
   * @param assignments Array of List (Long,Setup), current machine assignments for setup runs and their work estimates
   * @param first Int, first machine (i.e. row in matrix)
   * @param second Int, second machine (i.e. row in matrix)
   * @return Assignments, with the two elements swapped
   */
  def exchange(column: Int, assignments: Array[ListBuffer[(Long,Setup)]], first: Int, second: Int): Array[ListBuffer[(Long,Setup)]] = {
    // Extract old entries
    val firstItem: (Long,Setup) = assignments(first)(column)
    val secondItem: (Long,Setup) = assignments(second)(column)
    // Exchange for the entry on the other machine
    assignments(first)(column) = secondItem
    assignments(second)(column) = firstItem
    return assignments
  }

  /**
   * Attempt to reduce max-min difference by exchanging elements between "outer pairwise lists" (0 & n, 1 & n-1, 2 & n-2, etc.)
   */
  def slidingWindowExchange(assignments: Array[ListBuffer[(Long,Setup)]], start: Int): Array[ListBuffer[(Long,Setup)]] = {
    var assigned = assignments
    val n: Int = assigned.length - 1
    // Reverse for loop looks at biggest elements first in each column of the matrix
    for (i <- start to 0 by -1) {
      // Get the total amounts of work to be done on each machine
      var sums: Array[Long] = machineSums(assigned)
      // Get the run info at the same index across all machines
      var window: Array[(Long, Setup)] = getWindow(assigned, i)

      // Compare outer pairwise elements. For odd numbers of machines, we just ignore the exact middle by flooring the loop
      for (j <- 0 until math.floor(n/2).toInt) {
        var k: Int = n-j
        var diff: Long = math.abs(sums(j) - sums(k))
        // If exchanging these two elements would result in a lower difference between the two loads
        if (math.abs((sums(j) - window(j)._1 + window(k)._1) - (sums(k) - window(k)._1 + window(j)._1)) < diff) {
          // Since this is outer pairwise, we don't need to update sums and windows until the next outer loop iteration
          assigned = exchange(i, assigned, j, k)
        }
      }
    }
    return assigned
  }

  /**
   * An approximate solution to minimizing the subset sum:
   * - Sort in ascending order
   * - Round robin assignment (i % machines)
   * - Walk backwards through the lists and exchange elements that decrease overall set difference
   *
   * @param work RDD (Long,Setup) where long is the number of work units
   * @param machines Int the number of worker machines in this cluster
   * @return Array of Lists of Setups
   */
  def roundRobinAndExchange(work: RDD[(Long, Setup)], machines: Int, logger: Logger): ListBuffer[(Long, Setup)] = {
    // Sort the runs in ascending order of work units
    val sortedWork = work.sortByKey().collect()
    // Initialize a list for each machine
    var assignments: Array[ListBuffer[(Long,Setup)]] = new Array(machines)
    for (i <- 0 until assignments.length) {
      assignments(i) = new ListBuffer[(Long, Setup)]
    }

    // Round Robin assignment - like dealing cards: one to each person until none left
    for (i <- 0 until sortedWork.length) {
      assignments(i % machines) += sortedWork(i)
    }

    // The last list will be the shortest if length of setup file was not a multiple of machines
    val start: Int = assignments(machines - 1).length - 1
    // Sliding Window Exchange: Try to exchange elements to reduce the max-min difference for better load balance
    assignments = slidingWindowExchange(assignments, start)

    // Display the load balancing data
    var balancing: StringBuilder = new StringBuilder()
    balancing.append("\n\t---- LOAD BALANCING RESULTS ----")
    val sums = machineSums(assignments)
    for (i <- 0 until assignments.length) {
      balancing.append("\nMachine : " + i)
      balancing.append("\nWorkload: " + sums(i))
      balancing.append("\nRun List: " + assignments(i))
    }
    balancing.append("\n\t>>> Max-Min Difference: " + (sums.max - sums.min) + "\n")
    logger.info(balancing.toString())

    // Return the partitioning as a ListBuffer of (machine #, setup)
    var partitioning = new ListBuffer[(Long, Setup)]
    for (i <- 0 until assignments.length) {
      for (j <- 0 until assignments(i).length) {
        partitioning += ((i.toLong, assignments(i)(j)._2))
      }
    }
    return partitioning
  }

  /**
   * Remaps the list of integer assignments (index references) to a list of tuples of (clusterID, title, artist).
   * @param assignments
   * @param info
   * @return list of tuples of (clusterID, title, artist)
   */
  def songLookup(assignments: ListBuffer[ListBuffer[Int]], info: Array[(String, String)]): ListBuffer[(Int, String,String)] = {
    var readable: ListBuffer[(Int, String,String)] = new ListBuffer[(Int, String, String)]
    for (cluster <- 0 until assignments.length) {
      for (song <- assignments(cluster)) {
        var ref: (String,String) = info(song)
        readable += ((cluster, ref._1, ref._2))
      }
    }
    return readable
  }

  /**
   * Preprends the cluster ID number to the centroid.
   * @param assignments
   * @param info
   * @return
   */
  def indexCentroids(centroids: ListBuffer[ListBuffer[Double]]): ListBuffer[(Int, ListBuffer[Double])] = {
    var indexedCentroids = new ListBuffer[(Int, ListBuffer[Double])]
    for (i <- 0 until centroids.length) {
      indexedCentroids += ((i, centroids(i)))
    }
    return indexedCentroids
  }

  /**
   * Finds the song name based on its feature vector. This operation may be slow on huge datasets.
   * @param center
   * @param scaledFeatures
   * @param info
   * @return
   */
  def seekIndexByVector(center: Vector[Double], scaledFeatures: Array[Vector[Double]], info: Array[(String, String)]): (String,String) = {
    val compare = DenseVector.fill(center.length)(0.0)
    for (i <- 0 until scaledFeatures.length) {
      // Verified that equality is based on dimensional attributes, not memory address
      if (center == scaledFeatures(i)) {
        return info(i)
      }
    }
    return (("<Not Found>", "<Not Found>"))
  }

  /**
   * Finds the real songs that correspond to the centroid vectors. These do not exist for KMeans, but do for KMedoids.
   * @param centroids
   * @param info
   * @param scaledFeatures
   * @return
   */
  def centerLookup(centroids: ListBuffer[ListBuffer[Double]], info: Array[(String, String)], scaledFeatures: Array[Vector[Double]], nameCentroids: Boolean): ListBuffer[(Int,String,String)] = {
    var readableCentroids = new ListBuffer[(Int,String,String)]
    for (i <- 0 until centroids.length) {
      if (!nameCentroids) {
        readableCentroids += ((i, "<Synthetic>", "<Synthetic>"))
      }
      else {
        var ref = seekIndexByVector(Vector(centroids(i).toArray), scaledFeatures, info)
        readableCentroids += ((i, ref._1, ref._2))
      }
    }
    return readableCentroids
  }

  /**
   * Turns a RDD of RawClusterings into an RDD of ReadableClusterings (just a convenience method).
   */
  def remapAsInfo(raw: RDD[(Setup,RawClustering)], info: Array[(String, String)], scaledFeatures: Array[Vector[Double]], nameCentroids: Boolean): RDD[(Setup,ReadableClustering)] = {
    return raw.mapValues(data => ReadableClustering(data.iters,
      data.wsos,
      songLookup(data.assignments, info),
      indexCentroids(data.centroids),
      centerLookup(data.centroids, info, scaledFeatures, nameCentroids)))
  }

  /**
   * Custom Partitioner is really just an identity partitioner, expecting that the machine assignments were already
   * determined by some pre-processing and given as the key.
   * The number of available machines should be passed in as numPartitions.
   */
  class MachinePartitioner(override val numPartitions: Int) extends Partitioner {
    // The given key should already be the machine assignment, so use it as the partition number
    override def getPartition(key: Any): Int = {
      if (key.isInstanceOf[Long]) {
        return key.toString.toInt
      }
      else if (key.isInstanceOf[Int]) {
        return key.asInstanceOf[Int]
      }
      else {
        scala.sys.error("Machine Partitioner - Key Type Error: Expected Long or Int, but received " + key.getClass)
      }
    }
  }

  /**
   * Used to return the results of a local clustering algorithm function call.
   */
  case class RawClustering(iters: Int,
                           wsos: Double,
                           assignments: ListBuffer[ListBuffer[Int]],
                           centroids: ListBuffer[ListBuffer[Double]])

  /**
   * Used to print out the cluster results in a readable format
   * @param assignments
   * @param centroids
   * @param realCenters
   */
  case class ReadableClustering(iters: Int,
                                wsos: Double,
                                assignments: ListBuffer[(Int, String,String)],
                                centroids: ListBuffer[(Int, ListBuffer[Double])],
                                realCenters: ListBuffer[(Int, String,String)])

  /**
   * Used to read in the setup file and organize clustering calls.
   */
  case class Setup(K: Int, seed: Int, maxIters: Int, epsilon: Double)

  /**
   * Driver function.
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spotify KMeans: Try Multiple K Simultaneously")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // Check that the correct number of args is given
    val logger: Logger = LogManager.getRootLogger
    // TODO update numArgs to expect 7 arguments
    numArgs(args, logger)

    // Rename for readability later
    val input: String = args(0)
    val setup: String = args(1)
    val output: String = args(2)
    // TODO take all of these as program arguments. Need to change Makefile later, leaving it for now to avoid conflicts
    var clusterAlgorithm: (Array[Vector[Double]], Setup) => RawClustering = KMeansLocal
    var algo: String = "KMeans"
    if (args(3).toLowerCase() == "kmedoids") {
      clusterAlgorithm = KMedoidsLocal
      algo = "KMedoids"
    }
    var balance: Boolean = true
    if (args(4).toLowerCase() == "false") {
      balance = false
    }
    val machines: Int = args(5).toInt
    var nameCentroids: Boolean = false
    if (args(6).toLowerCase() == "true") {
      nameCentroids = true
    }

    // Echo parameter details to logs
    logger.info("\n---- PROGRAM ARGS ----" + "\nInput Path : " + input + "\nSetup Path : " + setup + "\nOutput Path: " +
      output + "\nAlgorithm  : " + algo + "\nBalance    : " + balance + "\nMachines   : " + machines +
      "\nName Centroids: " + nameCentroids)

    // Load song data from file, and split into index consistent arrays: (title,artist) & (feature vector)
    val data = getScaledFeaturesWithData(getSongsDF(spark, input)).collect()
    val scaledFeatures = spark.sparkContext.broadcast(data.map(row => row._3))
    val songsInfo = spark.sparkContext.broadcast(data.map(row => (row._1, row._2)))

    // Load run setup info. Assumes Setup file is given in N line format as: K, Random Seed, Max Iterations, Epsilon
    // Non-balanced run just assigns keys based on indexing with the default hash partitioner. Entries as (index, setup)
    var runs = spark.sparkContext.textFile(setup)
      .map(line => line.split(" "))
      .map(array => Setup(array(0).toInt, array(1).toInt, array(2).toInt, array(3).toDouble))
      .zipWithIndex()
      .map(inverted => (inverted._2, inverted._1))

    // Load balancing creates minimally different buckets by heuristic(setup), changes keys to set machine assignments
    if (balance) {
      // Create an RDD of (work units, setup) to use for machine assignments
      val work = runs.map(instance => (estimateWork(instance._2), instance._2))
      // Get a list of the setup run machine assignments, with each entry as (machine #, setup), ready to be a PairRDD
      val partitioning = roundRobinAndExchange(work, machines, logger)
      // Final: runs pairRDD, with setups assigned to machines
      runs = spark.sparkContext.parallelize(partitioning)
       .partitionBy(new MachinePartitioner(machines))
    }

    // Launches a map-only job where the clustering algorithm is just executed within the map itself
    val raw = runs.map(instance => (instance._2, clusterAlgorithm(scaledFeatures.value, instance._2)))
    val results = remapAsInfo(raw, songsInfo.value, scaledFeatures.value, nameCentroids)

    // Results written out to a single line, with the following schema:
    //   Setup(setup line)                       - The setup details for this run, echoed from the setup input file.
    //   ReadableCluster(int,                    - The number of iterations it took to converge (or stopped at max)
    //   ListBuffer((cluster,title,artist),...)  - Readable song list containing cluster assignment, title and artist.
    //   ListBuffer((cluster,centroid vals),...) - List of centroids' data and their cluster assignments.
    //   ListBuffer((cluster,title,artist),...)) - List of centroids' song info (if applicable) and cluster assignments.
    results.saveAsTextFile(output)
  }
}
