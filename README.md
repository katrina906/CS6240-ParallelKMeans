# CS6240-Spotify-Clustering
## Authors
George Dunnery            
Katrina Truebenbach

## Overview
This project creates a Spotify radio application by implementing distributed K-means clustering for over 2.5 million Spotify songs. Given a song selected by a user, we can present other similar songs in its cluster that the user will likely also enjoy. We are clustering based on 13 numeric audio features such as time signature, energy, and danceability.      

We implement distributed K-means using two methods: (1) distributed K-means that parallelizes the computation for each K and (2) sequential simultaneous K-means that computes each clustering locally while exploring different K values in parallel. We evaluate our clusters with Within Cluster Sum of Squares error (average distance between each song and its final cluster center) and choose the best K value by finding an “elbow” in the decreasing error over increasing values of K. This is the appropriate evaluation metric because for the radio application, we are more concerned about songs within a cluster being closely related than with clusters being distinct.      

Our two implementations of K-means are both highly scalable and exhibit strong speedup. For distributed K-means, we utilize Spark’s ability to hold data in memory between iterations to increase efficiency, and for sequential simultaneous K-means, we implement load balancing by balancing how setup configurations are assigned to machines. We also perform a series of experiments to understand how various parameters affect the algorithms and present interesting findings. We believe that we have successfully implemented effective, scalable clustering to find related songs to support a radio application. 


## Resources
Spotify Million Playlist Dataset:                     
https://www.kaggle.com/sadakathussainfahad/spotify-million-playlist-dataset                            
Spotify Web API:           
https://developer.spotify.com/documentation/web-api/         
Spotify Web API Audio Features:            
https://developer.spotify.com/documentation/web-api/reference/tracks/get-audio-features        
Spotify Web API Java Wrapper:            
https://github.com/thelinmichael/spotify-web-api-java

## Environment Setup

### Spark
1. Example ~/.bash_aliases: export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/ export HADOOP_HOME=/usr/local/hadoop/hadoop-2.9.2 export SCALA_HOME=/usr/share/java/scala-2.11.12 export SPARK_HOME=/opt/spark/spark-2.4.7-bin-without-hadoop export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin export SPARK_DIST_CLASSPATH=$(hadoop classpath)
2. Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh: export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/

### MapReduce
1. Example ~/.bash_aliases: export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/ export HADOOP_HOME=/usr/local/hadoop/hadoop-2.9.2 export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
2. Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh: export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/

## Scripts 

### Data Creation (MapReduce/Java)
We use the song IDs from the Spotify Million Playlist Dataset to pull numeric audio features for each song from the Spotify Web API. We parallelized this process in MapReduce.
Source code folder: src/main/javaCode/data     
Scripts:           
- SliceCompressor.java: Compress JSON slices from Spotify Million Playlist Dataset into single line files so that MapReduce can take each line as input to the Mapper.
- MRDataProcessor.java: MapReduce program to read in playlist JSON objects, extract the individual songs, drop duplicate songs within and between playlists, and call the Spotify API to return one line per song with their audio features in a list. Also returns a list of all songs in each playlist. 
  - Input arguments: inputJsonLines (input directory: output from SliceCompressor), intermediate (intermediate directory), outputPlaylists (output playlists directory), outputSongs (output songs directory), clientID (spotify web api client id), clientSecret (spotify web api secret) 
- SpotifyData.java: helper class to use the Spotify API
- JSONConverter.java: helper class to parse the JSON data and create a hashmap of unique songs in each playlist
- LocalDatProcessor.java: local, java version of MRDataProcessor. Used for development 

To generate data:         
1. Run SliceCompressor.java (must be run locally)              
2. Run MRDataProcessor.java              

### Distributed K Means (Spark/Scala)
Source code folder: src/main/scalaCode
Scripts:
- KMeansParallelK.scala: implement distributed K-Means that parallelizes the computation for each K
  - Input arguments: inputDir (input directory), outputDir (output directory for cluster assignments), wSoSDir (output directory for within sum of squares error for each clustering), KArray (list of K values to try separated by commas), convergeDist (distance at which clustering converges), maxIters (maximum number of iterations), seed (random seed for selecting centers), algorithm (kmeans or kmedoids) 
- KMeansMultiK.scala: implement sequential K-means on multiple machines simultaneously
  - Input arguments: inputDir (input directory), setupDir (containing a single setup file), outputDir (output directory for clustering results of each setup line), algorithm (kmeans or kmedoids), balance (true/false for load balancing with partitioner), machines (number of machines), nameCenters (true/false for finding medoid centers if algorithm = kmedoids)
- KMeansUtils.scala: helper class to download songs data, format into dataframe, and scale numeric features such that mean = 0, standard deviation = 1

### Finding Optimal K (Python) 
We calculate the within cluster sum of squares error (wSoS) for each K value used in KMeansParallelK or KMeansMultiK. We feed these values into findBestClustering.py to plot the values of wSoS against K. We then find the optimal value of K by visually assessing this plot to find an "elbow" where the decrease in wSoS is minimal as K increases. We cannot simply choose the K with minimum wSoS because the error naturally decreases as K increases. 
- findBestClustering.py (must be run locally)
	- Input arguments: inputDir (input directory with wSoS errors from clustering), outputDir (directory to save plot to), algorithm (if contains any value, then will parse wsos output from multik. If no value in argument, will parse wsos output from parallelk) 


## Execution 

1. Open command prompt.
2. Navigate to directory with project files
3. Edit the Makefile to customize the environment and input arguments at the top.
4. Standalone Hadoop:
- Setup (set standalone Hadoop environment - execute once): ```make switch-standalone```
- Code change for Spark: specify "local" master when create spark context: ```val spark = SparkSession.builder().appName("").master("local").getOrCreate()```
- Run Scripts:
  - MRDataProcessor: ```make local-mr```
  - KMeansParallelK: ```make local-spark-parallelk```
  - KMeansMultiK: ```make local-spark-multik```
  - findBestClustering: ```make local-python-plot```
5. Pseudo-Distributed Hadoop:
- Setup (set pseudo-clustered Hadoop environment - execute once): ```make switch-pseudo```
- Run Scripts (first execution): 
   - MRDataProcessor: ```make pseudo-mr```
   - KMeansParallelK: ```make pseudo-spark-parallelk```
   - KMeansMultiK: ```mark pseudo-spark-multik```
- Run Scripts (later excecutions): 
   - MRDataProcessor: ```make pseudoq-mr```
   - KMeansParallelK: ```make pseudoq-spark-parallelk```
   - KMeansMultiK: ```make pseudoq-spark-multik```
6. AWS EMR Hadoop:
- Upload input:
   - MRDataProcessor: ```make upload-input-aws-mr```
   - KMeansParallelK: ```make upload-input-aws-spark-parallelk```
   - KMeansMultiK: ```make upload-input-aws-spark-multik```
- Run Scripts:
   - MRDataProcessor: ```make aws-mr```
   - KMeansParallelK: ```make aws-spark-parallelk```
   - KMeansMultiK: ```make aws-spark-multik```
- Check for successful execution with aws web interface
- After successful execution & termination, download log and output files: 
  - MRDataProcessor: ```make download-output-aws-mr```
  - KMeansParallelK: ```make download-output-aws-spark-parallelk```
  - KMeansMultiK: ```make download-output-aws-spark-multik```
