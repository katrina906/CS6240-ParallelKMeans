# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/usr/local/hadoop/hadoop-2.9.2
spark.root=/opt/spark/spark-2.4.7-bin-without-hadoop
hdfs.user.name=katrina

#hadoop.root=/home/george/spark/spark-2.3.1-bin-without-hadoop
#spark.root=/home/george/hadoop/hadoop-2.9.1
#hdfs.user.name=george

jar.name=spotify-kmeans-1.0.jar
jar.path=target/${jar.name}
app.name=KMeans

# AWS execution
aws.emr.release=emr-5.30.1
aws.region=us-east-1
aws.bucket.name.mr=cs6240-truebenbach/spotify-clustering/mr
aws.bucket.name.spark=cs6240-truebenbach/spotify-clustering/spark
aws.num.nodes=3
aws.instance.type=m4.xlarge

# ------------ MapReduce: SpotifyAPI data -------------------
clientid=e840ce30f1184e6999971e37ef35f5ff
clientsecret=195d5c34b6f242f7afaa1e1022eac024
job.mr.name=javaCode.data.MRDataProcessor

# local arguments
local.mr.input=input_test_line
local.mr.intermediate=intermediate-mr
local.mr.output.songs=output-songs
local.mr.output.playlists=output-playlists
local.mr.log=log-mr

# pseudo-cluster arguments
hdfs.mr.input=input_line
hdfs.mr.intermediate=intermediate-mr
hdfs.mr.output.songs=output-songs
hdfs.mr.output.playlists=output-playlists

# AWS EMR arguments
aws.mr.input=input-mr
aws.mr.intermediate=intermediate-mr
aws.mr.output.songs=output-songs
aws.mr.output.playlists=output-playlists
aws.mr.log.dir=log

# ------------ Spark: K Means Parallel K -------------------
job.spark.parallelk.name=scalaCode.KMeansParallelK

# local arguments
local.spark.parallelk.input=output_songs_sample_4
local.spark.parallelk.output=local-output-spark-K
local.spark.parallelk.wSoS=local-output-wSoS
local.spark.parallelk.klist=100,300,500,700
local.spark.parallelk.convergedist=0.5
local.spark.parallelk.maxiters=10
local.spark.parallelk.seed=123
local.spark.parallelk.algorithm=kmeans
local.spark.parallelk.master=local[4]
local.spark.parallelk.log=log-spark

# pseudo-cluster arguments
hdfs.spark.parallelk.input=output_songs_sample
hdfs.spark.parallelk.output=output-spark-K
hdfs.spark.parallelk.wSoS=output-wSoS
hdfs.spark.parallelk.klist=3,5
hdfs.spark.parallelk.convergedist=1
hdfs.spark.parallelk.maxiters=1
hdfs.spark.parallelk.seed=123
hdfs.spark.parallelk.algorithm=kmeans

# AWS EMR arguments
aws.spark.parallelk.input=output_songs_sample
aws.spark.parallelk.output=output-spark-K
aws.spark.parallelk.wSoS=output-wSoS
aws.spark.parallelk.klist=100,200,300,400,500,600
aws.spark.parallelk.convergedist=1
aws.spark.parallelk.maxiters=10
aws.spark.parallelk.seed=123
aws.spark.parallelk.algorithm=kmeans
aws.spark.parallelk.log.dir=log

# ------------ Spark: K Means Multi K -------------------
job.spark.multik.name=scalaCode.KMeansMultiK

# Local Arguments
local.spark.multik.input=multik_input
local.spark.multik.setup=multik_setup
local.spark.multik.output=multik_output
local.spark.multik.algo=kmeans
local.spark.multik.balance=true
local.spark.multik.machines=3
local.spark.multik.nameCenters=false
local.spark.multik.log=log-spark

# Pseudo-cluster Arguments
hdfs.spark.multik.input=multik_input
hdfs.spark.multik.setup=multik_setup
hdfs.spark.multik.output=multik_output
hdfs.spark.multik.algo=kmeans
hdfs.spark.multik.balance=true
hdfs.spark.multik.machines=3
hdfs.spark.multik.nameCenters=false
hdfs.spark.multik.log=log-spark

# AWS EMR Arguments
aws.spark.multik.input=multik_input
aws.spark.multik.setup=multik_setup
aws.spark.multik.output=multik_output
aws.spark.multik.algo=kmeans
aws.spark.multik.balance=true
aws.spark.multik.machines=3
aws.spark.multik.nameCenters=false
aws.spark.multik.log.dir=log

# ------------ Python: find Best Clustering -------------------
python.version = 3.8

# Local Arguments
local.python.script = src/main/pythonCode/findBestClustering.py 
	# must include full path
local.python.input = local-output-wSoS
local.python.output = local_graph_output
local.python.algo = multi

# Only valid for local run, not pseudo or aws 

# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output-mr:
	rm -rf ${local.mr.output.songs}*
	rm -rf ${local.mr.output.playlists}*
	rm -rf ${local.mr.intermediate}*
clean-local-output-spark-parallelk:
	rm -rf ${local.spark.parallelk.output}*
	rm -rf ${local.spark.parallelk.wSoS}*
clean-local-output-spark-multik:
	rm -rf ${local.spark.multik.output}*

# Removes local log directory
clean-local-log-mr:
	rm -rf ${local.mr.log}*	
clean-local-log-spark-parallelk:
	rm -rf ${local.spark.parallelk.log}*
clean-local-log-spark-multik:
	rm -rf ${local.spark.multik.log}*

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
local-mr: jar clean-local-output-mr
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.mr.name} ${local.mr.input} ${local.mr.intermediate} ${local.mr.output.playlists} ${local.mr.output.songs} ${clientid} ${clientsecret}
local-spark-parallelk: jar clean-local-output-spark-parallelk
	spark-submit --class ${job.spark.parallelk.name} --master ${local.spark.parallelk.master} --name "${app.name}" ${jar.path} ${local.spark.parallelk.input} ${local.spark.parallelk.output} ${local.spark.parallelk.wSoS} ${local.spark.parallelk.klist} ${local.spark.parallelk.convergedist} ${local.spark.parallelk.maxiters} ${local.spark.parallelk.seed} ${local.spark.parallelk.algorithm}
local-spark-multik: jar clean-local-output-spark-multik
	spark-submit --class ${job.spark.multik.name} --master local[4] --name "${app.name}" ${jar.path} ${local.spark.multik.input} ${local.spark.multik.setup} ${local.spark.multik.output} ${local.spark.multik.algo} ${local.spark.multik.balance} ${local.spark.multik.machines} ${local.spark.multik.nameCenters}
local-python-plot: 
	/usr/bin/python${python.version} ${local.python.script} ${local.python.input} ${local.python.output} ${local.python.algo}
	
# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${hadoop.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs-mr: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.mr.input}
init-hdfs-spark-parallelk: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.spark.parallelk.input}
init-hdfs-spark-multik: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.spark.multik.input}
		
# Load data to HDFS
upload-input-hdfs-mr: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.mr.input}/* /user/${hdfs.user.name}/${hdfs.mr.input}
upload-input-hdfs-spark-parallelk: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.spark.parallelk.input}/* /user/${hdfs.user.name}/${hdfs.spark.parallelk.input}
upload-input-hdfs-spark-multik: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.spark.multik.input}/* /user/${hdfs.user.name}/${hdfs.spark.multik.input}
	${hadoop.root}/bin/hdfs dfs -put ${local.spark.multik.setup}/* /user/${hdfs.user.name}/${hdfs.spark.multik.setup}
	
# Removes hdfs output and intermediate directory.
clean-hdfs-output-mr:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.mr.output}*
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.mr.intermediate}*
clean-hdfs-output-spark-parallelk:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.spark.parallelk.output}*
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.spark.parallelk.wSoS}*
clean-hdfs-output-spark-multik:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.spark.multik.output}*
	
	
# Download output from HDFS to local.
download-output-hdfs-mr: clean-local-output-mr
	mkdir ${local.mr.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.mr.output}/* ${local.mr.output.songs}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.mr.output}/* ${local.mr.output.playlists}
download-output-hdfs-spark-parallelk: clean-local-output-spark-parallelk
	mkdir ${local.spark.parallelk.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.spark.parallelk.output}/* ${local.spark.parallelk.output}
	mkdir ${local.spark.parallelk.wSoS}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.spark.parallelk.wSoS}/* ${local.spark.parallelk.wSoS}
download-output-hdfs-spark-multik: clean-local-output-spark-multik
	mkdir ${local.spark.multik.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.spark.multik.output}/* ${local.spark.multik.output}	
	
# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo-mr: jar stop-yarn format-hdfs init-hdfs-mr upload-input-hdfs-mr start-yarn clean-local-output-mr 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.mr.name} ${hdfs.mr.input} ${hdfs.mr.intermediate} ${hdfs.mr.output.playlists} ${hdfs.mr.output.songs} ${clientid} ${clientsecret}
	make download-output-hdfs-mr
pseudo-spark-parallelk: jar stop-yarn format-hdfs init-hdfs-spark-parallelk upload-input-hdfs-spark-parallelk start-yarn clean-local-output-spark-parallelk
	spark-submit --class ${job.spark.parallelk.name} --master yarn --deploy-mode cluster ${jar.name} ${hdfs.spark.parallelk.input} ${hdfs.spark.parallelk.output} ${hdfs.spark.parallelk.wSoS} ${hdfs.spark.parallelk.klist} ${hdfs.spark.parallelk.convergedist} ${hdfs.spark.parallelk.maxiters} ${hdfs.spark.parallelk.seed} ${hdfs.spark.parallelk.algorithm}
	make download-output-hdfs-spark-parallelk
pseudo-spark-multik: jar stop-yarn format-hdfs init-hdfs-spark-multik upload-input-hdfs-spark-multik start-yarn clean-local-output-spark-multik
	spark-submit --class ${job.spark.multik.name} --master yarn --deploy-mode cluster ${jar.name} ${hdfs.spark.multik.input} ${hdfs.spark.multik.setup} ${hdfs.spark.multik.output} ${hdfs.spark.multik.algo} ${hdfs.spark.multik.balance} ${hdfs.spark.multik.machines} ${hdfs.spark.multik.nameCenters}
	make download-output-hdfs-spark-multik	
	
# Runs pseudo-clustered (quickie).
pseudoq-mr: jar clean-local-output-mr clean-hdfs-output-mr
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.mr.name} ${hdfs.mr.input} ${hdfs.mr.intermediate} ${hdfs.mr.output.playlists} ${hdfs.mr.output.songs} ${clientid} ${clientsecret}
	make download-output-hdfs-mr
pseudoq-spark-parallelk: jar clean-local-output-spark-parallelk clean-hdfs-output-spark-parallelk
	spark-submit --class ${job.spark.parallelk.name} --master yarn --deploy-mode cluster ${jar.name} ${hdfs.spark.parallelk.input} ${hdfs.spark.parallelk.output} ${hdfs.spark.parallelk.wSoS} ${hdfs.spark.parallelk.klist} ${hdfs.spark.parallelk.convergedist} ${hdfs.spark.parallelk.maxiters} ${hdfs.spark.parallelk.seed} ${hdfs.spark.parallelk.algorithm}
	make download-output-hdfs-spark-parallelk
pseudoq-spark-multik: jar clean-local-output-spark-multik clean-hdfs-output-spark-multik
	spark-submit --class ${job.spark.multik.name} --master yarn --deploy-mode cluster ${jar.name} ${hdfs.spark.multik.input} ${hdfs.spark.multik.setup} ${hdfs.spark.multik.output} ${hdfs.spark.multik.algo} ${hdfs.spark.multik.balance} ${hdfs.spark.multik.machines} ${hdfs.spark.multik.nameCenters}
	make download-output-hdfs-spark-multik		
	     
# Create S3 bucket.
make-bucket-mr:
	aws s3 mb s3://${aws.bucket.name.mr}
make-bucket-spark:
	aws s3 mb s3://${aws.bucket.name.spark}

# Upload data to S3 input dir.
upload-input-aws-mr: 
	aws s3 sync ${local.mr.input} s3://${aws.bucket.name.mr}/${aws.mr.input}
upload-input-aws-spark-parallelk: 
	aws s3 sync ${local.spark.parallelk.input} s3://${aws.bucket.name.spark}/${aws.spark.parallelk.input}
upload-input-aws-spark-multik: 
	aws s3 sync ${local.spark.multik.input} s3://${aws.bucket.name.spark}/${aws.spark.multik.input}
	aws s3 sync ${local.spark.multik.setup} s3://${aws.bucket.name.spark}/${aws.spark.multik.setup}
		
# Delete S3 output dir.
delete-output-aws-mr:
	aws s3 rm s3://${aws.bucket.name.mr}/ --recursive --exclude "*" --include "${aws.mr.output}*"
delete-output-aws-spark-parallelk:
	aws s3 rm s3://${aws.bucket.name.spark}/ --recursive --exclude "*" --include "${aws.spark.parallelk.output}*"
	aws s3 rm s3://${aws.bucket.name.spark}/ --recursive --exclude "*" --include "${aws.spark.parallelk.wSoS}*"
delete-output-aws-spark-multik:
	aws s3 rm s3://${aws.bucket.name.spark}/ --recursive --exclude "*" --include "${aws.spark.multik.output}*"
			
# Upload application to S3 bucket.
upload-app-aws-mr:
	aws s3 cp ${jar.path} s3://${aws.bucket.name.mr}/
upload-app-aws-spark:
	aws s3 cp ${jar.path} s3://${aws.bucket.name.spark}/
	
# Main EMR launch.
aws-mr: jar upload-app-aws-mr delete-output-aws-mr
	aws emr create-cluster \
		--name "Spotify API Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.mr.name}","s3://${aws.bucket.name.mr}/${aws.mr.input}","s3://${aws.bucket.name.mr}/${aws.mr.intermediate}","s3://${aws.bucket.name.mr}/${aws.mr.output.playlists}","s3://${aws.bucket.name.mr}/${aws.mr.output.songs}","${clientid}","${clientsecret}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name.mr}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name.mr}/${aws.mr.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate
aws-spark-parallelk: jar upload-app-aws-spark delete-output-aws-spark-parallelk
	aws emr create-cluster \
		--name "KMeans ParallelK Spark Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.spark.parallelk.name}","s3://${aws.bucket.name.spark}/${jar.name}","s3://${aws.bucket.name.spark}/${aws.spark.parallelk.input}","s3://${aws.bucket.name.spark}/${aws.spark.parallelk.output}","s3://${aws.bucket.name.spark}/${aws.spark.parallelk.wSoS}","${aws.spark.parallelk.klist}","${aws.spark.parallelk.convergedist}","${aws.spark.parallelk.maxiters}","${aws.spark.parallelk.seed}","${aws.spark.parallelk.algorithm}"] \
		--log-uri s3://${aws.bucket.name.spark}/${aws.spark.parallelk.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate	
aws-spark-multik: jar upload-app-aws-spark delete-output-aws-spark-multik
	aws emr create-cluster \
		--name "KMeans MultiK Spark Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.spark.multik.name}","s3://${aws.bucket.name.spark}/${jar.name}","s3://${aws.bucket.name.spark}/${aws.spark.multik.input}","s3://${aws.bucket.name.spark}/${aws.spark.multik.setup}","s3://${aws.bucket.name.spark}/${aws.spark.multik.output}","${aws.spark.multik.algo}","${aws.spark.multik.balance}","${aws.spark.multik.machines}","${aws.spark.multik.nameCenters}"] \
		--log-uri s3://${aws.bucket.name.spark}/${aws.spark.multik.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate
				
# Download output from S3.
download-output-aws-mr: clean-local-output-mr clean-local-log-mr
	mkdir ${local.mr.log}
	aws s3 sync s3://${aws.bucket.name.mr}/${aws.mr.log.dir} ${local.mr.log}
	mkdir ${local.mr.output.songs}
	aws s3 sync s3://${aws.bucket.name.mr}/${aws.mr.output.songs} ${local.mr.output.songs}
	mkdir ${local.mr.output.playlists}
	aws s3 sync s3://${aws.bucket.name.mr}/${aws.mr.output.playlists} ${local.mr.output.playlists}
download-output-aws-spark-parallelk: clean-local-output-spark-parallelk clean-local-log-spark-parallelk
	mkdir ${local.spark.parallelk.log}
	aws s3 sync s3://${aws.bucket.name.spark}/${aws.spark.parallelk.log.dir} ${local.spark.parallelk.log}
	mkdir ${local.spark.parallelk.output}${aws.spark.parallelk.klist}
	aws s3 sync s3://${aws.bucket.name.spark}/${aws.spark.parallelk.output}${aws.spark.parallelk.klist} ${local.spark.parallelk.output}${aws.spark.parallelk.klist}
	mkdir ${local.spark.parallelk.wSoS}
	aws s3 sync s3://${aws.bucket.name.spark}/${aws.spark.parallelk.wSoS} ${local.spark.parallelk.wSoS}
download-output-aws-spark-multik: clean-local-output-spark-multik clean-local-log-spark-multik
	mkdir ${local.spark.multik.log}
	aws s3 sync s3://${aws.bucket.name.spark}/${aws.spark.multik.log.dir} ${local.spark.multik.log}
	mkdir ${local.spark.multik.output}
	aws s3 sync s3://${aws.bucket.name.spark}/${aws.spark.multik.output} ${local.spark.multik.output}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop
