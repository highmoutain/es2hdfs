export HADOOP_CONF_DIR=/home/hadoop/hadoop-2.7.2/etc/hadoop/
#export SPARK_MASTER_IP=172.17.1.79
#export SPARK_MASTER_PORT=7077
export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080 -Dspark.history.retainedApplications=50 -Dspark.history.fs.logDirectory=hdfs:///sparkhistoryserver"
export SPARK_PID_DIR=/home/hadoop/spark-2.2.1-bin-hadoop2.7
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_SCALA_VERSION=2.11
