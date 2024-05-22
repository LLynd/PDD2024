export user=hadoop
export home=/home/$user
export hdfs_dir="$home/hdfsdata"
export cluster_dir="$home/cluster"
export hadoop_name=hadoop-3.3.6
export hadoop_name_gz=$hadoop_name.tar.gz
export spark_name=spark-3.5.1-bin-hadoop3
export spark_url_dir=spark-3.5.1
export spark_name_gz=$spark_name.tgz

export workers="localhost"
export master="localhost"
export all_nodes="localhost"

export JAVA_HOME=$home/cluster/jdk1.8.0_181
export HADOOP_HOME=$home/cluster/$hadoop_name
export SPARK_HOME=$home/cluster/$spark_name
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin


