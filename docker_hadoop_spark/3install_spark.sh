. ./settings.sh

echo "start of 3install_spark.sh"

#Spark runs on Java 8/11, Scala 2.12, Python 3.6+ and R 3.5+.
#Java 8 prior to version 8u92 support is deprecated as of Spark 3.0.0.
#For the Scala API, Spark 3.1.1 uses Scala 2.12. You will need to use a compatible Scala version (2.12.x).

#For Python 3.9, Arrow optimization and pandas UDFs might not work due to the supported Python versions in Apache Arrow. Please refer to the latest Python Compatibility page.
#For Java 11, -Dio.netty.tryReflectionSetAccessible=true is required additionally for Apache Arrow library. This prevents java.lang.UnsupportedOperationException: sun.misc.Unsafe or java.nio.DirectByteBuffer.(long, int) not available when Apache Arrow uses Netty internally.

echo 
echo "***************************************************************************"
echo configuring Spark
echo "***************************************************************************"

cat <<EOF > $SPARK_HOME/conf/spark-env.sh
#!/usr/bin/env bash

export JAVA_HOME=${JAVA_HOME}
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
#export SCALA_HOME=$home/cluster/scala-2.12
export SPARK_HOME=${SPARK_HOME}
export SPARK_WORKER_CORES=1
export SPARK_WORKER_MEMORY=1g
EOF

cp ${HADOOP_HOME}/etc/hadoop/workers $SPARK_HOME/conf/slaves

echo "end of 3install_spark.sh" 
