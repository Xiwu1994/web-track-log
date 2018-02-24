#!/bin/bash
jar_path=./target/web-track-log-1.0.jar

spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 1g \
--executor-memory 2g \
--conf "spark.driver.extraJavaOptions=-XX:+PrintHeapAtGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
--class com.etl.webTrackLogEtl \
--files /home/hadoop/spark-2.1.0-bin-hadoop2.7/conf/hive-site.xml ${jar_path}
