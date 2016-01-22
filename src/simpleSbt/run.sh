#build jar: sbt package

/opt/bigdata/spark/bin/spark-submit --master spark://analytics01:7077 --jars "/opt/bigdata/spark_extra_libs/mysql-connector-java-5.1.38-bin.jar" --driver-memory 1G --class "SimpleApp" /tmp/simple-project_2.11-1.0.jar
