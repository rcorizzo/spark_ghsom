export SPARK_LOCAL_IP=172.16.0.14
spark-submit --class com.sparkghsom.main.input_generator.UserKnowledgeData --master spark://192.168.101.13:7077 --conf "spark.driver.host=172.16.0.14" /home/ameya/git/mcs-thesis/ghsom/target/ghsom-0.0.1-jar-with-dependencies.jar







