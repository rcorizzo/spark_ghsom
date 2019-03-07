export SPARK_LOCAL_IP=172.16.0.18
spark-submit --class com.sparkghsom.main.input_generator.YeastDataset \
    --master spark://192.168.101.13:7077 \
    --conf "spark.driver.host=172.16.0.18" \
    /home/ameya/git/mr_ghsom/target/mr_ghsom-0.0.1-jar-with-dependencies.jar $1

