export SPARK_LOCAL_IP=147.9.113.24
spark-submit --class com.sparkghsom.main.input_generator.IrisDatasetReader \
    --master spark://147.9.113.24:7077 \
    --conf "spark.driver.host=localhost" \
    /Users/hydergine/git/mr_ghsom/target/mr_ghsom-0.0.1-jar-with-dependencies.jar $1

