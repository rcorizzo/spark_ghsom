export SPARK_LOCAL_IP=localhost
spark-submit --class com.sparkghsom.main.input_generator.CensusDataset \
    --master localhost \
    --conf "spark.driver.host=localhost" \
    --conf "spark.executor.extraJavaOptions=-XX:+UseCompressedOops" \
    /Users/hydergine/git/mr_ghsom/target/mr_ghsom-0.0.1-jar-with-dependencies.jar $1

