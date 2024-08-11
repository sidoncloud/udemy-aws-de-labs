
WORKSPACE_LOCATION=$(pwd)
SCRIPT_FILE_NAME=spark-transactional-delta-lake.py

docker build -t spark-deltalake-job .

docker run -it \
-v ~/.aws:/home/glue_user/.aws \
-v "${WORKSPACE_LOCATION}:/home/glue_user/workspace/" \
-v "$(pwd)/jars:/home/glue_user/jars" \
-e AWS_PROFILE=default \
-e DISABLE_SSL=true \
--rm -p 4040:4040 -p 18080:18080 \
--name glue_spark_submit \
spark-deltalake-job \
spark-submit --jars /home/glue_user/jars/delta-core_2.12-1.0.0.jar \
/home/glue_user/workspace/"${SCRIPT_FILE_NAME}"