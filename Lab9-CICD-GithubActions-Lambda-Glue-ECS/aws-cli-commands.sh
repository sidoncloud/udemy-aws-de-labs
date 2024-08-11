# Zip the python script for lambda 
zip function.zip lambda_function.py

# Deploy Lambda function
aws lambda create-function --function-name test-lambda \
--zip-file fileb://function.zip \
--handler lambda_function.lambda_handler \
--runtime python3.11 \
--role arn:aws:iam::127489365181:role/lambda-kinesis-streams-consumer-role

# AWS Glue deployment

# Copy python script to S3
aws s3 cp mysql-extraction.py s3://nl-aws-de-labs/glue-scripts/

# Deploy glue job using the uploaded script from S3 
aws glue create-job --name "mysql-extraction-job" --role "arn:aws:iam::127489365181:role/custom-glue-role" \
--command '{"Name":"deploy-mysql-extraction", "ScriptLocation":"s3://nl-aws-de-labs/glue-scripts/mysql-extraction.py", "PythonVersion":"3"}' --glue-version "4.0" --max-capacity 1
