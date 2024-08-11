# Docker Authentication with ECR
aws ecr get-login-password \
        --region us-east-1 | docker login \
        --username AWS \
        --password-stdin {aws-account-number}.dkr.ecr.us-east-1.amazonaws.com

# Commands for data-validation task  
docker build -t ecom_data_validation .
docker run -d -v ~/.aws:/root/.aws ecom_data_validation
docker tag ecom_data_validation:latest {aws-account-number}.dkr.ecr.us-east-1.amazonaws.com/ecommerce-pipelines:ecom_data_validation
docker push {aws-account-number}.dkr.ecr.us-east-1.amazonaws.com/ecommerce-pipelines:ecom_data_validation

# Newly tagged image 
docker tag ecom_data_validation:latest {aws-account-number}.dkr.ecr.us-east-1.amazonaws.com/ecommerce-pipelines:ecom_data_validation_v1
docker push {aws-account-number}.dkr.ecr.us-east-1.amazonaws.com/ecommerce-pipelines:ecom_data_validation_v1


# Commands for ETL Job
docker build -t etl_aggregations . 
docker run -d -v ~/.aws:/root/.aws etl_aggregations
docker tag etl_aggregations:latest {aws-account-number}.dkr.ecr.us-east-1.amazonaws.com/ecommerce-pipelines:etl_aggregations
docker push {aws-account-number}.dkr.ecr.us-east-1.amazonaws.com/ecommerce-pipelines:etl_aggregations


# Get task definition once created 
aws ecs describe-task-definition \
                --task-definition redshift-ingestion \
                --query taskDefinition "Angle Bracket" task-definition.json


aws ecs describe-task-definition --task-definition redshift-ingestion