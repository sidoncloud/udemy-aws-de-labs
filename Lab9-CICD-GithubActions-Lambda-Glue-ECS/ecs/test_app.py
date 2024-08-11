import pytest
import boto3
from moto import mock_aws
import app
import pandas as pd
from io import StringIO
from unittest.mock import patch
import os

# Setup mock S3 with mock_aws
@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture(scope="module")
def s3(aws_credentials):
    with mock_aws():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='nl-aws-de-labs')
        csv_content = "id,order_id,user_id,product_id,created_at\n1,100,200,300,2021-06-01T00:00:00Z"
        s3.put_object(Bucket='nl-aws-de-labs', Key='ecommerce-data/new/orders/test.csv', Body=csv_content)
        yield s3

# Fixture to set up the environment variable for tests
@pytest.fixture(autouse=True)
def set_env_vars():
    with patch.dict(os.environ, {'TASK_TOKEN': 'example_task_token'}):
        yield

def test_list_files(s3):
    files = app.list_files('nl-aws-de-labs', 'ecommerce-data/new/orders')
    assert files == ['ecommerce-data/new/orders/test.csv'], "File list is incorrect"

def test_read_from_s3(s3):
    df = app.read_from_s3('nl-aws-de-labs', 'ecommerce-data/new/orders/test.csv')
    assert not df.empty, "DataFrame should not be empty"
    assert 'created_at' in df.columns, "created_at column should be present and converted"

def test_check_format():
    df = pd.read_csv(StringIO("id,order_id,user_id,product_id,created_at\n1,100,200,300,2021-06-01"))
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
    column_formats = {
        'id': 'int64',
        'order_id': 'int64',
        'user_id': 'int64',
        'product_id': 'int64',
        'created_at': 'datetime64[ns, UTC]'  # Adjusted to include timezone information
    }
    result = app.check_format(df, column_formats, 'orders')
    assert result, "Format should be correct"


def test_main_function():
    with patch('boto3.client') as mock_client:  # Ensure this mocks 'boto3.client' directly
        app.main()
        mock_client.assert_called()  # Example assertion

if __name__ == "__main__":
    pytest.main()
