import json
import base64
from decimal import Decimal
import unittest
from unittest.mock import patch, MagicMock
from lambda_function import lambda_handler

class TestLambdaGlueFunction(unittest.TestCase):
    @patch('boto3.resource')
    @patch('boto3.client')
    def test_lambda_handler(self, mock_client, mock_resource):
        # Setup Mock for DynamoDB
        mock_dynamodb = MagicMock()
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_resource.return_value = mock_dynamodb

        # Setup Mock for Glue
        mock_glue = MagicMock()
        mock_client.return_value = mock_glue
        mock_glue.start_job_run.return_value = {'JobRunId': '123'}

        # Prepare test event data
        test_data = json.dumps({'trip_id': 'abc123', 'fare_amount': 100, 'tip_amount': 20, 'total_amount': 120})
        encoded_data = base64.b64encode(test_data.encode('utf-8'))
        event = {
            'Records': [
                {'kinesis': {'data': encoded_data}}
            ]
        }

        # Prepare response for get_item to simulate DynamoDB behavior
        mock_table.get_item.return_value = {'Item': {'trip_id': 'abc123'}}

        # Expected updates for expression attributes in DynamoDB update
        expression_attribute_values = {':fare_amount': Decimal('100'), ':tip_amount': Decimal('20'), ':total_amount': Decimal('120')}

        # Call the lambda handler
        response = lambda_handler(event, None)

        # Assertions to validate behavior
        mock_table.get_item.assert_called_once_with(Key={'trip_id': 'abc123'})
        mock_table.update_item.assert_called_once_with(
            Key={'trip_id': 'abc123'},
            UpdateExpression='SET fare_amount = :fare_amount, tip_amount = :tip_amount, total_amount = :total_amount',
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW"
        )
        mock_glue.start_job_run.assert_called_once_with(JobName='process_completed_trips')
        self.assertEqual(response['statusCode'], 200)
        self.assertIn('Updated 1 trips with 0 errors. Triggered Glue job 1 times.', response['body'])

if __name__ == '__main__':
    unittest.main()