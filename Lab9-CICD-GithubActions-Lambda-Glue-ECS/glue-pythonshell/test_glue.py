import unittest
from unittest.mock import patch, MagicMock
import sys

# Mocking the awsglue module
sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()
sys.modules['awsglue.utils'].getResolvedOptions = MagicMock(return_value={
    "table_name": "test_table",
    "load_type": "test_load_type"
})

from mysql_extraction import main, convert_to_csv

class TestMysqlExtraction(unittest.TestCase):

    @patch('mysql_extraction.pymysql.connect')
    @patch('mysql_extraction.boto3.client')
    def test_main_function(self, mock_boto_client, mock_db_connect):
        # Setup mocks for database cursor and S3 client
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [{'id': 1, 'name': 'Test Apartment', 'price': 1200}]
        mock_db_connect.return_value = mock_connection

        # Mock S3 client
        mock_s3 = mock_boto_client.return_value
        mock_s3.put_object.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

        # Execute the main function
        main()

        # Check if database and S3 interactions are called correctly
        mock_cursor.execute.assert_called_once()
        mock_s3.put_object.assert_called_once()

    def test_convert_to_csv_valid_data(self):
        data = [{'id': 1, 'name': 'Test Apartment', 'price': 1200}]
        expected_output = 'id,name,price\r\n1,Test Apartment,1200\r\n'
        result = convert_to_csv(data)
        self.assertEqual(result, expected_output)

if __name__ == '__main__':
    unittest.main()