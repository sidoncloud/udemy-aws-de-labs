import pytest
import json
import base64
from lambda_function import lambda_handler, is_mobile_browser, categorize_traffic_source

def test_is_mobile_browser():
    assert is_mobile_browser('Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1') == True
    assert is_mobile_browser('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3') == False

def test_categorize_traffic_source():
    assert categorize_traffic_source('Google') == 'Search Engine'
    assert categorize_traffic_source('Facebook') == 'Social Media'
    assert categorize_traffic_source('Email Newsletter') == 'Email'
    assert categorize_traffic_source('Direct Access') == 'Direct'

def test_lambda_handler():
    # Simulate an event with one record
    event = {
        'records': [
            {
                'recordId': '1',
                'data': base64.b64encode(json.dumps({
                    'id': '12345',
                    'user_id': '67890',
                    'session_id': 'abcd-1234',
                    'ip_address': '192.168.0.1',
                    'city': 'New York',
                    'state': 'NY',
                    'browser': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1',
                    'traffic_source': 'Google',
                    'uri': '/home',
                    'event_type': 'page_view'
                }).encode('utf-8')).decode('utf-8')
            }
        ]
    }

    result = lambda_handler(event, None)
    
    assert len(result['records']) == 1
    assert result['records'][0]['recordId'] == '1'
    assert result['records'][0]['result'] == 'Ok'
    
    decoded_data = base64.b64decode(result['records'][0]['data']).decode('utf-8')
    expected_data = '"12345","67890","abcd-1234","192.168.0.1","New York","NY","Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1","Yes","Google","Search Engine","/home","page_view"\n'
    assert decoded_data == expected_data

def test_lambda_handler_with_processing_error():
    # Simulate an event with one record that causes a JSON decoding error
    event = {
        'records': [
            {
                'recordId': '2',
                'data': base64.b64encode(b'invalid json data').decode('utf-8')
            }
        ]
    }

    result = lambda_handler(event, None)
    
    assert len(result['records']) == 1
    assert result['records'][0]['recordId'] == '2'
    assert result['records'][0]['result'] == 'ProcessingFailed'
