import json
import base64
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Helper function to determine if the browser indicates a mobile device
def is_mobile_browser(browser):
    # List of mobile indicators based on typical mobile browser strings
    mobile_indicators = ['Mobile', 'iPhone', 'Android', 'iPad']
    return any(indicator in browser for indicator in mobile_indicators)

# Helper function to categorize traffic sources
def categorize_traffic_source(source):
    if 'Google' in source or 'Bing' in source:
        return 'Search Engine'
    elif 'Facebook' in source or 'YouTube' in source:
        return 'Social Media'
    elif 'Email' in source:
        return 'Email'
    else:
        return 'Direct'

def lambda_handler(event, context):
    output = []
    
    # Process each record in the event batch
    for record in event['records']:
        try:
            # Decode the incoming data from base64 and convert from JSON
            payload = json.loads(base64.b64decode(record['data']))
            
            # Extract fields to be included in the CSV
            id = payload.get('id', '')
            user_id = payload.get('user_id', '')
            session_id = payload.get('session_id', '')
            ip_address = payload.get('ip_address', '')
            city = payload.get('city', '')
            state = payload.get('state', '')
            browser = payload.get('browser', '')
            traffic_source = payload.get('traffic_source', '')
            uri = payload.get('uri', '')
            event_type = payload.get('event_type', '')

            # Derive is_mobile and traffic_source_category
            is_mobile = 'Yes' if is_mobile_browser(browser) else 'No'
            traffic_source_category = categorize_traffic_source(traffic_source)

            # Quote each field to ensure proper CSV formatting
            data = f'"{id}","{user_id}","{session_id}","{ip_address}","{city}","{state}","{browser}","{is_mobile}","{traffic_source}","{traffic_source_category}","{uri}","{event_type}"\n'

            # Encode the CSV string to base64
            encoded_csv = base64.b64encode(data.encode('utf-8')).decode('utf-8')
            
            # Prepare the output record with unique recordId and transformed data
            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': encoded_csv
            }
            output.append(output_record)
            
            logger.info(f"Processed record ID: {record['recordId']}")

        except Exception as e:
            logger.error(f"Error processing record {record['recordId']}: {str(e)}")
            # Return the original record in case of failure
            output_record = {
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data']  # Return original data
            }
            output.append(output_record)

    # Return the transformed records to Firehose
    logger.info(f"Total records processed: {len(output)}")
    return {'records': output}