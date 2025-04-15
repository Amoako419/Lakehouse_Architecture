import boto3
import json
import os
import time

def lambda_handler(event, context):
    """
    Lambda function to move objects from one S3 location to another with batch processing
    """
    # Extract parameters from Step Functions input
    source_bucket = event['sourceBucket']
    source_prefix = event['sourcePrefix']
    destination_bucket = event['destinationBucket']
    destination_prefix = event['destinationPrefix']
    
    # Remove trailing slashes for consistency
    source_prefix = source_prefix.rstrip('/')
    destination_prefix = destination_prefix.rstrip('/')
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    # Set up batch processing
    max_keys = 100  # Process objects in batches of 100
    continuation_token = None
    
    # Initialize counters
    success_count = 0
    failure_count = 0
    total_objects = 0
    
    start_time = time.time()
    # Keep track of remaining time to avoid timeouts
    timeout_buffer = 10  # seconds
    
    # Process objects in batches
    while True:
        # Check if we're approaching timeout
        elapsed_time = time.time() - start_time
        remaining_time = context.get_remaining_time_in_millis() / 1000  # Convert to seconds
        
        if remaining_time < timeout_buffer:
            # Return partial results and continuation token for Step Functions to retry
            return {
                'statusCode': 202,  # Accepted but incomplete
                'message': f'Timeout approaching - processed {success_count} objects so far',
                'continuationToken': continuation_token,
                'isComplete': False,
                'partialCounts': {
                    'success': success_count,
                    'failure': failure_count,
                    'total': total_objects
                }
            }
        
        # List objects in the current batch
        list_args = {
            'Bucket': source_bucket,
            'Prefix': source_prefix,
            'MaxKeys': max_keys
        }
        
        if continuation_token:
            list_args['ContinuationToken'] = continuation_token
            
        response = s3.list_objects_v2(**list_args)
        
        # Process this batch
        if 'Contents' in response:
            batch_objects = response['Contents']
            total_objects += len(batch_objects)
            
            for obj in batch_objects:
                source_key = obj['Key']
                
                # Skip directory markers (objects with trailing slash)
                if source_key.endswith('/'):
                    continue
                    
                # Calculate destination key by replacing the source prefix with destination prefix
                relative_path = source_key[len(source_prefix):].lstrip('/')
                destination_key = f"{destination_prefix}/{relative_path}"
                
                try:
                    # Copy the object to the new location
                    copy_source = {'Bucket': source_bucket, 'Key': source_key}
                    s3.copy_object(
                        CopySource=copy_source,
                        Bucket=destination_bucket,
                        Key=destination_key
                    )
                    
                    # Delete the original object
                    s3.delete_object(
                        Bucket=source_bucket,
                        Key=source_key
                    )
                    
                    success_count += 1
                except Exception as e:
                    print(f"Error moving object {source_key}: {str(e)}")
                    failure_count += 1
        
        # Check if there are more objects to process
        if not response.get('IsTruncated', False):
            break
            
        continuation_token = response.get('NextContinuationToken')
    
    # All objects processed successfully
    result = {
        'statusCode': 200 if failure_count == 0 else 500,
        'message': f'Successfully moved {success_count} objects, failed to move {failure_count} objects',
        'isComplete': True,
        'totalObjectsProcessed': total_objects
    }
    
    # If any failures occurred, pass the error information to Step Functions
    if failure_count > 0:
        result['error'] = True
        result['cause'] = f'Failed to move {failure_count} objects'
    
    return result