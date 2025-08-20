import boto3
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the bucket and object key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Get the filename only (without folder path if any)
    filename = os.path.basename(key)

    # Determine destination folder based on filename prefix
    prefix = filename[:3].lower()

    if prefix == "img":
        dest_folder = "images/"
    elif prefix == "doc":
        dest_folder = "documents/"
    elif prefix == "pdf":
        dest_folder = "pdf/"
    else:
        print(f"File {filename} does not match any prefix. Skipping.")
        return {
            'statusCode': 200,
            'body': f"Skipped file {filename}"
        }

    # Destination key
    dest_key = dest_folder + filename

    # Copy file to new location
    s3.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': key},
        Key=dest_key
    )

    # Delete original file
    s3.delete_object(Bucket=bucket, Key=key)

    print(f"Moved {filename} to {dest_folder}")

    return {
        'statusCode': 200,
        'body': f"Moved {filename} to {dest_folder}"
    }
