import json
import boto3
import urllib.parse
from PIL import Image
import io
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    
    if 'Records' not in event or not event['Records']:
        print("Error: No Records found in event")
        return {
            'statusCode': 400,
            'body': 'Error: No Records found in event'
        }
    
    try:
        source_bucket = 'source-image-bucket-aq'
        destination_bucket = 'resized-image-bucket-aq'
        
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        print(f"Processing key: {key}")

        # Download image from S3
        response = s3.get_object(Bucket=source_bucket, Key=key)
        image_data = response['Body'].read()
        content_type = response['ContentType']
        print(f"Downloaded image, size: {len(image_data)} bytes, ContentType: {content_type}")

        # Open image using PIL
        image = Image.open(io.BytesIO(image_data))
        print(f"Image format: {image.format}, mode: {image.mode}, size: {image.size}")

        if image.format not in ['JPEG', 'PNG']:
            print(f"Unsupported image format: {image.format}")
            return {
                'statusCode': 400,
                'body': f'Unsupported image format: {image.format}'
            }

        # Prepare buffer
        buffer = io.BytesIO()

        # Handle JPEG
        if image.format == 'JPEG':
            if image.mode != 'RGB':
                image = image.convert('RGB')
                print("Converted image to RGB for JPEG")

            # Compress JPEG iteratively to target ~120 KB
            quality = 85
            target_size = 122880  # 120 KB
            image.save(buffer, format='JPEG', quality=quality, optimize=True)
            buffer_size = buffer.getbuffer().nbytes
            print(f"Initial JPEG size: {buffer_size} bytes at quality {quality}")

            while buffer_size > target_size and quality > 5:
                buffer.seek(0)
                buffer.truncate()
                quality -= 5
                image.save(buffer, format='JPEG', quality=quality, optimize=True)
                buffer_size = buffer.getbuffer().nbytes
                print(f"Adjusted JPEG to quality {quality}, size: {buffer_size} bytes")

            content_type = 'image/jpeg'
            extension = '.jpg'

        # Handle PNG
        elif image.format == 'PNG':
            if image.mode not in ['RGB', 'RGBA']:
                image = image.convert('RGBA')
                print("Converted image to RGBA for PNG")

            image.save(buffer, format='PNG', optimize=True)
            buffer_size = buffer.getbuffer().nbytes
            print(f"PNG compressed size: {buffer_size} bytes")
            
            content_type = 'image/png'
            extension = '.png'

        buffer.seek(0)

        # Prepare destination key
        basename = os.path.splitext(os.path.basename(key))[0]
        destination_key = f"compressed-{basename}{extension}"

        # Upload to destination bucket
        print(f"Uploading to {destination_bucket}/{destination_key}")
        s3.put_object(
            Bucket=destination_bucket,
            Key=destination_key,
            Body=buffer,
            ContentType=content_type,
            ServerSideEncryption='AES256'
        )
        print(f"Uploaded to {destination_bucket}/{destination_key}")

        return {
            'statusCode': 200,
            'body': f'Compressed {key} to approx {buffer_size // 1024} KB and saved to {destination_bucket}/{destination_key}'
        }

    except Exception as e:
        print(f"Error processing {key}: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': f'Error processing {key}: {str(e)}'
        }