
import boto3
from datetime import datetime
from dotenv import dotenv_values
my_secrets = dotenv_values(".env")

s3 = boto3.client(
     's3',
     aws_access_key_id=f'{my_secrets['AWS_ACCESS_KEY']}',
     aws_secret_access_key=f'{my_secrets['AWS_SECRET_KEY']}'
)
    
today = datetime.today()
day = today.day
month = today.month
year = today.year

folder_name = 'data2'
file_names = ['customer_address','customer']

bucket_name = f'{my_secrets['AWS_S3_BUCKET_NAME']}'

for files in file_names:
    file_name = f'{folder_name}/{files}.csv'
    object_name = f'{my_secrets['AWS_S3_BUCKET_FOLDER_NAME']}/{year}/{month}/{day}/{files}.csv'
    
    try:
        s3.upload_file(file_name, bucket_name, object_name)
        print(f'{files} uploaded successfully')
    except Exception as e:
        print(f'{files} failed due to : ',e)
