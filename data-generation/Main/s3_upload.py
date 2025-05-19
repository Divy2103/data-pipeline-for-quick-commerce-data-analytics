
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
day = 28
month = 4
year = today.year

print(day, month, year)

folder_name = 'data5'
file_names = ['customer_address','customer','delivery_agent','delivery','location','login_audit','menu_items','order_items','orders','restaurant']
# file_names = ['delivery_agent']

bucket_name = f'{my_secrets['AWS_S3_BUCKET_NAME']}'

for files in file_names:
    if files in ['delivery_agent']:
        file_name = f'{folder_name}/{files}.json'
        object_name = f'{my_secrets['AWS_S3_BUCKET_FOLDER_NAME']}/{year}/{month}/{day}/{files}.json'
    else:
        file_name = f'{folder_name}/{files}.csv'
        object_name = f'{my_secrets['AWS_S3_BUCKET_FOLDER_NAME']}/{year}/{month}/{day}/{files}.csv'
    
    try:
        s3.upload_file(file_name, bucket_name, object_name)
        print(f'{files} uploaded successfully')
    except Exception as e:
        print(f'{files} failed due to : ',e)