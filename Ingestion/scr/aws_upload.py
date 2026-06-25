# %% 

from config import access_key, secret_access_key
import boto3
import os
from botocore.exceptions import ClientError  # ← Importa isso!

def upload_parquets(local_file, path_s3):
     
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key
    )
    
    bucket = 'treinamento-tmw-raw-b'
    
    for dir, dirs, files in os.walk(local_file):
        for file in files:  
            if not file.endswith('.parquet'):
                continue
            
            local_file_path = os.path.join(dir, file)
            relative_path = os.path.relpath(local_file_path, local_file)
            s3_file_path = os.path.join(path_s3, relative_path).replace('\\', '/')
            
            print(f"{local_file_path} → s3://{bucket}/{s3_file_path}")
            
            # Right access error 
            try:
                s3.head_object(Bucket=bucket, Key=s3_file_path)
                print(" Already exists, upload not going to be made...")
                continue
            except ClientError as e:

            
                # 404 arquive does not exist or others
                if e.response['Error']['Code'] == '404':
                    pass
                else:
                    # Another error for permission 
                    print(f" Error checking file: {e}")
                    continue
                  
            try:
                s3.upload_file(local_file_path, bucket, s3_file_path)
                print(" Sent!")
            except Exception as e:
                print(f" Error uploading: {e}\n")
                
upload_parquets('../data/cdc', 'sistema-pontos/cdc')