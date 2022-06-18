# importing necessary modules
import requests
import os
import boto3

def lambda_handler(event, context):   
    #return None
    
    S3_BUCKET = os.environ.get('S3_BUCKET')    
    
    def upload_to_aws(local_file, bucket, s3_file):
        s3 = boto3.client('s3')
    
        try:
            s3.upload_file(local_file, bucket, s3_file)
            print("Upload Successful")
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False


    def run_fargate_task():
        client = boto3.client('ecs')
        print("Running task.")
        response = client.run_task(
            cluster='data-pipeline-cluster', 
            launchType='FARGATE',
            taskDefinition='FARGATE-profile-rds',  # <-- notice no revision number
            count=1,
            platformVersion='LATEST',
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': [
                        'subnet-02bfb160ef8e9d41d',
                        'subnet-0baff63def26a53c9'
                    ],
                    'securityGroups': [
                        'sg-07c4878be5b8de11a'
                    ],
                    'assignPublicIp': 'DISABLED'
                }
            })
        print("Finished invoking task.")
    
        return str(response)


    source_uris={
        'provincias':'https://infra.datos.gob.ar/catalog/modernizacion/dataset/7/distribution/7.7/download/provincias.csv',
        'departamentos':'https://infra.datos.gob.ar/catalog/modernizacion/dataset/7/distribution/7.8/download/departamentos.csv',
        'Covid19Casos':'https://pipeline-covid19.s3.amazonaws.com/Covid19Casos.zip'
    }

    for key, url in source_uris.items():
        print(f'Downloading {key} ...')

        # Downloading the file by sending the request to the URL
        req = requests.get(url)
    
        # Split URL to get the file name
        filename = url.split('/')[-1]
        filepath = '/tmp/' + filename


        # Writing the file to the local file system
        with open(filepath,'wb') as output_file:
            output_file.write(req.content)
            print('Downloading Completed')

        local_file = filepath
        s3_file = filename

        uploaded = upload_to_aws(local_file, S3_BUCKET, s3_file)
    
    run_fargate_task()
        