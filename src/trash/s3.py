import boto3

s3 = boto3.resource('s3')
bucket = s3.Bucket('kcd-de-production')

for object in bucket.objects.filter(Prefix="raw/", Delimiter="/"):
    print(object.key)