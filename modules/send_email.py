import boto3

def send_email(err_content):
    ses = boto3.client('ses')
    try:
        response = ses.send_email(
            Destination={"ToAddresses":["sknaresh2000@gmail.com"]},
            Message={
                "Body": {
                    "Text": {"Data":err_content}
                },
                "Subject":{"Data":"ERROR : COVID Data Process"}
            },
            Source="sknaresh2000@gmail.com"
        )
    except ClientError as e:
        print(e.response["Error"]["Message"])
