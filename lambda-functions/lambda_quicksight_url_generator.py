import os, boto3, json

qs = boto3.client('quicksight')
dashboard_id = os.environ.get('QS_DASHBOARD_ID')

def lambda_handler(event, context):
    account_id = event.get('requestContext').get('accountId')
    try:
        response = qs.get_dashboard_embed_url(AwsAccountId=account_id, DashboardId=dashboard_id, IdentityType="IAM")
        if response:
            return {
                'statusCode': 200,
                'headers': {"Access-Control-Allow-Origin": "*", 'Access-Control-Allow-Methods': 'GET, OPTIONS'},
                'body': json.dumps(response)
            }
    except Exception as e: print(e)
