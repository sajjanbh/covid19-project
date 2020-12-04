import datetime, boto3, json
import argparse
from boto3.dynamodb.conditions import Key

# This script is used a tool to manage the ETL jobs, such as for backfilling past data or resubmit failed jobs.]
# Usage to submit past dates: python3 manage-etl-jobs.py run-job --start-date <date> --end-date <date>
# Usage to resubmit any failed jobs: python3 manage-etl-jobs.py resubmit-failed

def invoke_lambda(l_client, d):
	year = str(d.year)
	month = "{0:0=2d}".format(d.month)
	day = "{0:0=2d}".format(d.day)
	print("{}-{}-{}".format(year, month, day))
	payload = {"year": year, "month": month, "day": day}
	payload = json.dumps(payload)
	payload = payload.encode()
	response = l_client.invoke(FunctionName="ETL-Scheduler", InvocationType="Event", Payload=payload)

def run_job_func(args):
	print("Running job")
	end_date = datetime.datetime.strptime(args.end_date, "%Y-%m-%d")
	start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d")
	delta = end_date - start_date
	if args.profile:
		boto3.setup_default_session(profile_name=args.profile)

	if args.region:
		l_client = boto3.client("lambda", region_name=args.region)
	else:
		l_client = boto3.client("lambda")

	print("Invoking Lambda function:")
	for i in range(delta.days + 1):
		d = start_date + datetime.timedelta(days=i)
		invoke_lambda(l_client, d)

def resubmit_failed_func(args):
	if args.profile:
		boto3.setup_default_session(profile_name=args.profile)

	if args.region:
		l_client = boto3.client("lambda", region_name=args.region)
	else:
		l_client = boto3.client("lambda")

	if args.region:
		ddb = boto3.resource("dynamodb", region_name=args.region)
	else:
		ddb = boto3.resource("dynamodb")

	table = ddb.Table('Covid-Project')

	# Get items from DDB which are stuck in RUNNING status.
	# It'll invoke Lambda function with such dates, then Lambda function will only submit those jobs which have their corresponding EMR Steps as FAILED.
	uncompleted_jobs = table.query(IndexName='status-date-index', KeyConditionExpression=Key('status').eq('RUNNING'))
	for item in uncompleted_jobs['Items']:
		d = datetime.datetime.strptime(item.get('date'), "%Y-%m-%d")
		invoke_lambda(l_client, d)

parser = argparse.ArgumentParser()
parser.add_argument("--profile", type=str, help="AWS profile name. If not specified, default will be used.")
parser.add_argument("--region", type=str, help="AWS Region where Lambda function is located.")

subparsers = parser.add_subparsers(title='subcommands',
                                   description='valid subcommands: run-job, resubmit-failed',
                                   help='additional help')
run_parser = subparsers.add_parser("run-job", help="run-job --start-date <date> --end-date <date>")
run_parser.add_argument("--start-date", type=str, required=True, help="Start date for ETL. Eg. 2020-01-01")
run_parser.add_argument("--end-date", type=str, required=True, help="Start date for ETL. Eg. 2020-02-01")

resubmit_parser = subparsers.add_parser("resubmit-failed")
run_parser.set_defaults(func=run_job_func)
resubmit_parser.set_defaults(func=resubmit_failed_func)

args = parser.parse_args()
args.func(args)
