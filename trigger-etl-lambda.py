import datetime, boto3, json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--profile", type=str, help="AWS profile name. If not specified, default will be used.")
parser.add_argument("--start-date", type=str, required=True, help="Start date for ETL. Eg. 2020-01-01")
parser.add_argument("--end-date", type=str, required=True, help="Start date for ETL. Eg. 2020-02-01")
parser.add_argument("--region", type=str, help="AWS Region where Lambda function is located.")
args = parser.parse_args()

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
# for i in range(99):
	d = start_date + datetime.timedelta(days=i)
	# if d.strftime("%A") == "Wednesday":
	year = str(d.year)
	month = "{0:0=2d}".format(d.month)
	day = "{0:0=2d}".format(d.day)
	print("{}-{}-{}".format(year, month, day))
	payload = {"year": year, "month": month, "day": day}
	payload = json.dumps(payload)
	payload = payload.encode()
	response = l_client.invoke(FunctionName="ETL-Scheduler", InvocationType="Event", Payload=payload)