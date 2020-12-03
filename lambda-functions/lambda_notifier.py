import os, urllib3 
import json
import datetime

http = urllib3.PoolManager()

url = os.environ.get('chime_url')

def format_time(tstamp):
    return datetime.datetime.fromtimestamp(float(tstamp)).strftime("%Y-%m-%d %H:%M:%S")
    
def lambda_handler(event, context):
    for record in event['Records']:
        item = record['dynamodb'].get('NewImage', None)
        if item:
            attrs = {}
            for k,v in item.items():
                for i,j in v.items():
                    attrs[k] = j
            # print(attrs)
            hr = "------------- | -------------"
            status = attrs.get('status', None)
            if not status:
                continue
            
            if status == "SUBMITTED" or status == "RESUBMITTED":
                tr = "/md ETL | Data Date | Cluster ID | Step ID | Status | Submit Time"
                td = "{} | {} | {} | {} | :arrows_counterclockwise: {} | {}".format(attrs['etl'], attrs['date'], attrs['cluster_id'], attrs['step_id'], attrs['status'], format_time(attrs['submit_time']))
            elif status == "RUNNING":
                tr = "/md ETL | Data Date | Step ID | App ID | Status | User | Queue | Start Time"
                td = "{} | {} | {} | {} | :recycle: {} | {} | {} | {}".format(attrs['etl'], attrs['date'], attrs['step_id'], attrs['app_id'], attrs['status'], attrs['user'], attrs['queue'], format_time(attrs['app_starttime']))
            elif status == "COMPLETED" or status == "FAILED":
                tr = "/md ETL | Data Date | Step ID | App ID | Status | End Time | Time Taken"
                st = ":white_check_mark: {}".format(status) if status == "COMPLETED" else ":red_circle: {}".format(status)
                td = "{} | {} | {} | {} | {} | {} | {}".format(attrs['etl'], attrs['date'], attrs['step_id'], attrs['app_id'], st, format_time(attrs['app_endtime']), "{}s".format(attrs['app_runtime']))
            else:
                # print(record)
                continue
            
            message = "\n".join([tr, hr, td])
            encoded_message = json.dumps({"Content": message}).encode('utf-8')
            try:
                resp = http.request('POST',url, headers={'Content-Type':'application/json'}, body=encoded_message)
                print("Message sent to Chime!")
            except Exception as e:
                print(e)
                continue
    
    return True