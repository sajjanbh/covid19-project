#!/bin/bash
# Run it as Step in EMR. Eg. s3://<region>.elasticmapreduce/libs/script-runner/script-runner.jar Args: s3://<your-bucket>/scripts/zeppelin/configure_zeppelin.sh

shiro_location="s3://<your-bucket>/scripts/zeppelin/shiro.ini"
interpreter_location="s3://<your-bucket>/scripts/zeppelin/interpreter.json"

# Create HDFS home folders for users in Active Directory
users=(etl <user1> <user2>)
for user in ${users[@]}; do
	hdfs dfs -mkdir /user/${user}
	hdfs dfs -chown ${user}:${user} /user/${user}
done

sudo mv /etc/zeppelin/conf/shiro.ini /etc/zeppelin/conf/shiro.ini_BKP
sudo mv /etc/zeppelin/conf/interpreter.json /etc/zeppelin/conf/interpreter.json_BKP
sudo aws s3 cp ${shiro_location} /etc/zeppelin/conf/
sudo aws s3 cp ${interpreter_location} /etc/zeppelin/conf/

sudo chown zeppelin:zeppelin /etc/zeppelin/conf/shiro.ini
sudo chown zeppelin:zeppelin /etc/zeppelin/conf/interpreter.json 

echo 'zeppelin ALL=(ALL) NOPASSWD: ALL' | sudo EDITOR='tee -a' visudo

sudo stop zeppelin
sudo start zeppelin