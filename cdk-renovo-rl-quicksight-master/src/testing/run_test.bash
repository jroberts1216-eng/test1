#!/bin/bash

export REDSHIFT_WORKGROUP_NAME='dev-renovolive'
export REDSHIFT_DATABASE='renovolive'
export REDSHIFT_SECRET_ARN='arn:aws:secretsmanager:us-east-1:719035735300:secret:redshift!dev-renovolive-admin-guRcHA'
export RL_DATABASE='renovomaster'
export TABLE='users'
export PRIMARY_KEYS='userid'

aws s3 cp s3://renovolive-gluescripts/console-scripts/test2.py . && python test2.py