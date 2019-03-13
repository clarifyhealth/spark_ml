#!/usr/bin/env bash

if [[ $# -eq 0 ]]
  then
    sudo echo "No Local python36 dependencies to install"
   else
    sudo aws s3 cp s3://aws-logs-947634201780-us-west-2/code/${1} /home/hadoop
    sudo cd /home/hadoop
    sudo python3 -m pip install ${1}
fi
