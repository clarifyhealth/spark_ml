#!/usr/bin/env bash

if [[ $# -eq 0 ]]
  then
    sudo echo "No Local python36 dependencies to install"
   else
    cd /home/hadoop
    sudo aws s3 cp s3://workbenches-emr-misc/code/${1} /home/hadoop
    sudo python3 -m pip install ${1}
fi
