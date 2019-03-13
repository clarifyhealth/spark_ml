#!/usr/bin/env bash

if [[ $# -eq 0 ]]
  then
    sudo echo "No python36 dependencies to install"
   else
    sudo python3 -m pip install "$@"
fi