#!/usr/bin/env bash
master=ec2-52-50-233-45.eu-west-1.compute.amazonaws.com
ssh -i ~/.ssh/ireland.pem -N -L 8088:$master:8088 hadoop@$master &
ssh -i ~/.ssh/ireland.pem -N -L 20888:$master:20888 hadoop@$master &
ssh -i ~/.ssh/ireland.pem -N -L 19888:$master:19888 hadoop@$master &
