#!/bin/bash

# run in tandem preconfig file and rabbitmq
/scripts/config.sh &

dpkg-reconfigure -f noninteractive tzdata

# launching rabbitmq 
exec rabbitmq-server