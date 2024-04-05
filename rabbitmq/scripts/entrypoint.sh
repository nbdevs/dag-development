#!/bin/bash

# run in tandem preconfig file and rabbitmq
/scripts/config.sh &

# launching rabbitmq 
exec rabbitmq-server