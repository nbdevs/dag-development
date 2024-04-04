#!/bin/bash

# run in tandem preconfig file and rabbitmq
/config.sh &

# launching rabbitmq 
exec rabbitmq-server