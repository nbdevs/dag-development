#!/bin/bash

# This script needs to be executed just once
if [ -f /$0.completed ] ; then
  echo "$0 `date` /$0.completed found, will now skip this run..."
  exit 0
fi

# wait until rabbitmq node is active before doing the following
for (( ; ; )) ; do
  sleep 5
  # redirect output if error in node healthcheck to null file 
  rabbitmq-diagnostics -q check_running && rabbitmq-diagnostics -q check_local_alarms > /dev/null 2>&1
  if [ $? -eq 0 ] ; then
    echo "$0 `date` [SUCCESS] rabbitmq is now running..."
    break
  else
    echo "$0 `date` [PENDING] waiting for rabbitmq startup..."
  fi
done

# initialising two extra users for virtual views

# creating db user, tags and setting permissions
rabbitmqctl add_user $RABBITMQ_USER2 $RABBITMQ_PASSWORD2 2>/dev/null ; \
rabbitmqctl set_user_tags $RABBITMQ_USER2 "administrator" ; \
rabbitmqctl set_permissions -p / $RABBITMQ_USER2  ".*" ".*" ".*" ; \

# creating dw user, tags and setting permissions
rabbitmqctl add_user $RABBITMQ_USER3 $RABBITMQ_PASSWORD3 2>/dev/null ; \
rabbitmqctl set_user_tags $RABBITMQ_USER3 "administrator" ; \
rabbitmqctl set_permissions -p / $RABBITMQ_USER3  ".*" ".*" ".*" ; \

# Create FLAG to alert in future runs of prior completion
touch /$0.completed