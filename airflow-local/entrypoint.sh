#!/usr/bin/env bash
#adapted from https://github.com/marclamberti/airflow-prod/blob/master/script/entrypoint.sh label maintainer: marc lamberti

TRY_LOOP="3"

# Global defaults and back-compat
: "${AIRFLOW_HOME:="/usr/local/airflow"}"
: "${AIRFLOW__CORE__FERNET_KEY:="$AIRFLOW_FERNET_KEY"}"
: "${AIRFLOW__CORE__EXECUTOR:="$AIRFLOW_EXECUTOR"}"

export \
  AIRFLOW_HOME \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

# Other executors than SequentialExecutor drive the need for an SQL database, here PostgreSQL is used
if [ "$AIRFLOW__CORE__EXECUTOR" != "SequentialExecutor" ]; then
  # Check if the user has provided explicit Airflow configuration concerning the database
  if [ -z "$AIRFLOW__CORE__SQL_ALCHEMY_CONN" ]; then
    # Default values corresponding to the default compose files
    : "${POSTGRES_HOST:="$PG_MAS_HOST"}"
    : "${POSTGRES_PORT:="$PG_BOUNCER_PORT"}"
    : "${POSTGRES_USER:="$AIRFLOW_USER"}"
    : "${POSTGRES_PASSWORD:="$AIRFLOW_PASSWORD"}"
    : "${POSTGRES_DB:="$POSTGRES_DB"}"
    : "${POSTGRES_EXTRAS:-""}"

    AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}${POSTGRES_EXTRAS}"
    export AIRFLOW__CORE__SQL_ALCHEMY_CONN

    # Check if the user has provided explicit Airflow configuration for the broker's connection to the database
    if [ "$AIRFLOW__CORE__EXECUTOR" = "CeleryExecutor" ]; then
      AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${PG_MAS_HOST}:${PG_BOUNCER_PORT}/${POSTGRES_DB}/${POSTGRES_USER}"
      export AIRFLOW__CELERY__RESULT_BACKEND
    fi
  else
    if [[ "$AIRFLOW__CORE__EXECUTOR" == "CeleryExecutor" && -z "$AIRFLOW__CELERY__RESULT_BACKEND" ]]; then
      >&2 printf '%s\n' "FATAL: if you set AIRFLOW__CORE__SQL_ALCHEMY_CONN manually with CeleryExecutor you must also set AIRFLOW__CELERY__RESULT_BACKEND"
      exit 1
    fi

    # Derive useful variables from the AIRFLOW__ variables provided explicitly by the user
    POSTGRES_ENDPOINT=$(echo -n "$AIRFLOW__CORE__SQL_ALCHEMY_CONN" | cut -d '/' -f3 | sed -e 's,.*@,,')
    POSTGRES_HOST=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f1)
    POSTGRES_PORT=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f2)
  fi

  wait_for_port "postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"
fi

# CeleryExecutor drives the need for a Celery broker, here RabbitMQ is used
if [ "$AIRFLOW__CORE__EXECUTOR" = "CeleryExecutor" ]; then
  # Check for explicit Airflow configuration concerning the broker
  if [ -z "$AIRFLOW__CELERY__BROKER_URL" ]; then
    # Default values corresponding to the default compose files
    : "${RABBITMQ_PROTO:="amqp://"}"
    : "${RABBITMQ_HOST:="$RABBITMQ_HOST"}"
    : "${RABBITMQ_PORT:="$RABBITMQ_PORT"}"
    : "${RABBITMQ_USER:="$RABBITMQ_USER"}"
    : "${RABBITMQ_PASSWORD:="$RABBITMQ_PASSWORD"}"
    : "${RABBITMQ_VIRTUALHOST:="$RABBITMQ_VIRTUAL"}"

    AIRFLOW__CELERY__BROKER_URL="${RABBITMQ_PROTO}${RABBITMQ_USER}:${RABBITMQ_PASSWORD}@${RABBITMQ_HOST}:${RABBITMQ_PORT}/${RABBITMQ_VIRTUAL}"
    export AIRFLOW__CELERY__BROKER_URL
  else
    # Derive useful variables from the AIRFLOW__ variables provided explicitly by the user
    RABBITMQ_ENDPOINT=$(echo -n "$AIRFLOW__CELERY__BROKER_URL" | cut -d '/' -f3 | sed -e 's,.*@,,')
    RABBITMQ_HOST=$(echo -n "$RABBITMQ_HOST" | cut -d ':' -f1)
    RABBITMQ_PORT=$(echo -n "$RABBITMQ_PORT" | cut -d ':' -f2)
  fi

  wait_for_port "rabbitmq" "$RABBITMQ_HOST" "$RABBITMQ_PORT"

fi

case "$1" in
  webserver)
    # initialise and upgrade db, and create webserver user
    airflow db init
    airflow db upgrade 
    airflow users create \
      --role Admin --username $_AIRFLOW_WWW_USER_USERNAME --password $_AIRFLOW_WWW_USER_PASSWORD \
      --firstname air --lastname admin \
      --email $_AIRFLOW_WWW_USER_EMAIL

    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ] || [ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]; then
      # With the "Local" and "Sequential" executors it should all run in one container.
      airflow scheduler &
    fi
    exec airflow webserver
    ;;
  worker|scheduler)
    # Give the webserver time to run initdb.
    sleep 30
    exec airflow "$@"
    ;;
  flower)
    sleep 10
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    # substitute the correct values
    exec "$@"
    ;;
esac