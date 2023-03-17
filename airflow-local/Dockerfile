FROM python:3.9-slim-buster AS airflow-setup

# adapted from label maintainer mac lamberti https://github.com/marclamberti/airflow-prod/blob/master/Dockerfile

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# build arguments 
ARG AIRFLOW_VERSION=2.4.2
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
ENV PATH="$AIRFLOW_HOME/.local/bin:$PATH"
ENV PATH="$AIRFLOW_HOME/.local/lib/python3.9/site-packages:$PATH"

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-

#run as root user 
USER root
#amending updated path to z profile to enable container to find airflow CLI executable in container
RUN echo ${PATH} >> ~/.zshrc 

# build dependencies for packages and software 
RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
        gzip \
        gettext-base \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        erlang \
        rabbitmq-server \
        locales \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd airflow -u 502 -g 0 -d ${AIRFLOW_USER_HOME} -ms /bin/bash \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

#copying to container all config files needed to set up airflow 
COPY --chown=airflow:root ./config/airflow.cfg ${AIRFLOW_USER_HOME}/
COPY --chown=airflow:root ./dags ${AIRFLOW_USER_HOME}/dags
COPY --chown=airflow:root ./logs ${AIRFLOW_USER_HOME}/logs
COPY --chown=airflow:root ./requirements-py3.9.txt ${AIRFLOW_USER_HOME}/requirements-py3.9.txt
COPY --chown=airflow:root ./cache ${AIRFLOW_USER_HOME}/cache

#setting permissions on root directories for airflow user
RUN chown -R airflow:root ${AIRFLOW_USER_HOME}
RUN chmod -R 777 ${AIRFLOW_USER_HOME}

#copy entrypoint script to container and make executable 
COPY --chown=airflow:root ./entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# switch to airflow user to bypass permissions in container 
USER airflow

# creating set up tools and wheels to create custom python packages
# upgrade pip and install dependnencies needed to build pscyopg2 module 
RUN pip3.9 install --no-cache-dir --upgrade pip
RUN pip3.9 install --no-cache-dir setuptools wheel psycopg2 python-decouple fastf1
#install from requiremnts.txt to reduce the startup time for airflow scheduler, and install local packages to airflow image
RUN pip3.9 install --no-cache-dir 'apache-airflow[postgres,celery,amazon,hive,kubernetes,ftp,http,sftp,jdbc,snowflake,ssh,sqlite,imap,s3,rabbitmq]'==${AIRFLOW_VERSION} --constraint ${AIRFLOW_USER_HOME}/requirements-py3.9.txt 

WORKDIR ${AIRFLOW_USER_HOME}

EXPOSE 8080 8793
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver", "scheduler", "worker", "flower"]