FROM rabbitmq:3.11-management AS rabbitmq-setup
WORKDIR /rabbitmq

# rabbitmq setup and configuration files
COPY ./config/definitions.json /etc/rabbitmq/definitions.json
COPY ./config/rabbitmq.conf /etc/rabbitmq/rabbitmq.conf 
COPY ./logs /var/log/rabbitmq/log

# rabbit entrypoint script setup for users 
ADD ./entrypoint.sh /entrypoint.sh
ADD ./config.sh /config.sh
RUN chmod +x /entrypoint.sh /config.sh

# owner permissions 
RUN chown rabbitmq:rabbitmq /etc/rabbitmq/rabbitmq.conf /etc/rabbitmq/definitions.json

EXPOSE 15672 5672

ENTRYPOINT ["/entrypoint.sh"]
