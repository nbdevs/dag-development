FROM rabbitmq:3.11-management AS rabbitmq-setup

WORKDIR /var/lib/rabbitmq

USER root

# rabbitmq setup and configuration files
COPY --chown=rabbitmq:root ./config/definitions.json /etc/rabbitmq/definitions.json
COPY --chown=rabbitmq:root ./config/rabbitmq.conf /etc/rabbitmq/rabbitmq.conf 
COPY --chown=rabbitmq:root ./logs /etc/rabbitmq/local/rabbitmq

# rabbit entrypoint script setup for users 
COPY --chown=rabbitmq:root ./scripts /scripts

# changing owner permissions for logs, and config files
RUN chown -R rabbitmq:root /scripts /etc/rabbitmq
RUN chmod -R 777 /scripts /etc/rabbitmq

EXPOSE 15672 5672

#switch back to rabbimq user to bypass container permissions 
USER rabbitmq

ENTRYPOINT ["/scripts/entrypoint.sh"]
