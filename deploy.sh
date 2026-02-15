#!/bin/bash

# env variable
set -a
source .env
set +a

# deploy
docker stack deploy -c stockdata_portainer.yaml portainer
docker stack deploy -c stockdata_database.yaml database
docker stack deploy -c stockdata_airflow.yaml airflow
docker stack deploy -c stockdata_redash.yaml redash
docker stack deploy -c stockdata_monitoring.yaml monitoring

echo "All services successfully deploy !"