FROM alpine:3.13

COPY predator /usr/bin/predator
WORKDIR /app

CMD predator ${SUB_COMMAND} -s ${PREDATOR_URL} -u "${BQ_PROJECT}.${BQ_DATASET}.${BQ_TABLE}"
