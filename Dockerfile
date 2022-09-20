FROM golang:1.18-stretch

RUN mkdir /etc/predator
COPY out /etc/predator
WORKDIR /etc/predator

RUN ln /etc/predator/predator /usr/local/bin/predator

CMD predator