FROM golang:1.16-alpine

RUN apk add --update --no-cache build-base git bash

WORKDIR /code
ADD . .
RUN make

ENTRYPOINT ["./docker-entrypoint.sh"]
