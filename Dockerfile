FROM alpine:3.6

MAINTAINER jff.pereira@campus.fct.unl.pt

RUN apk --update add bash ca-certificates curl openjdk8-jre-base && rm -rf /var/cache/apk/*
ENV JAVA_HOME=/usr/lib/jvm/default-jvm

COPY ./target/ASD-1.0-jar-with-dependencies.jar ./
COPY ./src/network_config.properties ./src/network_config.properties
COPY ./dockerscript.sh ./
RUN chmod 777 dockerscript.sh
RUN mkdir pers
ENTRYPOINT ["/dockerscript.sh"]
