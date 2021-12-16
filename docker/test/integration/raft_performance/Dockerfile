# docker build -t yandex/clickhouse-postgresql-java-client .
# PostgreSQL Java client docker container

FROM yandex/clickhouse-integration-tests-runner:latest

RUN apt-get update && \
     apt-get install -y software-properties-common build-essential openjdk-11-jdk zip unzip \
     && wget http://apache-mirror.rbc.ru/pub/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz \
     && tar xzvf apache-maven-3.3.9-bin.tar.gz \
     && cp -R apache-maven-3.3.9 /usr/local/bin \
     && export PATH=apache-maven-3.3.9/bin:$PATH \
     && export PATH=/usr/local/bin/apache-maven-3.3.9/bin:$PATH \
     #RUN ls -l /usr/local/bin
     && echo $PATH


RUN rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
RUN apt-get clean

