# docker build -t raftkeeper/raftkeeper-integration-tests .
FROM raftkeeper/raftkeeper-test-base

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get -y install \
        openjdk-11-jdk \
        tzdata \
        python3 \
        libreadline-dev \
        libicu-dev \
        bsdutils \
        gdb \
        unixodbc \
        odbcinst \
        libsqlite3-dev \
        libsqliteodbc \
        odbc-postgresql \
        sqlite3 \
        curl \
        tar \
        krb5-user \
        iproute2 \
        lsof
RUN rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
RUN apt-get clean

ENV ZOOKEEPER_VERSION='3.6.3'
RUN curl -O "https://dlcdn.apache.org/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz"
RUN tar -zxvf apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz && mv apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper && chmod -R 777 /opt/zookeeper && rm apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz
RUN echo $'tickTime=2500 \n\
tickTime=2500 \n\
dataDir=/zookeeper \n\
clientPort=2181 \n\
maxClientCnxns=80' > /opt/zookeeper/conf/zoo.cfg
RUN mkdir /zookeeper && chmod -R 777 /zookeeper

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

