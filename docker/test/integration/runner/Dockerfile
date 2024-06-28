# docker build -t raftkeeper/raftkeeper-integration-tests-runner .
FROM ubuntu:22.04

## ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"

RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
    adduser \
    ca-certificates \
    bash \
    btrfs-progs \
    e2fsprogs \
    iptables \
    xfsprogs \
    tar \
    pigz \
    wget \
    git \
    iproute2 \
    cgroupfs-mount \
    python3-pip \
    tzdata \
    libicu-dev \
    bsdutils \
    curl \
    python3-pika \
    liblua5.1-dev \
    luajit \
    libssl-dev \
    libcurl4-openssl-dev \
    gdb \
    default-jdk \
    software-properties-common \
    libkrb5-dev \
    krb5-user \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV DOCKER_CHANNEL stable
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
RUN add-apt-repository "deb https://download.docker.com/linux/ubuntu $(lsb_release -c -s) ${DOCKER_CHANNEL}"

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
        docker-ce \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean

RUN dockerd --version; docker --version

RUN python3 -m pip install --no-cache-dir \
    avro==1.10.2 \
    dict2xml==1.7.4 \
    dicttoxml==1.7.16 \
    docker==6.1.3 \
    docker-compose==1.29.2 \
    grpcio==1.60.0 \
    grpcio-tools==1.60.0 \
    kazoo==2.9.0\
    protobuf==4.25.2 \
    psycopg2-binary==2.9.6 \
    pytest==7.4.4 \
    pytest-order==1.0.0 \
    pytest-timeout==2.2.0 \
    pytest-xdist==3.5.0 \
    pytest-repeat==0.9.3 \
    pytz==2023.3.post1 \
    pyyaml==5.3.1 \
    tzlocal==2.1 \
    urllib3==2.0.7 \
    requests-kerberos==0.14.0 \
    requests==2.31.0

COPY modprobe.sh /usr/local/bin/modprobe
COPY dockerd-entrypoint.sh /usr/local/bin/
COPY compose/ /compose/
COPY zookeeper.tar /usr/local/bin/
COPY raftkeeper-network-partition.tar  /usr/local/bin/
COPY raftkeeper-integration-tests.tar /usr/local/bin/

RUN set -x \
  && addgroup --system dockremap \
    && adduser --system dockremap \
  && adduser dockremap dockremap \
  && echo 'dockremap:165536:65536' >> /etc/subuid \
    && echo 'dockremap:165536:65536' >> /etc/subgid

EXPOSE 2375
ENTRYPOINT ["dockerd-entrypoint.sh"]
CMD ["sh", "-c", "pytest $PYTEST_OPTS"]
