# docker build -t raftkeeper/raftkeeper-integration-tests-runner .
FROM raftkeeper/raftkeeper-test-base

## ARG for quick switch to a given ubuntu mirror
#ARG apt_archive="http://mirrors.tuna.tsinghua.edu.cn"
#RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list
#RUN sed -i "s|https://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
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
    software-properties-common \
    libkrb5-dev \
    krb5-user \
    g++ \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean

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

RUN python3 -m pip install \
    aerospike==4.0.0 \
    avro==1.10.2 \
    dict2xml \
    dicttoxml \
    docker \
    docker-compose==1.28.2 \
    grpcio \
    grpcio-tools \
    kazoo \
    lz4 \
    minio \
    protobuf \
    psycopg2-binary==2.8.6 \
    pytest \
    pytest-order==1.0.0 \
    pytest-timeout \
    pytest-xdist \
    pytest-repeat \
    pytz \
    tzlocal==2.1 \
    urllib3 \
    requests-kerberos

COPY modprobe.sh /usr/local/bin/modprobe
COPY dockerd-entrypoint.sh /usr/local/bin/
COPY compose/ /compose/

RUN set -x \
  && addgroup --system dockremap \
    && adduser --system dockremap \
  && adduser dockremap dockremap \
  && echo 'dockremap:165536:65536' >> /etc/subuid \
    && echo 'dockremap:165536:65536' >> /etc/subgid

EXPOSE 2375
ENTRYPOINT ["dockerd-entrypoint.sh"]
CMD ["sh", "-c", "pytest $PYTEST_OPTS"]
