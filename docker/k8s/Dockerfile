FROM ubuntu:20.04

ENV RAFTKEEPER_DIR="/opt/raftkeeper"
ENV ZOOKEEPER_DIR="/opt/zookeeper" 
ENV PATH="$PATH:$ZOOKEEPER_DIR/bin:$RAFTKEEPER_DIR/bin"

EXPOSE 8101


RUN echo "# use mirrors" > /etc/apt/sources.list \
    && echo "deb [trusted=yes] http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal main restricted universe multiverse" >> /etc/apt/sources.list \
    && echo "deb [trusted=yes] http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-updates main restricted universe multiverse" >> /etc/apt/sources.list \
    && echo "deb [trusted=yes] http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-backports main restricted universe multiverse" >> /etc/apt/sources.list \
    && echo "deb [trusted=yes] http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-security main restricted universe multiverse" >> /etc/apt/sources.list \
    && apt-get update \
    && apt-get install wget openjdk-8-jre-headless python3 netcat --yes \
    && apt-get clean

ENV RAFTKEEPER_VERSION='v2.0.1'
RUN wget https://github.com/JDRaftKeeper/RaftKeeper/releases/download/${RAFTKEEPER_VERSION}/RaftKeeper-linux-${RAFTKEEPER_VERSION}.tar.gz \
    && tar -xzvf RaftKeeper-linux-${RAFTKEEPER_VERSION}.tar.gz && mv RaftKeeper ${RAFTKEEPER_DIR} && chmod -R 777 ${RAFTKEEPER_DIR} && rm RaftKeeper-linux-${RAFTKEEPER_VERSION}.tar.gz \
    && cd ${RAFTKEEPER_DIR}/bin \
    && wget https://raw.githubusercontent.com/JDRaftKeeper/RaftKeeper/master/k8s/scripts/config_generator.py \
    && wget https://raw.githubusercontent.com/JDRaftKeeper/RaftKeeper/master/k8s/scripts/start-raftkeeper.sh \
    && wget https://raw.githubusercontent.com/JDRaftKeeper/RaftKeeper/master/k8s/scripts/raftkeeper-ready.sh

# need zkCli.sh for demonstration
ENV ZOOKEEPER_VERSION='3.7.1'
RUN wget "https://dlcdn.apache.org/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz" \
    && tar -zxvf apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz && mv apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper && chmod -R 777 /opt/zookeeper && rm apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz
