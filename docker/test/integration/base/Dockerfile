# docker build -t raftkeeper/raftkeeper-integration-tests .
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=14

# For Chinese users, if there is something wrong, just comment it.
#RUN echo "# use mirrors" > /etc/apt/sources.list
#RUN echo "deb [trusted=yes] http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal main restricted universe multiverse" >> /etc/apt/sources.list
#RUN echo "deb [trusted=yes] http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-updates main restricted universe multiverse" >> /etc/apt/sources.list
#RUN echo "deb [trusted=yes] http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-backports main restricted universe multiverse" >> /etc/apt/sources.list
#RUN echo "deb [trusted=yes] http://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-security main restricted universe multiverse" >> /etc/apt/sources.list

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://mirrors.tuna.tsinghua.edu.cn"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list
RUN sed -i "s|https://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        wget \
        gnupg \
        apt-transport-https \
        bash \
        fakeroot \
        ccache \
        curl \
        vim \
        software-properties-common \
        llvm-${LLVM_VERSION} \
        debhelper \
        devscripts \
        gdb \
        git \
        gperf \
        lcov \
        moreutils \
        perl \
        pigz \
        pkg-config \
        tzdata \
        openjdk-11-jdk \
        python3 \
        libreadline-dev \
        libicu-dev \
        bsdutils \
        iproute2 \
        lsof \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=1 history_size=7'" >> /etc/environment; \
  echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
  echo "MSAN_OPTIONS='abort_on_error=1'" >> /etc/environment; \
  echo "LSAN_OPTIONS='suppressions=/usr/share/raftkeeper-test/config/lsan_suppressions.txt'" >> /etc/environment; \
  ln -s /usr/lib/llvm-${LLVM_VERSION}/bin/llvm-symbolizer /usr/bin/llvm-symbolizer;

# Sanitizer options for current shell (not current, but the one that will be spawned on "docker run")
# (but w/o verbosity for TSAN, otherwise test.reference will not match)
ENV TSAN_OPTIONS='halt_on_error=1 history_size=7'
ENV UBSAN_OPTIONS='print_stacktrace=1'
ENV MSAN_OPTIONS='abort_on_error=1'

ENV ZOOKEEPER_VERSION='3.7.1'
RUN curl "https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz" | \
      tar -C opt -zxv && \
  mv /opt/apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper && \
  chmod -R 777 /opt/zookeeper && \
  echo $'tickTime=2500 \n\
tickTime=2500 \n\
dataDir=/zookeeper \n\
clientPort=2181 \n\
maxClientCnxns=80' > /opt/zookeeper/conf/zoo.cfg && \
  mkdir /zookeeper && \
  chmod -R 777 /zookeeper

# Set timezone
ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD sleep 1