# docker build -t raftkeeper/raftkeeper-test-base .
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=13

# For Chainese users, if there is something wrong, just comment it.
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
    && apt-get install ca-certificates wget gnupg apt-transport-https --yes --no-install-recommends --verbose-versions

RUN echo "# llvm " >> /etc/apt/sources.list
RUN echo "deb [trusted=yes] http://apt.llvm.org/focal/ llvm-toolchain-focal-${LLVM_VERSION} main" >> /etc/apt/sources.list

# initial packages
RUN apt-get update \
    && apt-get install \
        bash \
        fakeroot \
        ccache \
        curl \
        vim \
        software-properties-common \
        --yes --no-install-recommends

RUN apt-get update \
    && apt-get install \
        llvm-${LLVM_VERSION} \
        debhelper \
        devscripts \
        gdb  \
        git \
        gperf \
        lcov \
        moreutils \
        perl \
        pigz \
        pkg-config \
        tzdata \
        --yes --no-install-recommends

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

CMD sleep 1
