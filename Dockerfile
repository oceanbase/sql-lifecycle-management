FROM ubuntu:latest

ADD ./ /home/admin/sqless

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev \
libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev \
libgdbm-dev libnss3-dev libedit-dev libc6-dev pip vim

RUN wget \
    https://www.python.org/ftp/python/3.8.10/Python-3.8.10.tgz \
    && tar -xzf Python-3.8.10.tgz

WORKDIR Python-3.8.10

RUN ./configure --enable-optimizations  -with-lto  --with-pydebug
RUN env make altinstall

WORKDIR /home/admin/sqless

RUN make install


