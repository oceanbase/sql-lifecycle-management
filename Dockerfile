FROM centos:centos7

# create user
RUN groupadd -g 201 admin
RUN useradd -u 300 -g admin -G admin -d /home/admin -s /bin/bash -m admin
RUN mkdir -p /root/install
ADD ./install/ /root/install

# install depend rpm
RUN yum install -y gcc-c++ zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel libpcap-devel xz-devel pcre-devel
RUN yum -y install mysql
RUN yum -y install wget
RUN yum -y install gcc automake autoconf libtool make



# install python and package
RUN mkdir -p /usr/local/python3
RUN tar -zxvf /root/install/Python-3.6.6.tgz -C /root/install
WORKDIR /root/install/Python-3.6.6
RUN env ./configure --prefix=/usr/local/python3
RUN env make
RUN env make install
RUN env ln -sf /usr/local/python3/bin/python3 /usr/bin/python3
RUN env python3 -m pip install --upgrade setuptools -i https://mirrors.aliyun.com/pypi/simple  --extra-index-url http://pypi.tuna.tsinghua.edu.cn/pypi/simple/  --extra-index-url http://pypi.douban.com/pypi/simple/  --trusted-host mirrors.aliyun.com   --trusted-host pypi.tuna.tsinghua.edu.cn   --trusted-host pypi.douban.com
RUN env python3 -m pip install -r /root/sqless_install/requirements.txt -i https://mirrors.aliyun.com/pypi/simple --extra-index-url http://pypi.tuna.tsinghua.edu.cn/pypi/simple/  --extra-index-url http://pypi.douban.com/pypi/simple/  --trusted-host mirrors.aliyun.com   --trusted-host pypi.tuna.tsinghua.edu.cn   --trusted-host pypi.douban.com

WORKDIR /root/sqless_install