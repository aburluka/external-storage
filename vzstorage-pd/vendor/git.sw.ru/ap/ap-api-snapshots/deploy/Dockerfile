FROM centos:7
ENTRYPOINT ["vzsnap-cmd"]

ADD virtuozzo.repo /etc/yum.repos.d/
ADD vzlinux.repo /etc/yum.repos.d/
RUN printf "upgrade \n install vstorage-ctl vstorage-client ploop gdisk \n run" | yum shell -y

RUN yum install -y git make gcc
RUN yum install -y openssl-devel libuuid-devel libxml2-devel e2fsprogs-devel
RUN git clone            https://avagin%40openvz.org@src.openvz.org/scm/ovz/ploop.git /ploop
WORKDIR /ploop
RUN git fetch origin refs/pull-requests/56/from
RUN git checkout FETCH_HEAD
RUN sed -i 's|.*ploop/libploop.h.*|#include "libploop.h"|' include/libvolume.h
RUN yum install -y python-devel
RUN make
RUN make install

ADD vzsnap-cmd /usr/bin/
