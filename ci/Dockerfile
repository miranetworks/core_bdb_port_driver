FROM ruanjonker/erlang:ubuntu1404_17.4

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update

RUN apt-get install -y dput
RUN apt-get install -y ctags

RUN echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir /workspace
WORKDIR /workspace/base

CMD /bin/bash

