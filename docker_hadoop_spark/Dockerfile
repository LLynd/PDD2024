FROM ubuntu:20.04
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y sudo sshpass wget python3 python3-pip openssh-server && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir gdown

RUN useradd -ms /bin/bash hadoop && echo "hadoop:hadoop" | chpasswd && adduser hadoop sudo

USER hadoop
WORKDIR /home/hadoop
COPY . .
RUN mkdir ~/.ssh && chmod 700 /home/hadoop && chmod 700 ~/.ssh
RUN echo "hadoop" | sudo -S service ssh restart ; ./0keys.sh ; ./1download.sh ; ./2install_hadoop.sh ; ./3install_spark.sh ; . ./settings.sh ; hdfs namenode -format

EXPOSE 9870
EXPOSE 8088
EXPOSE 8080
CMD echo "hadoop" | sudo -S service ssh restart ; . ./settings.sh ; /bin/bash

