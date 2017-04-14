FROM ubuntu:16.04

RUN apt-get update && apt-get -y install zookeeper

CMD rm -rf /tmp/zookeeper && /usr/share/zookeeper/bin/zkServer.sh start-foreground
