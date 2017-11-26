FROM ubuntu:16.04

# Install Java.
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle
ENV PATH="$PATH:$JAVA_HOME/bin"
RUN apt-get update && \
    apt-get install software-properties-common -y
RUN \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer

# Install Kafka.
RUN apt-get update && apt-get install -y \
    unzip \
    wget \
    curl \
    jq \
    coreutils
ENV KAFKA_VERSION="0.11.0.2"
ENV SCALA_VERSION="2.12"
ENV KAFKA_HOME /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}
COPY download-kafka.sh /tmp/download-kafka.sh
RUN chmod 755 /tmp/download-kafka.sh
RUN /tmp/download-kafka.sh && tar xfz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt && rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
ENV PATH="$PATH:$KAFKA_HOME/bin"

COPY config.properties /server.properties

CMD echo "Kafka starting" && rm -rf /var/lib/kafka  && kafka-server-start.sh /server.properties
