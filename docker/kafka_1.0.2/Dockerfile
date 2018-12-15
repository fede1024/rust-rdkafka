FROM ubuntu:16.04

# Install Java.
ENV JAVA_HOME /usr/lib/jvm/java-11-oracle
ENV PATH="$PATH:$JAVA_HOME/bin"
RUN apt-get update && \
    apt-get install software-properties-common -y
RUN \
  echo oracle-java11-installer shared/accepted-oracle-license-v1-2 select true | /usr/bin/debconf-set-selections && \
  add-apt-repository -y ppa:linuxuprising/java && \
  apt-get update && \
  apt-get install -y oracle-java11-installer

# Install Kafka.
RUN apt-get update && apt-get install -y \
    unzip \
    wget \
    curl \
    jq \
    coreutils
ENV KAFKA_VERSION="1.0.2"
ENV SCALA_VERSION="2.12"
ENV KAFKA_GC_LOG_OPTS=" "
ENV KAFKA_HOME /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}
COPY download-kafka.sh /tmp/download-kafka.sh
RUN chmod 755 /tmp/download-kafka.sh
RUN /tmp/download-kafka.sh && tar xfz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt && rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
ENV PATH="$PATH:$KAFKA_HOME/bin"

COPY config.properties /server.properties

CMD echo "Kafka starting" && rm -rf /var/lib/kafka && kafka-server-start.sh /server.properties
