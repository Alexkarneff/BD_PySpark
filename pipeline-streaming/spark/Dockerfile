# syntax=docker/dockerfile:1
#
# Spark 3.5 (Scala 2.12, Java 17, Python 3) + Kafka and Cassandra connectors baked into the image.
# Baking the jars in avoids --packages downloads at every start and keeps the image reproducible.
#
# RULE: every Spark connector must match SPARK_VERSION and SCALA_BINARY exactly.
#   - kafka-clients and commons-pool2 versions come from Spark 3.5.9's own pom.xml
#   - the Cassandra connector 3.5.x is the line built for Spark 3.5 (no Spark 4 support)

FROM spark:3.5.9-scala2.12-java17-python3-ubuntu

ARG SPARK_VERSION=3.5.9
ARG SCALA_BINARY=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG CASSANDRA_CONNECTOR_VERSION=3.5.1
ARG MAVEN=https://repo1.maven.org/maven2

# --- Kafka source for Structured Streaming + its runtime dependencies
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar /opt/spark/jars/

# --- Cassandra connector: the "assembly" artifact is a single jar with the Java driver shaded inside
ADD --chmod=644 ${MAVEN}/com/datastax/spark/spark-cassandra-connector-assembly_${SCALA_BINARY}/${CASSANDRA_CONNECTOR_VERSION}/spark-cassandra-connector-assembly_${SCALA_BINARY}-${CASSANDRA_CONNECTOR_VERSION}.jar /opt/spark/jars/

# --- Checkpoint folder owned by the "spark" user.
# A new named volume copies this folder's ownership, so Spark can write its checkpoints there.
USER root
RUN mkdir -p /opt/spark/checkpoints && chown -R spark:spark /opt/spark/checkpoints

# The official image runs as the unprivileged "spark" user
USER spark
WORKDIR /opt/spark/work-dir
