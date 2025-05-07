# Stage 1: Build stage
FROM registry.access.redhat.com/ubi8/openjdk-21 AS builder

LABEL maintainer="Debezium Community"

#
# Set the version, home directory, and MD5 hash.
#
ENV DEBEZIUM_VERSION=3.0.8.Final \
    SERVER_HOME=/debezium \
    MAVEN_REPO_CENTRAL="https://repo1.maven.org/maven2"
ENV SERVER_URL_PATH=io/debezium/debezium-server-dist/$DEBEZIUM_VERSION/debezium-server-dist-$DEBEZIUM_VERSION.tar.gz \
    SERVER_MD5=8bbe45300cebec09364a6a1986c1134f

#
# Create a directory for Debezium Server
#
USER root
RUN microdnf -y install gzip && \
    microdnf clean all && \
    mkdir $SERVER_HOME && \
    chmod 755 $SERVER_HOME

#
# Copy local debezium tar file as root
COPY debezium-server-dist/target/debezium-server-dist-3.0.8.Final.tar.gz /tmp/debezium.tar.gz

# Extract the archive and set permissions as root
RUN tar xzf /tmp/debezium.tar.gz -C $SERVER_HOME --strip-components 1 && \
    rm -f /tmp/debezium.tar.gz && \
    chown -R jboss:jboss $SERVER_HOME && \
    chmod -R g+w,o+w $SERVER_HOME

# Switch to jboss user after file operations are complete
USER jboss
RUN mkdir -p $SERVER_HOME/config && \
    mkdir -p $SERVER_HOME/data


#
# Allow random UID to use Debezium Server
#
RUN chmod -R g+w,o+w $SERVER_HOME

# Stage 2: Final image
FROM registry.access.redhat.com/ubi8/openjdk-21

LABEL maintainer="Debezium Community"

ENV DEBEZIUM_VERSION=3.0.8.Final \
    SERVER_HOME=/debezium \
    MAVEN_REPO_CENTRAL="https://repo1.maven.org/maven2"

USER root
RUN microdnf clean all

USER jboss

COPY --from=builder $SERVER_HOME $SERVER_HOME

# Set the working directory to the Debezium Server home directory
WORKDIR $SERVER_HOME

#
# Expose the ports and set up volumes for the data, transaction log, and configuration
#
EXPOSE 8080
VOLUME ["/debezium/config","/debezium/data"]

CMD ["/debezium/run.sh"]