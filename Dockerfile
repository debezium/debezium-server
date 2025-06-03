FROM registry.access.redhat.com/ubi8/openjdk-21 AS builder

LABEL maintainer="Debezium Community"

ENV SERVER_HOME=/debezium \
    DEBEZIUM_ARCHIVE=/tmp/debezium.tar.gz

#
# Create a directory for Debezium Server
#
USER root
RUN microdnf -y install gzip && \
    microdnf clean all && \
    mkdir $SERVER_HOME && \
    chmod 755 $SERVER_HOME

#
# Change ownership and switch user
#
RUN chown -R jboss $SERVER_HOME && \
    chgrp -R jboss $SERVER_HOME
USER jboss

RUN mkdir $SERVER_HOME/conf && \
    mkdir $SERVER_HOME/data

ARG DEBEZIUM_SERVER_DIST_FILENAME
#
# Copy built artifact
#
COPY --chown=jboss:jboss debezium-server-dist/target/${DEBEZIUM_SERVER_DIST_FILENAME} $DEBEZIUM_ARCHIVE

#
# Verify the contents and then install ...
#
RUN tar xzf /tmp/debezium.tar.gz -C $SERVER_HOME --strip-components 1 &&\
    rm -f /tmp/debezium.tar.gz

#
# Add jolokia for healthchecks over jmx
#
ADD --chown=jboss:jboss https://repo1.maven.org/maven2/org/jolokia/jolokia-jvm/1.7.2/jolokia-jvm-1.7.2.jar $SERVER_HOME/jolokia.jar

COPY --chown=jboss:jboss bridge-run.sh $SERVER_HOME

#
# Allow random UID to use Debezium Server
#
RUN chmod -R g+w,o+w $SERVER_HOME

# Set the working directory to the Debezium Server home directory
WORKDIR $SERVER_HOME

#
# Expose the ports and set up volumes for the data, transaction log, and configuration
#
EXPOSE 8080
VOLUME ["/debezium/conf","/debezium/data"]

CMD ["/debezium/bridge-run.sh"]
