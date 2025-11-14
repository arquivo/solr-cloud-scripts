# Arquivo.pt Image and Page search requires a Solr with support for a Javascript engine.
# From Java 12 the JavaScript engine has been removed.
# We require that engine, because the insertion of data runs a JS file that in practice runs
# a deduplication, to prevent duplicated images or pages.

ARG SOLR_VERSION="9.10.0"
ARG MAVEN_VERSION="3"

# Stage 1: Use Maven to fetch Nashorn and dependencies
FROM maven:${MAVEN_VERSION} AS nashorn-fetcher

ARG NASHORN_VERSION="15.4"

# Create a temporary Maven project with Nashorn as a dependency
WORKDIR /app
RUN mkdir -p src/main/java

RUN cat <<EOF > pom.xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>temp</groupId>
    <artifactId>nashorn-download</artifactId>
    <version>1.0</version>
    <dependencies>
        <dependency>
            <groupId>org.openjdk.nashorn</groupId>
            <artifactId>nashorn-core</artifactId>
            <version>\${nashornVersion}</version>
        </dependency>
    </dependencies>
</project>
EOF

# Download Nashorn and all transitive dependencies
RUN mvn dependency:copy-dependencies -DoutputDirectory=/deps -DnashornVersion=$NASHORN_VERSION

# Stage 2: Solr base image
FROM solr:${SOLR_VERSION} AS arquivo-solr

# Copy Nashorn JARs into Solr's lib directory
COPY --from=nashorn-fetcher /deps/*.jar /opt/solr/server/lib/

# Enable Nashorn module in JVM options
# ENV SOLR_OPTS="$SOLR_OPTS --add-modules jdk.scripting.nashorn"

EXPOSE 8983
CMD ["solr-foreground"]
