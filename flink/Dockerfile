# =============================================================================
# Flink 1.20.3 (Java 17) + Apache Iceberg 1.9.1 (Hive catalog) + Hadoop S3A
# (Garage). Phase 12: streams Kafka raw.web_events -> iceberg.bronze.
#
#   Stage 1 (maven): resolves the exact dependency closure for
#                    iceberg-flink-1.20 + flink-sql-connector-kafka (pom.xml).
#   Stage 2 (flink): the Flink distribution with those jars in lib/ plus the
#                    self-contained flink-s3-fs-hadoop shaded jar (hadoop-common
#                    + hadoop-aws + AWS SDK v1) so Iceberg's HadoopFileIO can
#                    write s3a:// to Garage.
#
#   S3A configuration (fs.s3a.*) is NOT baked in - it is mounted at runtime
#   from flink/hadoop/core-site.xml (so .env values stay authoritative).
# =============================================================================

FROM apache/hive:3.1.3 AS hive-jars
# (source of the Hive 3.1.3 client jars - Iceberg's hive module is compiled
#  against the Hive 3.x client API; the Thrift protocol is compatible with
#  the local Hive 4.0.1 HMS server. NOTE: the hive-metastore jar in these
#  images is thinned - the full client lives in hive-standalone-metastore.)

FROM maven:3.9-eclipse-temurin-17 AS deps
COPY pom.xml /build/pom.xml
WORKDIR /build
RUN mvn -B -q dependency:copy-dependencies

FROM flink:1.20.3-scala_2.12-java17
COPY --from=deps /build/target/dependency/ /opt/flink/lib/
# Hive 3.1.3 client jars: Iceberg's hive catalog references
# org.apache.hadoop.hive.metastore.* at runtime, but Iceberg's published pom
# does NOT declare them (verified in 1.9.1). (No derby/jdo/datanucleus -
# those are only for the embedded metastore; no hive jackson - the closure's
# 2.15.2 serves the Hive client fine and avoids duplicate jars.)
COPY --from=hive-jars /opt/hive/lib/hive-standalone-metastore-3.1.3.jar /opt/flink/lib/
COPY --from=hive-jars /opt/hive/lib/hive-common-3.1.3.jar /opt/flink/lib/
COPY --from=hive-jars /opt/hive/lib/hive-storage-api-2.7.0.jar /opt/flink/lib/
COPY --from=hive-jars /opt/hive/lib/libthrift-0.9.3.jar /opt/flink/lib/
COPY --from=hive-jars /opt/hive/lib/libfb303-0.9.3.jar /opt/flink/lib/
COPY --from=hive-jars /opt/hive/lib/guava-19.0.jar /opt/flink/lib/
COPY --from=hive-jars /opt/hive/lib/commons-lang-2.6.jar /opt/flink/lib/
# Hadoop client jars (3.3.6 = same Hadoop as the S3A shaded jar, which
# ships a TRIMMED hadoop-common; the full one provides the rest):
#  - common: LeaseRecoverable & friends needed by the Iceberg write path
#  - hdfs-client: catalog init resolves HdfsConfiguration
#  - mapreduce-client core/common: the Iceberg write path instantiates JobConf
RUN curl -fsSL -o /opt/flink/lib/hadoop-common-3.3.6.jar \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar" \
    && curl -fsSL -o /opt/flink/lib/hadoop-hdfs-client-3.3.6.jar \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.6/hadoop-hdfs-client-3.3.6.jar" \
    && curl -fsSL -o /opt/flink/lib/hadoop-mapreduce-client-core-3.3.6.jar \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.6/hadoop-mapreduce-client-core-3.3.6.jar" \
    && curl -fsSL -o /opt/flink/lib/hadoop-mapreduce-client-common-3.3.6.jar \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-common/3.3.6/hadoop-mapreduce-client-common-3.3.6.jar" \
    && curl -fsSL -o /opt/flink/lib/hadoop-yarn-api-3.3.6.jar \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-yarn-api/3.3.6/hadoop-yarn-api-3.3.6.jar"
RUN grep -qa "org/apache/iceberg/flink/sink/IcebergSink.class" \
      /opt/flink/lib/iceberg-flink-1.20-1.9.1.jar \
    && grep -qa "org/apache/flink/streaming/connectors/kafka/table/KafkaDynamicSource.class" \
      /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar \
    && grep -qa "org/apache/hadoop/hive/metastore/IMetaStoreClient.class" \
      /opt/flink/lib/hive-standalone-metastore-3.1.3.jar \
    && echo "Iceberg + Kafka + Hive client classes present"
# (the zip central directory stores entry names uncompressed, so grep -a can
#  verify the jar's content without unzip)
RUN curl -fsSL -o /opt/flink/lib/flink-s3-fs-hadoop-1.20.3.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.20.3/flink-s3-fs-hadoop-1.20.3.jar" \
    && grep -qa "org/apache/hadoop/fs/s3a/S3AFileSystem.class" /opt/flink/lib/flink-s3-fs-hadoop-1.20.3.jar \
    && echo "S3A classes present"
