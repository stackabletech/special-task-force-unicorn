FROM alpine AS download
RUN apk add curl tar
RUN curl -o hadoop-3.3.1.tar.gz https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz
RUN tar xzf hadoop-3.3.1.tar.gz
RUN rm -r hadoop-3.3.1/share/doc hadoop-3.3.1/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.901.jar

FROM openjdk:11
COPY --from=download hadoop-3.3.1 /opt/hadoop