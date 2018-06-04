FROM openjdk:latest
COPY ./build/libs /tmp
WORKDIR /tmp
ENV CLASSPATH /tmp/*
ENTRYPOINT ["java","com.thapovan.App"]