FROM apache/airflow:2.10.2
USER root

RUN apt update -y && \
    apt upgrade -y && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get install -y zip && \
    apt-get install -y unzip && \
    apt clean;

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

RUN curl -s "https://get.sdkman.io" | bash;
RUN /bin/bash -c "source /home/airflow/.sdkman/bin/sdkman-init.sh; sdk version; sdk install sbt"


COPY ./requirements.txt /
RUN pip install -r /requirements.txt