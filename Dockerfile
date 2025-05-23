FROM apache/airflow:2.10.2
USER root

RUN apt update -y;

RUN apt-get install -y \ 
	openjdk-17-jdk \
	ant \
	zip \
	unzip && \
    apt-get clean;

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

RUN curl -s "https://get.sdkman.io" | bash  && \
    bash -c "source /home/airflow/.sdkman/bin/sdkman-init.sh; sdk install sbt"

COPY ./requirements.txt /
RUN pip install -r /requirements.txt