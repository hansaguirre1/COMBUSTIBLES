FROM apache/airflow:2.7.3-python3.9

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get install -y libpq-dev && \
    apt-get clean

# Set JAVA_HOME environment variable
# ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64 # cuando es para computadoras con chip intel o amd usar amd64
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt