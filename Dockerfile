FROM python:3.8.13

#update
RUN  apt-get update \
  && apt-get upgrade -y \
  && apt-get install -y gdal-bin libgdal-dev

#install spark 3.2.1
RUN apt-get install -y curl mlocate default-jdk wget -y && \
  wget https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz && \
  tar xvf spark-3.2.1-bin-hadoop3.2.tgz && mv spark-3.2.1-bin-hadoop3.2/ /opt/spark

ENV DEBIAN_FRONTEND=noninteractive 
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

#get poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
RUN /root/.local/bin/poetry config virtualenvs.create false
RUN /root/.local/bin/poetry config virtualenvs.in-project false

##copy in poetry deps
RUN mkdir /root/code
COPY poetry.lock /root/code
COPY pyproject.toml /root/code

# build the environment
WORKDIR /root/code
RUN /root/.local/bin/poetry install --no-root
ENTRYPOINT ["/root/.local/bin/poetry","run","kedro"]
CMD ["registry","list"]
