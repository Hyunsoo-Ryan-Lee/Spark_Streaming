# Base Python 3.7.10 image
FROM python:3.7.10

# Expose Port
EXPOSE 8888 4040

USER root

# Change shell to /bin/bash
SHELL ["/bin/bash", "-c"]

# Install OpenJDK
RUN apt-get update && \
    apt-get install -y  default-jdk && \
    apt-get clean;
    
# Fix certificate issues
RUN apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

# Insatall nano & vi
RUN apt-get install -y nano && \
    apt-get install -y vim;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Download and Setup Spark binaries
WORKDIR /tmp
RUN wget https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
RUN tar -xvf spark-3.3.0-bin-hadoop3.tgz
RUN mv spark-3.3.0-bin-hadoop3 spark
RUN mv spark /
RUN rm spark-3.3.0-bin-hadoop3.tgz

# Set up environment variables
ENV SPARK_HOME /spark
RUN export SPARK_HOME
ENV PYSPARK_PYTHON /usr/local/bin/python
RUN export PYSPARK_PYTHON
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip
RUN export PYTHONPATH
ENV PATH $PATH:$SPARK_HOME/bin
RUN export PATH

# Fix configuration files
RUN mv $SPARK_HOME/conf/log4j2.properties.template $SPARK_HOME/conf/log4j2.properties
RUN mv $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
# RUN mv $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
COPY spark-defaults.conf $SPARK_HOME/conf/
RUN bash $SPARK_HOME/sbin/start-history-server.sh

# Change to working directory and clone git repo
# Install Jupyter Lab, PySpark, Kafka, boto & Delta Lake
RUN mkdir -p /home/workspace
WORKDIR /home/workshop
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Install scala kernel
RUN pip install spylon-kernel && python3 -m spylon_kernel install

# Install Auto Complete
RUN pip install jupyter-tabnine --user
RUN jupyter nbextension install --py jupyter_tabnine --user
RUN jupyter nbextension enable --py jupyter_tabnine --user
RUN jupyter serverextension enable --py jupyter_tabnine --user
# RUN jupyter server extension enable --user --py jupyter_scheduler

# Clone Ease with Apache Spark Repo to Start
# RUN git clone https://github.com/subhamkharwal/ease-with-apache-spark.git
# RUN git clone https://github.com/subhamkharwal/ease-with-data.git
# RUN git clone https://github.com/subhamkharwal/pyspark-zero-to-hero.git

WORKDIR /home

# Fix Jupyter logging issue
RUN ipython profile create
RUN echo "c.IPKernelApp.capture_fd_output = False" >> "/root/.ipython/profile_default/ipython_kernel_config.py"

# Start the container with root privilages
CMD ["python3", "-m", "jupyterlab", "--ip", "0.0.0.0", "--allow-root"]