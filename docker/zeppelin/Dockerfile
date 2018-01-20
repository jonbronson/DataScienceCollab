FROM apache/zeppelin:0.7.3


RUN apt-get update 
RUN apt-get install -y python3-pip python3-dev 
RUN cd /usr/local/bin && ln -s /usr/bin/python3 python 
RUN pip3 install --upgrade pip

RUN pip install pandas
RUN pip3 install pandas

RUN export PYSPARK_PYTHON=python3
RUN export PYSPARK_DRIVER_PYTHON=python3

