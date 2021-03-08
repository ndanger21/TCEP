FROM containernet/containernet

RUN mkdir /containernet/tcep
COPY containernet_entrypoint.sh /containernet/tcep/containernet_entrypoint.sh
RUN apt-get update
#RUN apt-get upgrade -y
# additional libraries and files
#RUN pip install --upgrade pip
RUN apt-get install python3-pip
# ===========================================
# install pandas for handling the trace files and apscheduler for scheduling tasks during simulation
RUN pip3 install pandas apscheduler
# add modified version of mobility.py from mininet examples
COPY mobility.py /containernet/tcep/mobility.py
WORKDIR /containernet/tcep

ENTRYPOINT ["/containernet/tcep/containernet_entrypoint.sh"]
