FROM ibmjava:8-sfj-alpine
#FROM ibmjava:8-sdk-alpine
# install zulu openjdk11-jre for alpine
#RUN mkdir /usr/lib/jvm
#WORKDIR /usr/lib/jvm
#ARG version=zulu11.31.11-ca-jre11.0.3-linux_musl_x64
#ARG openjdk_file=${version}.tar.gz
#RUN wget -O /usr/lib/jvm/${openjdk_file} https://cdn.azul.com/zulu/bin/${openjdk_file} && \
#    tar -xzvf ${openjdk_file} && \
#    rm -f ${openjdk_file}
#ENV PATH=/usr/lib/jvm/${version}/bin:$PATH

WORKDIR /app

RUN apk update && \
    apk upgrade && \
    apk add net-tools && \
    apk add iputils && \
    apk add iproute2 && \
    apk add openntpd && \
    apk add libstdc++ && \
    apk add tcptraceroute && \
    apk add iperf3 && \
    apk add bash && \
    apk add iw && \
    apk add wpa_supplicant && \
    apk add wireless-tools && \
    apk add ethtool && \
    apk add openvswitch

# two entries: 1. nserver for docker network (used in cluster setup) 2. ip for mininet setup
# make sure the ntpd container is started before all others so it gets this ip
RUN truncate -s 0 /etc/ntpd.conf && \
    #echo "server nserver" >> /etc/ntpd.conf && \
    echo "server 172.17.0.2" >> /etc/ntpd.conf

# add cplex for CONTRAST mapek; add before jarfile to avoid re-sending .so file every time the jarfile changes
RUN mkdir /app/cplex
ENV CPLEX_LIB_PATH "/app/cplex/"
ENV JMX_PORT 8484
COPY ./libcplex1280.so /app/cplex/
COPY ./cplex.jar /app/cplex/
COPY docker-entrypoint.sh /app/
COPY mobility_traces/ /app/mobility_traces/
RUN chmod +x /app/docker-entrypoint.sh

COPY tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar /app/

CMD ./docker-entrypoint.sh