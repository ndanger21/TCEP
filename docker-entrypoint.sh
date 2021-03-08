#!/bin/bash
iperf3 -s -D
# ntpd complains about empty drift file otherwise
echo "0.000" >> /var/db/ntpd.drift
ntpd -s
chmod -R a+w /app/logs

# change maximum heapsize here if encountering OOM crashes (-Xmx1024m)
# enable garbage collection log
# enable heap dump on OOM exceptions, .phd files (look in container's /app folder if not showing up in /app/logs)
#  can be analyzed with IBM Memory Anaylzer Tool
# https://publib.boulder.ibm.com/httpserv/cookbook/Major_Tools-IBM_Memory_Analyzer_Tool.html#Major_Tools-IBM_Memory_Analyzer_Tool_MAT-Standalone_Installation
# set path for CPLEX library needed for CONTRAST
# enable remote profiling with visualvm when executing on localhost
java \
  -DlogFilePath=${LOG_FILE_PATH} \
  -Xverbosegclog:"/app/logs/jvm_gc.txt" \
  -DIBM_HEAPDUMP=true \
  -DIBM_HEAPDUMP_OUTOFMEMORY=true \
  -DIBM_HEAPDUMPDIR="/app/logs/" \
  -Djava.library.path="/app/cplex/" \
  -DCPLEX_LIB_PATH="/app/cplex/" \
  -Dcom.sun.management.jmxremote=true \
  -Dcom.sun.management.jmxremote.local.only=false \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Djava.rmi.server.hostname=localhost \
  -Dcom.sun.management.jmxremote.port=${JMX_PORT} \
  -Dcom.sun.management.jmxremote.rmi.port=${JMX_PORT} \
  -cp /app/tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar ${MAIN} ${ARGS}
