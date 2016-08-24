#!/bin/bash

HOST_NAME=centos-vm.petersburg.epam.com
INPUT_JOB_PATH=job_input
OUTPUT_JOB_PATH=job_output
FILE_NAMES_ARRAY[0]=000000

cd $HADOOP_PREFIX

while [ -z `jps | grep NodeManager` ]
do
    echo "Waiting for NodeManager to start ..."
    sleep 3
done

echo "NodeManager started. Hadoop cluster initialized. Leaving NameNode from SafeMode state ..."
bin/hdfs dfsadmin -safemode leave
echo "NameNode leaved SafeMode state"

echo "Staring history server"
./sbin/mr-jobhistory-daemon.sh start historyserver
echo "History server started"

bin/hdfs dfs -mkdir hdfs://$HOSTNAME:9000/$INPUT_JOB_PATH
for n in "${FILE_NAMES_ARRAY[@]}"; do
    echo "Coping ${n} from local docker FS to HDFS ..."
    bin/hdfs dfs -copyFromLocal /opt/${n} hdfs://$HOSTNAME:9000/$INPUT_JOB_PATH/${n}
    echo "Coping ${n} from local docker FS to HDFS complete"
done

#This is needed cause by default docker container try to knock-knock to host my its hostname.domainname alias
#And fail in case docker container simply installed on same machine as MarkLogic DB
echo "Setting alias to hostname ${HOST_NAME} ..."
echo "10.16.9.112	centos-vm.petersburg.epam.com" >> /etc/hosts
echo "Setting alias to hostname ${HOST_NAME} complete "

export HADOOP_CLIENT_OPTS="-Xmx4g -Xmn1g -Xms4g $HADOOP_CLIENT_OPTS"
echo "Running a job ..."
bin/hadoop jar /opt/MarkLogicHadoopTest.jar ru.hokan.text.IpBytesCounter marklogic-connection-properties.xml hdfs://$HOSTNAME:9000/$INPUT_JOB_PATH/ hdfs://$HOSTNAME:9000/$OUTPUT_JOB_PATH
echo "Job has finished"