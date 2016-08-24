To run this example you will need:

MarkLogic developer CentOS 6.8 virtual machine (from here: https://mlu.marklogic.com/downloads/centos_64_fundamentals.ova)
Installed docker ON THE SAME Virtual Machine
Docker/Hadoop container from sequenceiq group (from here:  https://hub.docker.com/r/sequenceiq/hadoop-docker/)
NOTE: be ware it doesn't support native Hadoop compression codecs like Snappy (see Issues on corresponding github)

Then just change ip of your virtual host in execute-job.sh and pom.xml files
Then just run with
mvn clean install -Pdocker