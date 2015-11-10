Running wikipedia pagecounts job
================================

## Local environment

Build project and create jar file
```
cd <project home>
mvn clean install
```

Put data into HDFS and run the job
```
bin/hadoop -put <local data folder> <hdfs data folder>
bin/hadoop jar  bin/hadoop jar /vagrant/mapreduce-job/target/wikipedia-visitors-1.0-SNAPSHOT.jar /input/micro-data /output
```

Get data to local
```
bin/hadoop fs -get /output/* /vagrant/output/mapreduce-job/
```

## Cloud environment

Upload jar and test set into S3. 70 GBs takes a few tens of minutes to upload 
from EC2 m1.medium instance to s3.
```sh
aws s3 cp wikipedia-visitors.jar s3://lttr-hadoop-us/jar/
aws s3 cp big-data/ s3://lttr-hadoop-us/input/big-data/
```

Run EMR cluster with 16 task nodes
```sh
aws emr create-cluster
--name "lttr-wiki-main-run"
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m1.medium
                  InstanceGroupType=CORE,InstanceCount=16,InstanceType=m1.medium
--use-default-roles
--ec2-attributes KeyName=hadoop
--release-label emr-4.0.0
--applications Name=Hadoop
--steps Type=CUSTOM_JAR,
        Name="Wikipedia pagecounts",
        ActionOnFailure=CONTINUE,
        Jar=s3://lttr-hadoop-us/jar/wikipedia-visitors.jar,
        Args=["s3://lttr-hadoop-us/input/big-data/","s3://lttr-hadoop-us/output/"]
--no-auto-terminate
--log-uri s3://lttr-hadoop-us/logs/main-run/
--enable-debugging
```

Needs to be on one line on Windows:
```sh
aws emr create-cluster --name "lttr-wiki-main-run" --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m1.medium InstanceGroupType=CORE,InstanceCount=16,InstanceType=m1.medium --use-default-roles --ec2-attributes KeyName=hadoop --release-label emr-4.0.0 --applications Name=Hadoop --steps Type=CUSTOM_JAR,Name="Wikipedia pagecounts",ActionOnFailure=CONTINUE,Jar=s3://lttr-hadoop-us/jar/wikipedia-visitors.jar,Args=["s3://lttr-hadoop-us/input/big-data/","s3://lttr-hadoop-us/output/"] --no-auto-terminate --log-uri s3://lttr-hadoop-us/logs/main-run/ --enable-debugging
```

Checking status
```sh
aws emr describe-cluster --cluster-id "j-21FARLELR682V" | jq ".Cluster.Status.State"
```

[emr-ssh-tunnel.html](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-ssh-tunnel.html)
```sh
aws emr socks --cluster-id "j-2ESTC1U356MGN" --key-pair-file "c:\Users\Lukas\OneDrive\conf\aws\hadoop.pem"
```

Go to GUI of Resource manager
```sh
aws emr describe-cluster --cluster-id "j-21FARLELR682V" | jq ".Cluster.MasterPublicDnsName"
```
Go to URL: `<MasterPublicDnsName>:9026`

```sh
aws emr terminate-clusters --cluster-id "j-21FARLELR682V"
```

Download results
```sh
aws s3 cp s3://lttr-hadoop-us/output/ . --recursive
aws s3 cp s3://lttr-hadoop-us/logs/ . --recursive
```
