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
bin/hadoop jar /root/share/wikipedia-visitors.jar input/data outputSumX outputTotalX
```


With 3 pagecounts files:
- 1. job completed in 1m 45s
- 2. job completed in 31s

Get data to local
```
bin/hadoop fs -get outputSum6/* /root/share/data/sum
bin/hadoop fs -get outputTotal6/* /root/share/data/total
```

Obtain top N most visited pages
```
cd <local data folder>
cat total/part-* | sort -nr -k2 | head -50 | cut -f1 > top50pages.txt
```
- 2 seconds to generate file

Extract data for top N pages
```
perl extract-top-pageviews.pl
```

## Cloud environment

Upload jar and test set into S3.
```sh
aws s3 cp wikipedia-visitors.jar s3://lttr-hadoop-us/jar/
```

Test set contains 3 files from 9 days, about 2.2 GB

```sh
aws emr create-cluster
--name "lttr-wiki-test-2"
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m1.medium
                  InstanceGroupType=CORE,InstanceCount=2,InstanceType=m1.medium
--use-default-roles
--ec2-attributes KeyName=hadoop
--release-label emr-4.0.0
--applications Name=Hadoop
--steps Type=CUSTOM_JAR,
        Name="Wikipedia pagecounts",
        ActionOnFailure=CONTINUE,
        Jar=s3://lttr-hadoop-us/jar/wikipedia-visitors.jar,
        Args=["s3://lttr-hadoop-us/data/wiki-test","s3://lttr-hadoop-us/output/test2/sum","s3://lttr-hadoop-us/output/test2/total"]
--no-auto-terminate
--log-uri s3://lttr-hadoop-us/logs/test2
--enable-debugging
```

```sh
aws emr create-cluster --name "lttr-wiki-test-2" --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m1.medium InstanceGroupType=CORE,InstanceCount=2,InstanceType=m1.medium --use-default-roles --ec2-attributes KeyName=hadoop --release-label emr-4.0.0 --applications Name=Hadoop --steps Type=CUSTOM_JAR,Name="Wikipedia pagecounts",ActionOnFailure=CONTINUE,Jar=s3://lttr-hadoop-us/jar/wikipedia-visitors.jar,Args=["s3://lttr-hadoop-us/data/wiki-test","s3://lttr-hadoop-us/output/test2/sum","s3://lttr-hadoop-us/output/test2/total"] --no-auto-terminate --log-uri s3://lttr-hadoop-us/logs/test2 --enable-debugging
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

Job              | Duration
---              | --------
sumVisitorsByDay | 8mins, 33sec
sumTotalVisitors | 1mins, 39sec

```sh
aws emr terminate-clusters --cluster-id "j-21FARLELR682V"
```

Download results
```sh
aws s3 cp s3://lttr-hadoop-us/output/test2 . --recursive
aws s3 cp s3://lttr-hadoop-us/logs/test2 ./logs --recursive
```

Combine sum output parts
```sh
copy sum/part* sum/parts-combined
perl extract-top-pageviews.pl
```

## Main run

```sh
aws emr create-cluster
--name "lttr-wiki-main"
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m1.medium
                  InstanceGroupType=CORE,InstanceCount=20,InstanceType=m1.medium
--use-default-roles
--ec2-attributes KeyName=hadoop
--release-label emr-4.0.0
--applications Name=Hadoop
--steps Type=CUSTOM_JAR,
        Name="Wikipedia pagecounts",
        ActionOnFailure=CONTINUE,
        Jar=s3://lttr-hadoop-us/jar/wikipedia-visitors.jar,
        Args=["s3://lttr-hadoop-us/data/wiki-all","s3://lttr-hadoop-us/output/main/sum","s3://lttr-hadoop-us/output/main/total"]
--no-auto-terminate
--log-uri s3://lttr-hadoop-us/logs/main
--enable-debugging
```

```sh
aws emr create-cluster --name "lttr-wiki-main-2" --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m1.medium InstanceGroupType=CORE,InstanceCount=19,InstanceType=m1.medium --use-default-roles --ec2-attributes KeyName=hadoop --release-label emr-4.0.0 --applications Name=Hadoop --steps Type=CUSTOM_JAR,Name="Wikipedia pagecounts",ActionOnFailure=CONTINUE,Jar=s3://lttr-hadoop-us/jar/wikipedia-visitors.jar,Args=["s3://lttr-hadoop-us/data/wiki-all","s3://lttr-hadoop-us/output/main/sum","s3://lttr-hadoop-us/output/main/total"] --no-auto-terminate --log-uri s3://lttr-hadoop-us/logs/main --enable-debugging
```

