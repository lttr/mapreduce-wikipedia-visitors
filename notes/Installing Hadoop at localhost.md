Installing Hadoop
=================

Sources 
-------

[Downloads and docs](http://hadoop.apache.org/)

### Default configuration values 

-   [core-default.xml](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/core-default.xml)
-   [hdfs-default.xml](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml)
-   [mapred-default.xml](http://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml)
-   [yarn-default.xml](http://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-common/yarn-default.xml)

Single node cluster setup 
-------------------------

*For Ubuntu based distribution.*

### Download Hadoop 

Download current version from [Apache Hadoop Releases](http://hadoop.apache.org/releases.html)

Current version is `2.7.1` (2015-08-18).

### Install Hadoop 

#### Java

``` 
sudo apt-get install openjdk-7-jdk -y
```

#### Untar 

``` 
mkdir ~/hadoop
cd ~/hadoop
tar -xf /vagrant/installators/hadoop-2.7.1.tar.gz
```

#### Add to .bashrc or similar 

``` 
# Hadoop
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
export HADOOP_HOME=/home/vagrant/hadoop/hadoop-2.7.1
export PATH=$HADOOP_HOME/bin:$PATH
export PATH=$JAVA_HOME/bin:$PATH
export HADOOP_CLASSPATH=$/lib/tools.jar
```

#### Alter java home in hadoop-env.sh conf file 

``` 
vi $HADOOP_HOME/etc/hadoop/hadoop-env.sh
# insert
export JAVA_HOME=${JAVA_HOME}
# or explicitly
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
```

#### Set up ssh 

``` 
mkdir -p ~/.ssh
cd ~/.ssh
ssh-keygen # and Enter, Enter... no password and defaults
cp id_rsa.pub authorized_keys # authorize myself
ssh localhost # should work
```

### Run example job

#### Calculate PI (no input required)

``` 
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.1.jar pi 4 100
```

Pseudo-distributed mode setup 
-----------------------------

### Prepare directory for HDFS data 

``` 
cd ~/hadoop
mkdir data
sudo chown 777 data
```

### Add configuration properties 

``` 
vi $HADOOP_HOME/etc/hadoop/core-site.xml
# add:
  <property>
    <name>fs.default.name</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/home/vagrant/hadoop/data</value>
  </property>
```

``` 
vi $HADOOP_HOME/etc/hadoop/hdfs-site.xml
# add:
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
```

``` 
cp $HADOOP_HOME/etc/hadoop/mapred-site.xml.template $HADOOP_HOME/etc/hadoop/mapred-site.xml
vi $HADOOP_HOME/etc/hadoop/mapred-site.xml
# add:
  <property>
    <name>mapred.job.tracker</name>
    <value>localhost:9001</value>
  </property>
```

### Format namenode 

``` 
hdfs namenode -format
```

### Start application 

``` 
./sbin/start-dfs.sh
./sbin/start-yarn.sh
jps
# Output should be like:
# 24050 NameNode
# 24828 Jps
# 24410 SecondaryNameNode
# 24185 DataNode
# 24590 ResourceManager
# 24725 NodeManager
```

### Prepare data 

``` 
cd $HADOOP_HOME
# use bin/hadoop fs... or hdfs dfs...
hdfs dfs -ls / # list nothing (but also no error), hdfs was formatted
echo "This is a test." > test.txt
hdfs dfs -mkdir /data
hdfs dfs -mkdir /data/in
hdfs dfs -put test.txt /data/in
```

### Run example wordcount job 

``` 
hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.1.jar wordcount /data/in /data/out
hdfs dfs -cat /data/out/part-r-00000
# Output should be like:
# This   1
# a      1
# is     1
# test.  1
```

### Run your own code 

#### Create example .java file 

Code from official [Hadoop tutorial](http://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0).

``` 
# copy paste or edit the code
vi WordCount.java
```

#### Compile properly 

``` 
javac -classpath `yarn classpath` WordCount.java
jar cvf wc.jar WordCount*.class
```

#### Clean up 

``` 
hdfs dfs -rm -r /data/out
```

#### Run 

``` 
hadoop jar wc.jar WordCount /data/in/test.txt /data/out
```

#### Get output 

``` 
hdfs dfs -get /data/out/part-r-00000 output/
```

