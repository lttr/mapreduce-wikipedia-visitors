# Wikipedia visitors statistics using Hadoop

This project contains Hadoop MapReduce job and accompanying scripts for 
aggregating statistics about Wikipedia visitors. This project is part of 
bachelor thesis _Parallel computing using MapReduce model_.

### Project structure

- `data` structure of folders for input data
- `mapreduce-job` MapReduce job
- `notes` notes about running scripts and MapReduce job
- `scripts` bash scripts which process the data similar as MapReduce job
- `Vagrantfile` definition of testing machine


### Get this project

```
git clone https://github.com/lttr/mapreduce-wikipedia-visitors
```

### Build jar archive with MapReduce job

```
cd mapreduce-job
mvn install
```

### Download data

```
wget --convert-links http://dumps.wikimedia.org/other/pagecounts-raw/2015/2015-07 -O index.html
cat index.html | sed -n "/href/ s/.*href=\"\(.*\)\".*/\1/gp" | grep "gz$" > urls.txt
wget -i urls.txt
```

### Run script with small data set

```
cd scripts
./mapreduce-wikipedia-visitors.sh ../data/small-data/*
```

### Start testing machine

```
vagrant up
vagrant ssh
```
