Notes about running pagecounts mapreduce job
============================================

## Main run with big-data

```
Number of master nodes: 	 1
Number of task nodes:   	 15
Number of applications: 	 3 (sub-jobs)
```

Net time: 

| phase     | duration           |
|-----------|--------------------|
| _sum_     | 2077 s             |
| _totals_  | 101 s              |
| _filling_ | 69 s               |
| total     | **2247 s (37:27)** |


### Sub-job 1 (sum by day phase)

```
Number of splits:      	 744
Job running at:        	 07:25:28
Job completed at:      	 08:00:05
Duration:              	 34:37 (2077 s)
Killed map tasks:      	 1
Killed reduce tasks:   	 1
Launched map tasks:    	 745
Launched reduce tasks: 	 32
```


### Sub-job 2 (total top n phase)

```
Number of splits:      	 31
Job running at:        	 08:00:07
Job completed at:      	 08:01:48
Duration:              	 01:41 (101 s)
Killed map tasks:      	 1
Killed reduce tasks:   	 0
Launched map tasks:    	 32
Launched reduce tasks: 	 1
```


### Sub-job 2 (top n by day phase)

```
Number of splits:      	 31
Job running at:        	 08:01:50 
Job completed at:      	 08:02:59
Duration:              	 01:09 (69 s)
Killed map tasks:      	 0
Killed reduce tasks:   	 0
Launched map tasks:    	 31
Launched reduce tasks: 	 1
```


## Run with large-data

```
Number of master nodes: 	 1
Number of task nodes:   	 4
Number of applications: 	 3 (sub-jobs)
```

Net time: 

| phase     | duration           |
|-----------|--------------------|
| _sum_     | 625 s              |
| _totals_  | 155 s              |
| _filling_ | 167 s              |
| total     | **947 s (15:47)**  |


### Sub-job 1 (sum by day phase)

```
Number of splits:      	 48
Job running at:        	 13:43:58 
Job completed at:      	 13:54:23 
Duration:              	 10:25 (625 s)
Killed map tasks:      	 1
Killed reduce tasks:   	 1
Launched map tasks:    	 49
Launched reduce tasks: 	 32
```


### Sub-job 2 (total top n phase)

```
Number of splits:      	 31
Job running at:        	 13:54:25 
Job completed at:      	 13:57:00 
Duration:              	 02:35 (155 s)
Killed map tasks:      	 0
Killed reduce tasks:   	 0
Launched map tasks:    	 31
Launched reduce tasks: 	 1
```


### Sub-job 2 (top n by day phase)

```
Number of splits:      	 31
Job running at:        	 13:57:01 
Job completed at:      	 13:59:48
Duration:              	 02:47 (167 s)
Killed map tasks:      	 1
Killed reduce tasks:   	 0
Launched map tasks:    	 32
Launched reduce tasks: 	 1
```

## Conclusion

There is about 16 times more data in big-data set then in large-data set. There 
is 4 times more task nodes in main run with big-data set.

> Main run with big-data should be about 16/4 = 4 times slower.

But it is

> 2247 s / 947 s = 2.37 times slower.

With bigger data set the MapReduce job is more efficient. The overhead is less 
significant.

## medium data

```
314 s (5mins, 14sec)
167 s (2mins, 47sec)
147 s (2mins, 28sec)
====
628 s
```
