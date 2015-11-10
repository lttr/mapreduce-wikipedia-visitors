Notes about running pagecounts script
=====================================

### Input data

[Index of page view statistics for 2015-07](http://dumps.wikimedia.org/other/pagecounts-raw/2015/2015-07/)

Download using _wget_
```
wget --convert-links http://dumps.wikimedia.org/other/pagecounts-raw/2015/2015-07 -O index.html
" on Windows
cat index.html | sed -n "/href/ s#.*href=\"\(.*\)\".*#http://dumps.wikimedia.org/other/pagecounts-raw/2015/2015-07/\1#gp" | grep "gz$" > urls.txt
# on Amazon Linux:
cat index.html | sed -n "/href/ s/.*href=\"\(.*\)\".*/\1/gp" | grep "gz$" > urls.txt
wget -i urls.txt
```

_All sizes are for input data compressed in `gz` format._

**Micro data** 
4 files from 2 days (for fast tests) - 8 kB.

**Small data** 
Also 4 files from 2 days but originals - 320 MB.

**Medium data** 
16 files from 4 days - 1,1 GB. Roughly 4 times more data then in _Small_ data 
set.

**Double-medium data** 
The medium data, but twice the same - 2,2 GB.

**Large data**
48 files in 2 full days - 3,8 GB.

**Big data** 
Whole set of pagecounts for 1 month (7/2015) has 65 GB in 744 files (31 days x 24 hours).

### Running the script

At first the script creates sums of visits for pages in every day of input data. 
I call this phase _sum_ phase.

Then the script calculates _totals_ for every page and picks up the top 20. This 
phase is quick because data is already prepared for it in sum phase.

The third phase creates file for every page from top 20 and fills it with page 
visits for every day. This is _filling_ phase.

Running the script with _Micro data_ takes less then 1 second. Other data sets 
are more interesting (times in seconds).

On 3 GHz 2 cores processor, 2 GB of RAM (inside Linux virtual machine):

| _phase_   | **Small** | **Medium**  | **Double-medium** | **Large**    | **Big** _estimation_ |
|-----------|-----------|-------------|-------------------|--------------|----------------------|
|           |           | 4 x _Small_ | 2 x _Medium_      | 3 x _Medium_ | 46 x _Medium_        |
| _sum_     | 19 s      | 68 s        | 132 s             | 217 s        | 3100 s               |
| _totals_  | 1 s       | 1 s         | 2 s               | 2 s          | 15 s                 |
| _filling_ | 27 s      | 66 s        | 67 s              | 127 s        | 1300 s               |
|           | **46 s**  | **134 s**   | **198 s**         | **344 s**    | **4415 s**           |

On 2 GHz 1 core processor, 4 GB of RAM (Amazon m1.medium instance):

| _phase_   | **Small** | **Medium**  | **Double-medium** | **Large**    | **Big** _estimation_ | **Big** _reality_     |
|-----------|-----------|-------------|-------------------|--------------|----------------------|-----------------------|
|           |           | 4 x _Small_ | 2 x _Medium_      | 3 x _Medium_ | 46 x _Medium_        | 46 x _Medium_         |
| _sum_     | 35 s      | 133 s       | 272 s             | 443 s        | 6700 s               | 12728 s (03:32:8)     | 
| _totals_  | 2 s       | 3 s         | 3 s               | 5 s          | 20 s                 | 38 s                  |
| _filling_ | 13 s      | 30 s        | 31 s              | 60 s         | 600 s                | 1009 s (16:49)        |
|           | **50 s**  | **166 s**   | **306 s**         | **508 s**    | **7320 s**           | **13775 s** (3:49:35) |

The _sum_ phase is growing linearly with the size of input data. The _filling_ 
phase is growing slower because it depends on the size of data generated in 
previous phases which is not growing as much. When the input data is duplicated 
(for _Double-medium_ data set) there are no additional data generated after 
second phase and therefore the _filling_ phase took the same time as with 
_Medium_ data set. The durations for _Large_ data set correspondes with previous 
ideas.

The script was not run for _Big_ data, the durations are estimated.

### Conclusion

The problem can be solved on a single machine although it would most likely take 
more then hour to finish. The input data are processed for every day separately. 
This means that the task is ideal for parallel processing. In the same time it 
means that the task can be solved relatively efectively on a single machine, 
because the data can fit in memory (there is about 2 GB of data for one day). 

Because of the overhead of running the task in parallel is high, it may not be 
the best solution to use Hadoop for processing single month of data (about 60 
GB). However bigger amount of data would take unreasonable time to finish on 
a single machine.
