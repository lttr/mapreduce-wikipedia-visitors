@REM (Unix utils are used when possible)

@REM Create combined files from output from reducers
cat sum/part-* > sum/combined.txt
cat total/part-* > total/combined.txt
@REM Obtain top 50 pages acording to view counts (column 2)
cat total/combined.txt | sort -nr -k2 | head -50 > top50pages-with-counts.txt
cat top50pages-with-counts.txt | cut -f1 > top50pages.txt
@REM Extract pageviews for top ones
perl extract-top-pageviews.pl
