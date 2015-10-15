# ungzip all input files to stdout
gunzip -c ../data/${1}-data/pagecounts*.gz |
# sum all lines starting with "cs" and not too long
# finally print the associative array
awk '
	$1=="cs" && length($2)<100 {a[$2]+=$3}
	END { for (i in a) print i,a[i] }
' |
# sort lines by second column (number values) descendantly
sort -r -n -k 2 |
# top 20 is enough
head -20 |
# decode url encoding
python -c "import sys, urllib as ul; print ul.unquote_plus(sys.stdin.read())"
