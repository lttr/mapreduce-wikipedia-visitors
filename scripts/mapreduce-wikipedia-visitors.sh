cat pc1 pc2 | gawk '$1=="cs" && length($2)<100 {a[$2]+=$3} END{for (i in a) print i,a[i]}' | sort -r -n -k 2 | he
ad -50 | urldecode > results
