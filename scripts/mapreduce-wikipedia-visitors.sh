# Takes input files (Wikipedia pagecounts) as arguments,
# splits them into groups by day and process them

# stopwatch
stopwatch_initial=$(date +%s)

# Prepare directories
OUTPUT_DIR=../output/scripts
PAGES_DIR=$OUTPUT_DIR/pages-data
SUM_DIR=$OUTPUT_DIR/sum
TOTAL_DIR=$OUTPUT_DIR/total

rm -rf $OUTPUT_DIR/*
mkdir -p $PAGES_DIR $SUM_DIR $TOTAL_DIR

# Group input files by key (date in file name)
declare -A input_files
for file in $* 
do
	KEY=$(echo $file | awk -F[/-] '{print $(NF-1)}')
	input_files[$KEY]+=" "$file
done

# Calculate sums (subtotals) of page counts for every day
for key in ${!input_files[@]}
do
	# ungzip all input files to stdout
	gunzip -c ${input_files[$key]} |
	# sum all lines starting with "cs" and not too long
	# finally print the associative array
	awk '
		$1=="cs" && length($2)<120 && NF>3 && $2 !~ /:/ { a[$2] += $3 }
		END { for (i in a) print i,a[i] }
	' |
	# decode url encoding
	# python -c "import sys, urllib as ul; print ul.unquote_plus(sys.stdin.read())" |
	sort \
	> $SUM_DIR/$key
done

# stopwatch
stopwatch_sums=$(date +%s)
printf "%-25s %s\n" "Sums calculated in " $(expr $stopwatch_sums - $stopwatch_initial)

# Calculate top 20 pages total
cat $SUM_DIR/* |
awk ' { a[$1] += $2 } END { for (i in a) print i,a[i] } ' |
sort -r -n -k 2 |
head -20 \
> $TOTAL_DIR/total

# stopwatch
stopwatch_totals=$(date +%s)
printf "%-25s %s\n" "Totals calculated in " $(expr $stopwatch_totals - $stopwatch_sums)

# Fill files with chronological page counts for top pages
declare -a top_pages=($(awk '{print $1}' $TOTAL_DIR/total | sort))
for day_file in $SUM_DIR/* # for every day
do
	top_pages_counter=1
	day_number=${day_file: -2}

	while read line # for every page and its sum of its counts in the day
	do
		line_array=($line)
		page_name=${line_array[0]}
		page_count=${line_array[1]}
		top_page=${top_pages[$top_pages_counter]}
		# test if the page is in top pages
		if [ "$top_page" = "$page_name" ] 
		then
			# append the page count to appropriate file
			echo $day_number" "$page_count >> $PAGES_DIR/$page_name
			((top_pages_counter+=1)) # move to next top page
		fi
	done < $day_file
done

# stopwatch
stopwatch_output=$(date +%s)
printf "%-25s %s\n" "Output files filled in " $(expr $stopwatch_output - $stopwatch_sums)

# stopwatch
echo "----------------------------"
printf "%-25s %s\n" "Script finished in " $(expr $stopwatch_output - $stopwatch_initial)
