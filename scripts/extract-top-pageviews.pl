#!/usr/bin/env perl

#
# Generates file for every page from input list.
# Content of these files is based on file
# with page count for every page and date.
#

use strict;
use warnings;
use Encode 'encode';

# 
# Constants
#
my $topPagesFile  = "top50pages.txt";
my $pageviewsFile = "sum/combined.txt";
my $pagesDataDir  = "pages-data";

# 
# Clean the output directory
#
unlink(glob($pagesDataDir . "/*"));
rmdir($pagesDataDir);
unless (-e $pagesDataDir or mkdir $pagesDataDir) {
    die "Unable to create $pagesDataDir directory. \n";
}

#
# Obtain input list of page name
#
my @topPages = read_file_by_lines($topPagesFile);

# 
# Load pagecounts
#
my %pagecounts;
my $key = "";
my $day = "";
open (my $fhi, "<:encoding(utf8)", $pageviewsFile) or die "Can't read file! $!";
while (my $line = <$fhi>) {
    chomp $line;
    foreach my $page (@topPages) {
	my @parts = split /\t/, $line;
	# check if page name from input list equals page name in pageviews file
	if ($parts[0] eq $page) {
	    $day = substr($parts[1], 6, 2);
	    $key = $parts[0] . "@" . $day;
	    if (exists $pagecounts{$key}) {
		$pagecounts{$key} += $parts[2];
	    } else {
		$pagecounts{$key} = $parts[2];
	    }
	}
    }
}
close ($fhi);

# Debug print
# foreach my $key (sort keys %pagecounts) {
#     print $key . ' '. $pagecounts{$key} . "\n";
# }

#
# Generate files for the list of page names
#
foreach my $key (sort keys %pagecounts) {
    my @keyParts = split('@', $key);
    my $page = $keyParts[0];
    my $day = $keyParts[1];
    # create file or append to file with sane name
    $page = unaccent($page);
    my $outputFileName = $pagesDataDir . '/' . $page . '.txt';
    open (my $fho, '>>:encoding(utf8)', encode('cp1250', $outputFileName));

    # format line like: <page-name> <day-number> <page-count>
    print {$fho} $page . ' ' . $day . ' ' . $pagecounts{$key} . "\n";
    close ($fho);
}

#
# Removes czech accents from text.
#
sub unaccent {
    my $text = shift;
    $text =~ tr/áčďéěíňóřšťúůýžÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ/acdeeinorstuuzACDEEINORSTUUZ/;
    return $text;
}

#
# Reads a text file and returns its content as a list of lines.
#
sub read_file_by_lines {
    my @lines;
    open (my $fh, "<:encoding(utf8)", @_) or die "Can't read file! $!";
    while (my $line = <$fh>) {
        chomp $line;
        push @lines, $line;
    }
    close ($fh);
    return @lines;
}
