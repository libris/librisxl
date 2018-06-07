#!/bin/perl
# Counts records that have been loaded according to batchimport log
# Kai P, 120117

$r=0;
$m=0;

while (<STDIN>){
	if ( /^mul-bib:/o ) { $m++; next; }
	elsif ( /^...-bib:/o) { $r+=1+$m; $m=0; }
}
print STDOUT $r;
