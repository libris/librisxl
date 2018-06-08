#!/bin/perl
# KP 150513

if ( ! -f $ARGV[0] ) {
	die "usage: ./filefilter.pl exclude-file";
}

%exfiles = ();

open EXF, "<$ARGV[0]";

while ( <EXF> ) {
	$exfiles{$_} = 1;
}

close EXF;

while ( <STDIN> ) {
	if ( ! defined $exfiles{$_} ) {
		print $_;
	}
}
