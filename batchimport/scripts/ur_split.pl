#!/bin/perl
# 100311 Kai P, UR file splitter

my $nsplit=5000;
my $i=0;
my $c=0;
my $buffer='';
my $head="<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<manifest xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" identifier=\"root_manifest\" version=\"1.1.4\" xmlns=\"http://www.imsglobal.org/xsd/imscp_v1p1\">\n\t<organizations />\n\t<resources />\n";
my $tail="</manifest>";

open INFILE, "<$ARGV[0]" or die "No such infile!";

while ( <INFILE> ) {
	if ( /<manifest identifier="product_/io ) {
		$buffer=$_;
		if ( $i == 0 ) {
			open OUTFILE, ">$ARGV[0].$c" or die "Couldn't open OUTFILE $ARGV[0].$c";
			print OUTFILE $head;
		}
	}
	elsif ( /<\/manifest>/io ) {
		if ( $buffer ne '' ) {
			$buffer.=$_;
			print OUTFILE $buffer;	
			$i++;
			if ( $i == $nsplit ) {
				print OUTFILE $tail;
				close OUTFILE;
				$c++;
				$i=0;
			}			
		$buffer='';
		}
	}
	else {
		if ( $buffer ne '' )  { $buffer.=$_; }
	}
}

if ( OUTFILE ) {
	print OUTFILE $tail;
	close OUTFILE;
}
close INFILE;
