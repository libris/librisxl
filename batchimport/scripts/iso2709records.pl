#!/usr/local/bin/perl
# counts iso2709 bib records
# Kai P, 120117

$/=chr(0x1d); # input iso2709 record end delimiter
$code='';
$i = 0;

while ( <STDIN> ) {
	$code=substr($_,6,1);
	if ( $code =~ /[acdefgijklmoprt]/o ) { $i++; } # Yes, bibliographic
}
print STDOUT $i;
