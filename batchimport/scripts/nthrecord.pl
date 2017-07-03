#!/usr/local/bin/perl
# Gets all records from after nth position
# Kai P, 120117
# TODO: generalize xml record and wrapper tagnames

$nth = $ARGV[0];
$type = $ARGV[1];
if ( $nth eq '' ) { die "Nth!"; }
if ( $type eq '' ) { die "Type!"; }
$i = 0;

if ( $type eq 'xml' ) { # xml should be in row format... REDO!?
	while ( <STDIN> ) {
		if ( /<\?xml/io ) { print STDOUT $_; next; }
		if ( /<\/?collection/io ) { print STDOUT $_; next; }
		if ( /<record(?:\s+type=\"bibliographic\")?>/io ) { $i++; }
		if ( $i >= $nth) { print STDOUT $_; }
	}
}
elsif ( $type eq 'elibxml' ) { # xml should be in row format... REDO!?
	while ( <STDIN> ) {
		if ( /<\?xml/io ) { print STDOUT $_; next; }
		if ( /<\/?Products/io ) { print STDOUT $_; next; }
		if ( /<Product>/io ) { $i++; }
		if ( $i >= $nth) { print STDOUT $_; }
	}
}
elsif ( $type eq 'iso' ) {
	$/=chr(0x1d); # input iso2709 record end delimiter
	$\=''; # no output iso2709 record end delimiter
	my $code='';
	while ( <STDIN> ) {
		$code=substr($_,6,1);
		if ( $code =~ /[acdefgijklmoprt]/o ) { $i++; }
		if ( $i >= $nth ) { print STDOUT $_; }
	}
}
else {
	die "Type $type is not supported.";
}
