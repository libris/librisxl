#!/usr/bin/perl
# Ebsco treatment.
# Kai P. 101101

while ( <> ) {
	if ( /<record/io ) {

		# Filter if:
		# leader,7 != s
		# ! 022a|022y 
		# ! 245a
		next if ( substr(&get_value(\$_, 'leader', ''), 7, 1) ne 's' );
		$a022=&get_value(\$_, '022', 'a');
		$y022=&get_value(\$_, '022', 'y');
		#print $a022, ' ', $y022, "\n";
		next if (( $a022 eq '' ) && ( ($y022 eq '') || ($y022 eq 'Pending') ));
		next if ( &get_value(\$_, '245', 'a') eq '' );

		# Prettyprint
		s/^\s*//o;
		s/></>\n</go;

		# Remove ??
		s/\s*\(?\s*\?{2,}\s*\)?\s*//gso; # catches nearly all
		s/\s*\(\s*\w*?\?\s*\)?\s*//gso; # rm string? in ()
		s/>\s*=\s*/>/gso; # rm =, wouldn't work in first regexp!
		s/\s*=\s*</</gso; # rm =, wouldn't work in first regexp!
		s/==\s*|=\s+/=/gso; # rm =, wouldn't work in first regexp!

		# dbg  statements
		#print &get_value(\$_, '022', 'a'), '=', &get_value(\$_, '022', 'y'),"\n";
		#print &get_value(\$_, '245', 'a'), "\n";
		#print &get_value(\$_, 'leader', ''), "\n";
		#print substr(&get_value(\$_, 'leader', ''), 7, 1), "\n";

		print;

	}	
	else {
		print;
	}
}

sub get_value() {
my $string = shift @_;
my $field = shift @_;
my $subfield = shift @_;
my $value = '';

if ( $field ne '' ) {
	if ( $subfield ne '' ) {	
		($value) = $$string =~ /tag=\"$field\"(.*?)<\/datafield/si;
                ($value) = $value =~ /code=\"$subfield\".*?>(.*?)</si;
	}
	else {
		($value) = $$string =~ /<$field.*?>(.*?)<\//si;
	}
}

return $value;
}
