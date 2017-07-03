#!/usr/bin/perl
# Ebsco treatment.

while ( <> ) {
        if ( /<record/io ) {

                print &get_value(\$_, '022', 'a'), '=', &get_value(\$_, '022', 'y'),"\n";
                print &get_value(\$_, '245', 'a'), "\n";
                #print &get_value(\$_, 'leader', ''), "\n";
                #print substr(&get_value(\$_, 'leader', ''), 7, 1), "\n";

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
