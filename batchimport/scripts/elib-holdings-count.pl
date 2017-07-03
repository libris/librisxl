#!/usr/local/bin/perl

%tot=();

while (<>) {

my ($p,$h) = split /:/;

if ( $h ne '' ) {

my @holds = split /;/, $h;

for my $i ( @holds ) {
	my ($sigel) = $i =~ /(.*?)=/o; 
	if ( $sigel ne '' ) {
		$tot{$sigel}++;
		#print $sigel, "\n";
	}
}

}
}

for my $k ( sort keys %tot ) {
	print $k, ' ', $tot{$k}, "\n";
}
