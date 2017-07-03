#!/usr/local/bin/perl

use JSON::XS;

my $json = JSON::XS->new->utf8;

chdir "/appl/import2/queues/elib";

local $/; # slurp
open(my $fh, '<:encoding(UTF-8)', './prev/elib.json');
my $prev_json = <$fh>;
$prev_products = $json->decode($prev_json);	
close $fh;

for my $p ( @{$prev_products->{'Products'}} ) {
	print $p->{ProductId}, ":";
	for my $s ( @{$p->{'Holdings'}} ) {
		print $s->{Sigel}, "=", $s->{Status}, ";";
	}
	print "\n";
}
