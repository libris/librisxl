#!/usr/local/bin/perl

use strict;
use warnings;
use Digest::SHA qw(sha512_hex);
use JSON::XS;
use LWP;
use HTML::Entities qw(encode_entities_numeric);

chdir "/appl/import2/queues/elib";

my $last_import_date='./prev/last_import_date';
#my $serviceid='1800';
#my $servicekey='qZU28CVNe5oulsQ';
my $serviceid='1863';
#my $serviceid='1845';
my $servicekey='2gCbnoVXuSONlyjkzvhTPwmsfUqpGFHeIDcZEYdJtQ63781r9i';
my $from='';

my $pid = $ARGV[0];

eval {
	open LAST_IMPORT, "<$last_import_date";
	$from=<LAST_IMPORT>;
	chomp $from;
	close LAST_IMPORT;
	die if ( $from !~ /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/o );
};
if ( $@ ) {
	error("elib: $last_import_date failed.");
}

my $libraryid = $pid;

#my $checksum=sha512_hex($serviceid.$from.$servicekey);
my $checksum=sha512_hex($serviceid.$libraryid.$servicekey);
#my $checksum=sha512_hex($serviceid.$pid.$servicekey);

#my $url = "https://webservices.elib.se/librarymanagement/v1.0/products?serviceid=$serviceid&from=$from&checksum=$checksum&format=json";
#my $url = "https://webservices.elib.se/librarymanagement/v1.0/products/$pid?serviceid=$serviceid&from=$from&checksum=$checksum&format=json";

#my $url = "https://webservices.elib.se/librarymanagement/v1.0/libraryproducts?serviceid=$serviceid&libraryid=$libraryid&from=$from&checksum=$checksum&format=json";
my $url = "https://webservices.elib.se/librarymanagement/v1.0/libraryproducts/$libraryid?serviceid=$serviceid&checksum=$checksum&format=json";

# get current date ( to next last_import_date ) before import!
my $new_from = `date "+%Y-%m-%dT%H:%M"`;
chomp $new_from;
error("elib: new_from = $new_from") if ( $new_from !~ /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/o );

print $url, "\n";

my $browser = LWP::UserAgent->new;
$browser->timeout(30);
my $response = $browser->get($url);
if ( ! $response->is_success ) {
	print $response->status_line . "\n";
	print $response->content, "\n";
	exit;
	error("elib: get failed.");
}

print $response->content, "\n";
exit;

my $data;

eval {
	#my $json_text = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($content);

	# an incremental jsondecode would be preferred if data is very large
	my $json = JSON::XS->new->utf8;
	$data = $json->decode($response->content);
};
if ( $@ ) {
	error("elib: json parse failed.");
}

eval {
	open(my $fh, ">:encoding(UTF-8)", "./elib.xml");
	print $fh "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
	print $fh "<Products>\n";

	for my $product ( @{$data->{'Products'}} ) {
		print $fh perl2xml($product, 'Product');
	}

	print $fh "</Products>\n";

};
if ( $@ ) {
	error("elib: write of ./elib.xml failed.");
}

exit; # remove when really done

# store $new_from to $last_import_date
eval {
	open LAST_IMPORT, ">$last_import_date";
	print LAST_IMPORT $new_from;
	close LAST_IMPORT;
};
if ( $@ ) {
	error("elib: write of $last_import_date failed.");
}

# Finally, mv to ./incoming
system("mv ./elib.xml ./incoming") or error("elib: mv of ./elib.xml failed.");

# perl struct to xml
sub perl2xml {
	my ($d,$p) = @_;
	my $t;
	my $result = '';
	
	$t = ref($d);

	$result = "<$p>\n";

	if ( $t eq 'HASH' ) {
		for my $k ( keys %$d ) {
			$result .= perl2xml($d->{$k}, $k);
		}
	}
	elsif ( $t eq 'ARRAY' ) {
		for my $i ( @$d ) {
			$result .= perl2xml($i, "$p-item");
		}
	}
	else {
		if ( $d ne '' ) {
			$result .= encode_entities_numeric("$d") . "\n";
		}
	}

	$result .= "</$p>\n";

	return $result;
}

sub error {
	my $err = shift @_;

	system("mailx -s '$err' kai.poykio\@kb.se </dev/null");
	die "$err";
}
