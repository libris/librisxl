#!/usr/local/bin/perl
# Elib import handling (total rewrite due to a new api).
# v2.2 Kai P

# Synopsis:
# 1. Get all 'Products' since 2001-01-01
# 2. For all LibraryIds, get LibraryProducts ('holdings')
# 2.1 Apply all sigels mapped from LibraryId to each 'Product' as 'holdings'
# 3. Do a diff on new and prev

#use strict;
#use warnings;
use Digest::SHA qw(sha512_hex);
use JSON::XS;
use LWP;
use HTML::Entities qw(encode_entities_numeric);
use File::Copy;
use Data::Dumper;
use Time::Piece;

chdir "/appl/import2/queues/elib";

my $from='2001-01-01';
my $serviceid='1863';
#my $libraryid='1845';
my $servicekey='2gCbnoVXuSONlyjkzvhTPwmsfUqpGFHeIDcZEYdJtQ63781r9i';

# $config = { serviceid: { sigels: [...], remark: , url_prefix: } ...}

my %holdings = ();

my $browser = LWP::UserAgent->new;
$browser->timeout(60);
my $json = JSON::XS->new->utf8;
#my $json = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($content);

my $config;
my $products;
my $prev_products;
my $response;
my $checksum;
my $url;
my $D = {}; # global diff struct

my $now = Time::Piece->new;

# Get config
if ( -f '/appl/import2/etc/elib.conf' ) {
	eval {
		local $/; # slurp
		open(my $fh, '<:encoding(UTF-8)', '/appl/import2/etc/elib.conf');
		my $conf = <$fh>;
		$config = $json->decode($conf);	
		close $fh;
		undef $conf;
	};
	if ( $@ ) {
		error("elib: $@");
	}
}
else {
	error("elib: Aborting, no configuration file.");
}

# GetProducts
eval {
	# an incremental jsondecode would be preferred if data is very large
	# no problem yet bcoz indata is 24 mb...

	$checksum=sha512_hex("${serviceid}${from}${servicekey}");
	$url = "https://webservices.elib.se/librarymanagement/v1.0/products?serviceid=${serviceid}&from=${from}&checksum=${checksum}&format=json";

	print 'Products ', $url, "\n";

	$response = $browser->get($url);
	if ( ! $response->is_success ) {
		#print $response->content, "\n";
		die "GetProducts failed.";
	}
	$products = $json->decode($response->content);
	print "Products get/decode DONE\n";

	undef $response;
};
if ( $@ ) {
	error("elib: $@");
}

#=pod
# GetLibraryProducts aka 'Holdings' for each LibraryId
eval {
	for my $lid ( sort keys %{$config} ) {
		next if ( $lid ne '1928' ); #bara Helg
		my $libraryproduct='';
		$checksum=sha512_hex("${serviceid}${lid}${servicekey}");
		$url = "https://webservices.elib.se/librarymanagement/v1.0/libraryproducts/${lid}?serviceid=${serviceid}&checksum=${checksum}&format=json";
		
		print 'Holdings ', $url, "\n";

		$response = $browser->get($url);
		if ( ! $response->is_success ) {
			#print $response->content, "\n";
			die "GetLibraryProducts id=${lid} failed.";
		}

		$libraryproduct = $json->decode($response->content);

		for my $product ( @{$libraryproduct->{'Products'}} ) {
			my $pid = '';
			if ( scalar @{$product->{'ModelAvailabilities'}} ) {
				# Eh, is it available?
				$pid = $product->{'ProductID'}; # bwar!
			}
			if ( $pid ) {
				if ( ! defined $holdings{$pid} ) {
					$holdings{$pid} = [];
				}
				for my $sigel ( @{$config->{$lid}->{'sigels'}} ) {
					push @{$holdings{$pid}} , { 'Sigel' => $sigel, 'Status' => 'n', 'UrlPrefix' => $config->{$lid}->{'url_prefix'}, 'Remark' => $config->{$lid}->{'remark'}};
				}
			}
		}
		print "LibraryProducts $lid get/decode DONE\n";
		undef $libraryproduct;
		undef $response;
	}
};
if ( $@ ) {
	error("elib: $@");
}
#=cut

# Add accumulated holdings 
for my $product ( @{$products->{'Products'}} ) {
	my $from = '';
	my $to = '';
	my $avail = 0;
	for my $status ( @{$product->{'Statuses'}} ) {
		if ( $status->{'Name'} eq 'Active' ) {
			$from = Time::Piece->strptime($status->{'ValidFrom'}||'1970-01-01T00:00', '%Y-%m-%dT%H:%M');
			$to = Time::Piece->strptime($status->{'ValidUntil'}||'2030-01-01T00:00', '%Y-%m-%dT%H:%M');
			if ( $now <= $to && $now >= $from) {
				$avail = 1;	
				last;
			}
		} 

	}

	if ( $avail && defined $holdings{$product->{'ProductId'}} ) {
		if ( scalar @{$product->{'AvailableFormats'}} ) {
			$product->{'Holdings'} = $holdings{$product->{'ProductId'}};
		}
	}
	else {
		$product->{'Holdings'} = []; # important or diff will fail...
		# remains for testing purposes
		#my $isbn='';
		#for my $r ( @{$product->{'PublicIdentifiers'}}) {
		#	if ( $r->{'IdType'} eq 'ISBN' ) {
		#		$isbn = $r->{'Id'};
		#		last;
		#	}
		#}
		#print $product->{'ProductId'}, "\t", $product->{'Title'}, "\t", $isbn, "\n"; 
	}
}

# save current json (mv later i script to ./prev/elib.json)
#eval {
#	open(my $fh, '>:encoding(UTF-8)', './elib.json');
#	my $data = $json->encode($products);
#	print $fh $data;
#	undef $data;
#	close $fh;
#};
#if ( $@ ) {
#	error("elib: $@");
#}


build_index($products, 'a');
print "'a' index DONE\n";

#if ( -f './prev/elib.json' ) {
#	eval {
#		local $/; # slurp
#		open(my $fh, '<:encoding(UTF-8)', './prev/elib.json');
#		my $prev_json = <$fh>;
#		$prev_products = $json->decode($prev_json);	
#		build_index($prev_products, 'b');
#		print "'b' index DONE\n";
#		close $fh;
#		undef $prev_json;
#	};
#	if ( $@ ) {
#		error("elib: $@");
#	}
#}


# Print resulting xml ( Product + Holdings )
eval {
	#open(my $fh, '>:encoding(UTF-8)', './elib.xml');
	my $fh = \*STDOUT;
	print $fh "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
	print $fh "<Products>\n";
	compare_print(\*$fh);
	print $fh "</Products>\n";
	#close $fh;
};
if ( $@ ) {
	error("elib: write of ./elib.xml failed.");
}

# Finally, mv to ./prev & ./incoming
#move('./elib.json', './prev');
#if ( $@ ) {
#	error("elib: mv of ./elib.json failed.");
#}
#move('./elib.xml', './incoming');
#if ( $@ ) {
#	error("elib: mv of ./elib.xml failed.");
#}

sub compare_print {
my $fh = shift @_; # Filehandle

my $current = '';
my $position = 0;

for my $p ( keys %$D ) {
	if ( $D->{$p}->{'a_cs'} == $D->{$p}->{'b_cs'} ) {
		#unchanged
		next;
	}
	elsif ( $D->{$p}->{'b_cs'} == 0 ) {
		# new
		$current = $products;
		$position = $D->{$p}->{'a_pos'};
	}
	elsif ( $D->{$p}->{'a_cs'} == 0 ) {
		# deleted
		$current = $prev_products;
		$position = $D->{$p}->{'b_pos'};
		# delete holdings
		for my $h ( @{$current->{'Products'}->[$position]->{'Holdings'}} ) {
			$h->{'Status'} = 'd';	
		}
	}
	else {
		# changed
		$current = $products;
		$position = $D->{$p}->{'a_pos'};
		# diff holdings
		my $holds = $current->{'Products'}->[$position]->{'Holdings'};
		my $prev_holds = $prev_products->{'Products'}->[$D->{$p}->{'b_pos'}]->{'Holdings'};
		HOLDINGS: for my $ph ( @$prev_holds ) {
			for my $i (0..(scalar(@$holds)-1)) {
				if ( $holds->[$i]->{'Sigel'} eq $ph->{'Sigel'} ) {
					if ( cksum($holds->[$i]) == cksum($ph) ) {
						splice @$holds, $i, 1;
					}
					next HOLDINGS;	
				}
			}
			$ph->{'Status'} = 'd';
			push @$holds, $ph;
		}
	}

	print $fh perl2xml($current->{'Products'}->[$position], 'Product');
}
}

sub build_index {
	my ($products,$name) = @_; # \%, 'a|b'
	my $i = 0;

	for my $p ( @{$products->{'Products'}} ) {
		my $pid = $p->{'ProductId'};
		if ( ! defined $D->{"$pid"} ) {
			$D->{"$pid"} = { 'a_cs' => 0, 'a_pos' => -1, 'b_cs' => 0, 'b_pos' => -1 };
		}
		$D->{"$pid"}->{"${name}_cs"} = cksum($p);
		$D->{"$pid"}->{"${name}_pos"} = $i;
		$i++;
	}
}

# perl struct checksum
sub cksum {
	my $n = shift @_;
	my $result = 0;

	if ( ref($n) eq 'HASH' ) {
		for my $k ( keys %$n ) {
			$result += cksum($n->{$k});
		}
	}
	elsif ( ref($n) eq 'ARRAY' ) {
		for my $i ( @$n ) {
			$result += cksum($i);
		}
	}
	else {
		if ( $n ne '' ) {

			use bytes;

			for my $i (0..(length($n)-1)) {
        			$result += ord(substr($n,$i,1));
			}

			no bytes;
		}
	}

	return $result;
}

# perl struct to xml
sub perl2xml {
	my ($d,$p) = @_;
	my $result = '';
	
	if ( ref($d) eq 'HASH' ) {
		$result = "<$p>\n";
		for my $k ( keys %$d ) {
			$result .= perl2xml($d->{$k}, $k);
		}
		$result .= "</$p>\n";
	}
	elsif ( ref($d) eq 'ARRAY' ) {
		$result = "<$p>\n";
		for my $i ( @$d ) {
			$result .= perl2xml($i, "$p-item");
		}
		$result .= "</$p>\n";
	}
	else {
		if ( $d ne '' ) {
			# strip xml/html ?
			$d =~ s/<\/?.*?>//gso;
			$result = "<$p>";
			$result .= encode_entities_numeric("$d");
			$result .= "</$p>\n";
		}
	}

	return $result;
}

sub error {
	my $err = shift @_;

	system("mailx -s '$err' kai.poykio\@kb.se </dev/null");
	die "$err";
}
