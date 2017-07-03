#!/usr/local/bin/perl
# Elib import handling (total rewrite due to a new api).
# v2.0 Kai P

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
#use HTML::Entities qw(encode_entities_numeric);
#use File::Copy;
use Data::Dumper;

my $lid = $ARGV[0]||'';
if ( $lid eq '' ) { error("serviceid!"); }

chdir "/appl/import2/queues/elib";

my $from='2001-01-01';
my $serviceid='1863';
#my $libraryid='1845';
my $servicekey='2gCbnoVXuSONlyjkzvhTPwmsfUqpGFHeIDcZEYdJtQ63781r9i';

my $browser = LWP::UserAgent->new;
$browser->timeout(60);
my $json = JSON::XS->new->utf8;
#my $json = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($content);

my $response;
my $checksum;
my $url;

# GetLibraryProducts aka 'Holdings' for LibraryId/ServiceId
eval {
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
				print $pid, "\t";
				#if ( $pid == 1005788 ) {
					print Dumper($product->{'ModelAvailabilities'}), "\n";
				#}
			}
			else {
					print Dumper($product->{'ModelAvailabilities'}), "\n";
					print "END\n";
					exit;
			}
		}
		print "LibraryProducts $lid get/decode DONE\n";
};
if ( $@ ) {
	error("elib: $@");
}

sub error {
	my $err = shift @_;

	#system("mailx -s '$err' kai.poykio\@kb.se </dev/null");
	die "$err";
}
