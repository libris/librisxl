#!/usr/local/bin/perl
# Elib import handling (total rewrite due to a new api).
# v2.2 Kai P

#use strict;
#use warnings;
use Digest::SHA qw(sha512_hex);
use JSON::XS;
use LWP;
use HTML::Entities qw(encode_entities_numeric);
use File::Copy;
use Data::Dumper;

my $from='2001-01-01';
my $serviceid='1863';
#my $libraryid='1845';
my $servicekey='2gCbnoVXuSONlyjkzvhTPwmsfUqpGFHeIDcZEYdJtQ63781r9i';

my $browser = LWP::UserAgent->new;
$browser->timeout(60);
#my $json = JSON::XS->new->utf8;
#my $json = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($content);

my $products;
my $response;
my $checksum;
my $url;

# GetProducts
eval {
	$checksum=sha512_hex("${serviceid}${from}${servicekey}");
	$url = "https://webservices.elib.se/librarymanagement/v1.0/products?serviceid=${serviceid}&from=${from}&checksum=${checksum}&format=json";

	print 'Products ', $url, "\n";

	$response = $browser->get($url);
	if ( ! $response->is_success ) {
		#print $response->content, "\n";
		die "GetProducts failed.";
	}
	#$products = $json->decode($response->content);

	print $response->content;
};
if ( $@ ) {
	print "elib: $@";
}
