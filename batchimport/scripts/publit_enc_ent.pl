#!/usr/local/bin/perl

my %me = (  '<'=>'&#60;',
        '>'=>'&#62;',
        '"'=>'&#34;',
        '\''=>'&#39;',
        '&'=>'&#38;',
        '&lt;'=>'&#60;',
        '&gt;'=>'&#62;',
        '&quot;'=>'&#34;',
        '&apos;'=>'&#39;',
        '&amp;'=>'&#38;'
        );

my $buf = '';
my $rec_start = 'product\s+.*?';
my $rec_end = 'product';

while (<>) {
	if ( $_ =~ /<$rec_start>/o ) { $buf = $_; next; }
	if ( $_ =~ /<\/$rec_end>/o ) {
		$buf .= $_;
		#$buf = trim($buf);
		trim(\$buf);
		print $buf;
		$buf = '';
		next;
	}
	if ( $buf ne '' ) { $buf .= $_; next; }
	print;
}

sub repl {
my $s = shift @_;

# Encode chars+entities to numeric+illegals? converted,numerics unconverted
$s =~ s/([<>\'\"]|&([\d\w]+);|&(?!#\d+;|#x[aAbBcCdDeEfF\d]+;))/$me{lc "$1"}||"&#38;$2"/goes;

return $s;
}


sub trim {
my $b = shift @_; # $ ref

# match textnodes, matches not if unencoded '>' is contained... (kludge!)
# do we really need to DOM the buffer instead?
$$b =~ s/>([^>]*?)<\//">" . repl($1) . "<\/"/goes;

return 1;
}
