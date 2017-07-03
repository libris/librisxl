#!/usr/local/bin/perl
# 110525, Kai P
# Converts mysterious ebsco marc8 diacritics.
# Todo: convert other characters

binmode(STDIN,':raw');

%cmap = (
	#0xc2b0 => 0xcabb, no diak
	#0xc2b1 => 0xc582, no diak
	0xc3a0 => 0xcc89,
	0xc3a1 => 0xcc80,
	0xc3a2 => 0xcc81,
	0xc3a3 => 0xcc82,
	0xc3a4 => 0xcc83,
	0xc3a5 => 0xcc84,
	0xc3a6 => 0xcc86,
	0xc3a7 => 0xcc87,
	0xc3a8 => 0xcc88,
	0xc3a9 => 0xcc8c,
	0xc3aa => 0xcc8a,
	0xc3ab => 0xcda1,
	#0xc3ac => , # ?
	0xc3ad => 0xcc95,
	0xc3ae => 0xcc8b,
	0xc3af => 0xcc90,
	0xc3b0 => 0xcca7,
	0xc3b1 => 0xcca8,
	0xc3b2 => 0xcca3,
	0xc3b3 => 0xcca4,
	0xc3b4 => 0xcca5,
	0xc3b5 => 0xccb3,
	0xc3b6 => 0xccb2,
	0xc3b7 => 0xcca6,
	0xc3b8 => 0xcc9c,
	0xc3b9 => 0xccae,
	0xc3ba => 0xcda0,
	#0xc3bb => , # ?
	0xc3bf => 0xcc93
);

while (<>) {
	if ( /Tillgänglig för användare/o ) {
		# Do nothing
	}
	elsif ( /\xc3[\xa0-\xbf]/ ) {
		# Convert chars
		s/((\xc3[\xa0-\xbf])+)(.)/&convert($1,$3)/eg;
	}
	print $_;
}

sub convert () {
# converts marc8(ebsco style) diacritics to utf
# $1 = diacritic sequence
# $2 = character

my $d=shift @_;
my $ch=shift @_;
my $r='';
my $c='';

if ( $ch =~ /\W/o ) { return $ch; } # Takes care of faulty diacritics

while ( $d =~ /(.{2})/go ) {
	$c=$cmap{unpack('S',$1)}||'';
	next if ( $c eq '' );
	$r.=chr($c>>8).chr($c&0x00ff);
}

return $ch . $r;
}
