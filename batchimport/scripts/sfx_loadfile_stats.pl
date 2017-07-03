#!/usr/bin/perl

%stats=();
$buf='';

while (<>) {
	if ( /<record type=\"Holdings\">/io ) {
		$buf=$_;
		next;
	}

	if ( /<\/record>/io && $buf ne '' ) {
		($change) = $buf =~ /<leader>-----(.)/sio;
		($sigel) = $buf =~ /tag=\"852\".*?code=\"b\">(.*?)<.*?<\/datafield/sio;
		$sigel = lc $sigel;

		if ( ($change ne '') && ($sigel ne '') ) {
			if ( ! defined $stats{$sigel} ) {
				$stats{$sigel} = {'n'=>0, 'c'=>0, 'd'=>0};	
			} 
			$stats{$sigel}->{$change} += 1;
		}

		#print $sigel, ' ', $change, "\n";

		$buf='';
		next;
	}
	if ( $buf ne '' ) {
		$buf .= $_;
	}
}

for $sigel (keys %stats) {
	open SIGEL, ">./sfx-$sigel.info";
	print SIGEL "new=", $stats{$sigel}->{'n'}, "\nchanged=", $stats{$sigel}->{'c'}, "\ndeleted=",  $stats{$sigel}->{'d'}, "\n";
	close SIGEL;
}
