#!/usr/bin/perl
# ur_diff.pl, KP 090929
# 130423 KP, added delete for enddate 

if ( $#ARGV != 2 ) {
	die "Usage: $0 <previous-xml> <new-xml> <outfile>\n";
}

# Algoritm: build index of both files, pos,size,cksum, key(022a|020a), status
# use cksum to compare records
# B-A= added , modified if cksum(A)!=cksum(B) ie keep B
# A-B= deleted
# remember optimal! one pass... what is left is deleted, or added|modified

$date = `date "+%Y-%m-%d"`;

%idx_a=();
%idx_b=();

open XMLA, "<$ARGV[0]" or die "$ARGV[0] open failed.\n";
open XMLB, "<$ARGV[1]" or die "$ARGV[1] open failed.\n";
open OUTFILE, ">$ARGV[2]" or die "$ARGV[2] open failed.\n";

&build_idx(\*XMLA, \%idx_a);
&build_idx(\*XMLB, \%idx_b);

&compare_idx(\%idx_a,\%idx_b);

# Write to file

print OUTFILE "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<manifest identifier=\"root_manifest\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instanc:xsd=\"http://www.w3.org/2001/XMLSchema\" version=\"1.1.4\" xmlns=\"http://www.imsglobal.org/xsd/imscp_v1p1\">\n    <organizations />\n    <resources />\n";

&print_idx(\*OUTFILE,\*XMLA,\%idx_a);
&print_idx(\*OUTFILE,\*XMLB,\%idx_b);

print OUTFILE "</manifest>\n";

close XMLA;
close XMLB;
close OUTFILE;

sub compare_idx() {
my $idx_a = shift @_;
my $idx_b = shift @_;
# Compare idx's
# idx_a keeps deleted posts
# idx_b keeps added and modified posts (and deleted ! 130423 KP )
for $k (keys %$idx_a) {
	if ( defined $$idx_b{$k} ) {
		if ( $$idx_b{$k}->{st} eq 'd' ) {
			if ( $$idx_a{$k}->{st} eq 'd' ) {
				delete $$idx_b{$k};
			}
		}
		elsif ( $$idx_b{$k}->{cs} != $$idx_a{$k}->{cs} ) {
			$$idx_b{$k}->{st} = 'c';
		}
		else {
			delete $$idx_b{$k};
		}
		delete $$idx_a{$k};
	}
	else {
		$$idx_a{$k}->{st} = 'd';
	}
}
}

sub build_idx() {
my $file=shift @_; #Filehandle reference
my $ix=shift @_;	#assocarray reference
my $ps=0;	#Position
my $sz=0;	#Size
my $cs=0;	#Cksum
my $id='';	#Identifier, title???
my $st='n';	#Status n,c,d
my $buffer='';
my $position=0;

while ( <$file> ) {
	if ( /<manifest identifier=\"product_(\d+)/io ) {
		$id=$1;
		$buffer=$_;
		$ps=$position;
	}		
	elsif ( $buffer && /<\/manifest>/io ) {
		$buffer.=$_;

		# Special for SFX, 008 includes exportdate, patch it out! Only for cmp.
		#$buffer =~ s/tag="008">.{6}/tag="008">uuuuuu/o;	
		
		$sz=length($buffer);
		$cs=&cksum(\$buffer);
		#$id=&get_key(\$buffer);

		if ( ! valid_record(\$buffer) ) {
			$st='d';
		}
		
		if ( $id ne '' ) {
			#if ( defined $$ix{$id} ) {
			#	print STDOUT "$id\n";
			#}
			# keep last duplicate
			#$st=&get_status(\$buffer);
			$$ix{$id} = {ps=>$ps, sz=>$sz, cs=>$cs, st=>$st};
		}

		$buffer='';
		$sz=0;
		$cs=0;
		$id='';
		$st='n';
		$ps=0;
	}
	elsif ( $buffer ne '' ) {
		$buffer.=$_;
	}
	else {
		# What else?
	}

$position+=length($_); 
}
}

sub valid_record {
my $buffer = shift @_;
my @dist_events = ();
my $r=0;
my $enddate='';

@dist_events = $$buffer =~ /<distributionevent>(.*?)<\/distributionevent>/gois;

for my $de (@dist_events) {
	$enddate='';
	if ( $de =~ /<platform>internet/ois ) {
		if ( $de =~ /<type>download/ois ) {
			if ( $de =~ /<receivingagentgroup>avc/ois ) {
				($enddate) = $de =~ /<enddate>(\d{4}-\d{2}-\d{2})/ois;
				if ( $enddate eq '' || $enddate ge $date ) {
					#print "Enddate: $enddate\n";
					$r=1;
					last;
				}
			}
		}
	} 		
}

return $r;
}

# Not needed...
sub get_status() {
my $string=shift @_;
my $status='';

($status) = $$string =~ /<leader>(.*?)<\/leader>/io;

return substr($status, 5, 1);
}

# Not needed...
sub get_key() {
my $string=shift @_;
my $title='';
my $isxn='';

($title) = $$string =~ /tag=\"245\".*?code=\"a\">(.*?)<\//sio;

# ISXN can lead to update problems...
($isxn) = $$string =~ /tag=\"022\".*?code=\"a\">(.*?)<\//sio;
if ( $isxn eq '' ) {
	($isxn) = $$string =~ /tag=\"020\".*?code=\"a\">(.*?)<\//sio;
}
$isxn =~ s/\-//go;

# target as key is problematic...
#($target) = $$string =~ /tag=\"866\".*?code=\"x\">(.*?)<\//sio;

return "$title:$isxn";
}

sub cksum() {
my $string=shift @_; #String reference
my $ck=0;
my $i=0;

for $i (0..(length($$string)-1)) {
	$ck+=ord(substr($$string,$i,1));
}
return $ck;
}

sub print_idx() {
my $out = shift @_;
my $in = shift @_;
my $idx = shift @_;
my $x = '';
my $buffer = '';

foreach $x (keys %$idx) {
        seek $in, $$idx{$x}->{ps}, 0 or die "Seek failed at pos $$idx{$x}->{ps}.";
        read $in, $buffer, $$idx{$x}->{sz} or die "Read failed at pos $$idx{$x}->{ps}.";
	#change leader according to st
	$buffer =~ s/<metadata>/<libris_leader>$$idx{$x}->{st}<\/libris_leader>\n        <metadata>/i;

        print $out $buffer;
        $buffer='';
}
}
