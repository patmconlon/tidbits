#!/usr/local/bin/perl
use Date::Manip;
use DBI;
use File::Path;

$WORKDIR = '~/start';    #Voice files are
@files = `find ${WORKDIR} -type f -name "v\.[0-9.]*"`; 

foreach( @files ) {
	print $_;
	chomp( $myfilename = ( split /\//, $_ )[ -1 ] );
	$pathy = $&;
	print "PATHY: " . $pathy . "\n";
	chomp( @tmpyear = split( /\./, $_ ) );
	$year = substr( $tmpyear[ 3 ], 0, 4 );
	print $myfilename . " " . $year . "\n";

	#exit 0;
}