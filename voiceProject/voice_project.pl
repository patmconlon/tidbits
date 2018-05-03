#!/usr/local/bin/perl
use Date::Manip;
use DBI;
use File::Path;
use IO::Handle;

$LOGDIR      = '~';      #Home directory
$WORKDIR     = '/voicefiles';    #/voicefiles Voice files are
$PUTDIR      = '/voicefiles/myClientName';       #/voicefiles/myClientName Final Rest before copy to External Hard drive
$PUTDIRTEMP  = $PUTDIR; #Temp holding location.
$PUTDIRMISC  = '/voicefiles/myClientName/miscellaneous'; #Final rest before copy to External HD
#The STAGING AREA is the location where the unconverted file will rest adn the converted file while
# processing the files. These two files will be deleted once the process if determined a success.
$STAGINGAREA = '~/stage'; #Place to convert from while freeing space from HD
$LOGHIST     = '/tmp/abbr';
$countLoop = 0; #used in pagingj

## Get the number of counts the file has ran. This is for the lock file.
open( COUNTFILE, "< ${LOGDIR}/currentLoop" );
chomp( $line = <COUNTFILE> );

$line = $line + 1; 
close( COUNTFILE );

open( COUNTS, "> ${LOGDIR}/currentLoop" ) or print "NOPE";
if( $line > 49 ) {
	pageMeOut( 'Lock File has existed for 50 attempts. Check lock file and verify process is running.' );
	$line = 0;
}
print COUNTS $line;
close( COUNTS );

## Does a lock file exist?
if( -e ${LOGDIR} . '/abbr_lock_file.lock' ) {
	print "Exit as lock file exists\n";
	exit 1;
}
else {
	$lock_file = ${LOGDIR} . '/abbr_lock_file.lock' ;
	open LOCKFILE, ">> $lock_file" or die "can't open $lock_file\n";
	print LOCKFILE "START: " . localtime() . "\n";
	close( LOCKFILE );
	`/usr/bin/chmod 666 ${LOGDIR}/abbr_lock_file.lock`;
}

mkpath( $PUTDIR ); #This path for the hds
mkpath( $PUTDIRMISC ); 

$CLIENT = 'abbr';
$CLIENT_NUM = 526808;
#do '/usr/local/bin/setvar.pl';

($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
$yearNow = $year;
$yearNow += 1900;

$mon = $mon + 1;
if( $mon < 10 ) {
	$mon = '0' . $mon;
}

if( $mday < 10 ) {
	$mday = '0' . $mday;
}
$monthNow = $mon;
$dayNow = $mday;
if( $hour < 10 ) {
	$hour = '0' . $hour;
}
if( $min < 10 ) {
	$min = '0' . $min;
}
if( $sec < 10 ) {
	$sec = '0' . $sec;
}

open( LOGHIST, ">> ${LOGHIST}/log." . $yearNow . $monthNow . $dayNow );
LOGHIST->autoflush(1);
print LOGHIST "START SCRIPT: " . $yearNow . $monthNow . $dayNow . ' ' . $hour . $min . $sec . "--------------------\n";
$VoxFile = "${LOGHIST}/vox_file_list." . $yearNow . $monthNow . $dayNow;
open VOXFILE, "> $VoxFile" or die "can't open $VoxFile\n";
print LOGHIST "VoxFile set to $VoxFile \n";

# $startingDate = '1167627600';
# ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($startingDate);
# $year += 1900; ## $year contains no. of years since 1900, to add 1900 to make Y2K compliant
# $mon = $mon + 1;
# my $nextDay = $startingDate;
# print "$sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst,$startingDate\n";
#$mon = $mon + 1;

# if( $mon < 10 ) {
	# $mon = '0' . $mon;
# }
# if( $mday < 10 ) {
	# $day = '0' . $mday;
# }


# $CPREVDATE = $year . $mon . $day;             #20050323
# $CFDATE    = $mon . '/' . $day . '/' . $year; #03/23/2005

# print "CPREVDATE: " . $CPREVDATE . " | CFDATE: " . $CFDATE . "\n"; 

$ENV{INFORMIXSERVER} = 'server_name';
$ENV{FET_BUF_SIZE} = '32000';
$ENV{INFORMIXDIR} = '/comp/informix';


#These are from: 
#$skills = "234,1610,1554,214,557,1992,335,548,1167,1632,504,554,1609,107,1606,1631,258,300,502,205,499,53,501,1165,1629,2104,2146,2158,2159,2161,2205,2206,2360,2361,2371";
$skills = "388,389,390,390,391,442,444,445,455,456,457,468,47,499,555,53,501,502,550,562,1625,1164,1166,1627,1628,1630,1866,1894,2002,2237,2259,2286,2364,2365,2366,2359,2384,2402,2405,2454,2460,503";
print LOGHIST $skills . "\n";

####################################################################################################
#		SET UP ALL OF OUR DATE VARIABLES THAT WE WILL NEED TO USE			   #
####################################################################################################

$now = time;
print "NOW: " . $now . "\n";
$yesterday	= $CPREVDATE;

####################################################################################################
#		OPEN LOG FILE TO LOG EVERYTHING THAT HAPPENS					   #
####################################################################################################
print LOGHIST " ----------------- Working on Building Files from db_name tables - $now ------------------ \n";

####################################################################################################
#		PERFORM OUR DATABASE QUERY
####################################################################################################

print LOGHIST "Querying Informix tables on db_name db on ibm354_spy to retrieve recordings\n";

$dbh2 = DBI->connect('dbi:Informix:db_name@server_name') ||  pageMeOut("DBERROR Cant connect to database: $DBI::errstr");
$dbh3 = DBI->connect('dbi:Informix:db_name@server_name') ||  pageMeOut("DBERROR Cant connect to database: $DBI::errstr");
$dbh2->do("set lock mode to wait 300");
##############################################################################################

$sth=sprintf("select skillnumber, abbreviation, name from skill where clientid=97 and skillnumber in ($skills);");
print LOGHIST $sth . "\n";
#$sth=sprintf("select setup_date, skillgroup, filename, ani from recordings where skillgroup in ($skills) and setup_date between '$DateBegin' and '$DateEnd';");
$getdata=$dbh2->prepare($sth) or die;
$getdata->execute();
$getdata->bind_columns(undef, \$skillnumber, \$abbreviation, \$skillNameFull );

while ($getdata->fetch()) {
	print LOGHIST 'SKILL STUFF: ' . $skillnumber . ' ' . $abbreviation . ' ' . $skillNameFull . "\n";
	$abbreviation =~ s/\s+//g; 
	@skill_name[$skillnumber] = $abbreviation;
	@skill_info[$skillnumber] = $skillNameFull;
}
$getdata->finish();


#exit 0;
##############################################################################################
$sth=sprintf("select agentkey, login from agent;");
print LOGHIST $sth . "\n";
$getdata=$dbh2->prepare($sth) or die;
$getdata->execute();
$getdata->bind_columns(undef, \$agentkey, \$login);

while ($getdata->fetch()) {
	$login =~ s/\s+//g;
	@agent_name[$agentkey] = $login;
}
$getdata->finish();
##############################################################################################
open( DATEFILE, "< $LOGDIR/NextProcessDate.txt" ) or die( pageMeOut( 'Couldnot open NextProcessDate.txt' ) );
chomp( $findDate = <DATEFILE> );
close( DATEFILE );

@files = `/usr/bin/find ${WORKDIR}/${findDate}* -type f -name "v\.[0-9.]*"`;

$counter = -1;
$max = @files - 1;

open( NOCHANGEFILE, ">> ${LOGHIST}/NOTCONVERTED.txt" ) or pageMeOut( 'FILE: NOTCONVERTED.txt could not be opened.' );
open( PROCESSED, ">> ${LOGHIST}/PROCESSED.txt" ) or pageMeOut( 'FILE: PROCESSED.txt could not be opened.' );
PROCESSED->autoflush(1);

#Here is the loop for the files already found.
$pathy = '';
$counterLoop = 0;
foreach( @files ) {
	if( $counterLoop == 10000 ) {
		
		pageMeOut( 'Pick up files from the /tmp directory on ibm108_old.' );
		$counterLoop = 0;
	}

	($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
	$yearNow = $year;
	$yearNow += 1900;

	$mon = $mon + 1;
	if( $mon < 10 ) {
		$mon = '0' . $mon;
	}

	if( $mday < 10 ) {
		$mday = '0' . $mday;
	}
	$monthNow = $mon;
	$dayNow = $mday;
	if( $hour < 10 ) {
		$hour = '0' . $hour;
	}
	if( $min < 10 ) {
		$min = '0' . $min;
	}
	if( $sec < 10 ) {
		$sec = '0' . $sec;
	}
	print LOGHIST "---- TIME STAMP: " . $yearNow . $monthNow . $dayNow . ' ' . $hour . $min . $sec . "----\n";

	chomp( $pathy = $_ );
	print "PATH: " . $pathy . "\n";
	chomp( $myfilename = ( split /\//, $_ )[ -1 ] );
	chomp( @tmpyear = split( /\./, $_ ) );
	$year = substr( $tmpyear[ 3 ], 0, 4 );
	print $myfilename . " " . $year . "\n";

	$return = system( `/usr/bin/cp ${pathy} ${STAGINGAREA}` ); ## Copy the /voicefile/ to /home/ directory.
	if( ! -e $STAGINGAREA . '/' . $myfilename ) {
		print LOGHIST "Could not copy " . $pathy . " to " . $STAGINGAREA . " and trying again.\n";
		`/usr/bin/cp ${pathy} ${STAGINGAREA}`;
	}
	#exit 0;

	print LOGHIST "Determine which connection and which table use.\n";
	# This can actually be a dbh2 connection only and remove the dbh3 connection all together.
	if( $year < 2009 ) {
		$sth=sprintf( 'select setup_date, skillgroup, agentid, filename, ani from temp_recordings where filename = "' . $myfilename . '";' ); 
		#$sth=sprintf("select setup_date, skillgroup, agentid, filename, ani from recordings where setup_date='$CFDATE' and skillgroup in ($skills);");
		print LOGHIST "Less than 2009 " . $sth . "\n";
		$getdata = $dbh3->prepare( $sth ) or pageMeOut( 'DBERROR: Failed to Prepare server_name' . $DBI::errstr ); #ibm353
	} else {
		$sth=sprintf( 'select setup_date, skillgroup, agentid, filename, ani from recordings where filename = "' . $myfilename . '";' ); 
		print LOGHIST "2009 and greater " . $sth . "\n";
		$getdata=$dbh2->prepare($sth) or pageMeOut( 'DBERROR: Failed To Prepare ibm354_spy' . $DBI::errstr ); #ibm354
	}

	$getdata->execute();
	$getdata->bind_columns(undef, \$setup_date, \$skillgroup, \$agentid, \$filename, \$ani);

	while ($getdata->fetch()) {
		($theFileMonth,$theFileDay,$theFileYear) = split( /\//, $setup_date );
		($z,$y,$x,$dt,$w,$u) = split( /\./, $filename );
		$timeOfFile = substr( $dt, 4, 6 );
		print $theFileMonth . "," . $theFileDay . "," . $theFileYear . "," . $timeOfFile . "\n";


		# Creating file name based on the return from the database
		print LOGHIST "AGENTNAME: " . $agent_name[ $agentid ] . " ANI: " . $ani . " SKILLGROUP: " . $skill_info[ $skillgroup ] . "\n";
		if( $agent_name[ $agentid ] eq '' || $ani eq '' || $skill_info[ $skillgroup ] eq '' ) {
			$PUTDIR = $PUTDIRMISC;
			$newFileName = $filename . ".mp3";
			print NOCHANGEFILE $filename . ": " . $agent_name[ $agentid ] . '|' . $theFileYear . $theFileMonth . $theFileDay . '|' . $ani . '|' . $skill_info[ $skillgroup ] . '|' . $timeOfFile . "\n";
		}
		else {
			#$newFileName = $agent_name[ $agentid ] . '_' . $theFileYear . $theFileMonth . $theFileDay . '_' . $ani . '_' . $skillgroup . '_' . $timeOfFile . '.mp3'; 
			#There is no account number stored.
			#Create the directory to put the file in 
			$newFileName = $theFileYear . '-' . $theFileMonth . '-' . $theFileDay . 'T' . $timeOfFile . '_' . $ani . '_' . $skill_info[ $skillgroup ] . '_' . $agent_name[ $agentid ] . '.mp3';
		}

		#Now convert the files within the ~/ directory
		#/usr/local/bin/sox /usr/bin/lame
		print LOGHIST "/usr/bin/cat " . $STAGINGAREA.'/'.$myfilename .' | /usr/local/bin/sox -t vox -r 6000 - -t wav -r 24000 - | /usr/bin/lame -b 48 - ' .$STAGINGAREA.'/'.$newFileName."\n";
		`/usr/bin/cat ${STAGINGAREA}/${myfilename} | /usr/local/bin/sox -t vox -r 6000 - -t wav -r 24000 - | /usr/bin/lame -b 48 - ${STAGINGAREA}/${newFileName}`;
		#`/usr/bin/cat $pathy | /usr/local/bin/sox -t vox -r 6000 - -t wav -r 24000 - | /usr/bin/lame -b 48 - $PUTDIR/$theFileYear/$theFileMonth/$theFileDay/$skill_info[ $skillgroup ]/${agent_name[ $agentid ]}/$newFileName`; 
		$fileOriginal = system( "ls ${STAGINGAREA}/${myfilename}" );
		if( $fileOriginal != 0 ) {
			pageMeOut( "MISSING the original after conversion: " . $myfilename . "\n" );
		}
		$fileConverted = system( "ls ${STAGINGAREA}/${newFileName}" );
		if( $fileConverted != 0 ) {
			pageMeOut( "MISSING the converted file after conversion: " . $newFileName . "\n" );
		}

		# Need to create the directories. 
		$DIRECTORYPATH = '';
		if( $agent_name[ $agentid ] eq '' || $ani eq '' || $skill_info[ $skillgroup ] eq '' ) {	
			$PUTDIR = $PUTDIRMISC;
			#Create the directory to put the file in
			$DIRECTORYPATH = $PUTDIR . '/' . $theFileYear . '/' . $theFileMonth . '/' . $theFileDay . '/';
			if( exists( $skill_info[ $skillgroup ] ) ) {
				$skill_info[ $skillgroup ] =~ s/\s+/_/g;
				$DIRECTORYPATH = $DIRECTORYPATH . '/' . $skill_info[ $skillgroup ];

				if( exists( $agent_name[ $agent_id ] ) ) {
					$agent_name[ $agent_id ] =~ s/\s+/_/g;
					$DIRECTORYPATH = $DIRECTORYPATH . '/' . $agent_name[ $agent_id ];
				}
			}
			unless( -d $DIRECTORYPATH ) {
				mkpath( $DIRECTORYPATH ); #This path for the hds
			}
			print LOGHIST "Creating the directory structure: " . $DIRECTORYPATH . "\n";
		}
		else {
			#$newFileName = $agent_name[ $agentid ] . '_' . $theFileYear . $theFileMonth . $theFileDay . '_' . $ani . '_' . $skillgroup . '_' . $timeOfFile . '.mp3'; 
			#There is no account number stored.
			#Create the directory to put the file in 
			$DIRECTORYPATH = $PUTDIR . '/' . $theFileYear . '/' . $theFileMonth . '/' . $theFileDay . '/' . $skill_info[ $skillgroup ] . '/' . $agent_name[ $agentid ];
			unless( -d $DIRECTORYPATH ) {
				mkpath( $DIRECTORYPATH ); #This path for the hds
			}
		}
		unlink( $pathy ); #Remove the original file.
		`/usr/bin/mv ${STAGINGAREA}/${newFileName} ${DIRECTORYPATH}`;
		print LOGHIST "MOVED " . $STAGINGAREA . '/' . $newFileName . ' ' . $DIRECTORYPATH . "\n";

		#Writing to the processed log
		unless( -e $DIRECTORYPATH . '/' . $newFileName ) {
			print "FILE NOT FOUND IN FINAL DIRECTORY\n";
			pageMeOut("MISSING File after move: " . $newFileName . "\n");
			print NOTPROCESSED $pathy . "\n"; #Write a processed file to track changes.
			print LOGHIST "NOTPROCESSED: " . $pathy . "\n";
			print LOGHIST "Moving " . $STAGINGAREA . '/' . $myfilename . "\n";
			`/usr/bin/mv ${STAGINGAREA}/${myfilename} ${pathy}`; #Moving the file back for processing later.
		} 
		else {
			print PROCESSED $pathy . ' ' . $DIRECTORYPATH . '/' . $newFileName . "\n"; #Write a processed file to track changes.
			print LOGHIST "PROCESSED and Removed: " . $pathy . "\n";
			print LOGHIST "Removing " . $STAGINGAREA . '/' . $myfilename . "\n";
			unlink( $STAGINGAREA . '/' . $myfilename );
		}
		print VOXFILE "$setup_date,$skill_name[$skillgroup],$agent_name[$agentid],$filename,$ani,$newFileName\n";

		#### MIGHT NEED TO PUT A REMOVE FILE HERE. NEED TO FIX LOGIC LINE 231 - 239.
		$PUTDIR = $PUTDIRTEMP; #Resetting the directory back to the default.
	}
	$counterLoop = $counterLoop + 1;
}
$getdata->finish();



#$getdata2->finish();

$dbh2->disconnect();
$dbh3->disconnect();

#####################################################################
# PERFORM OUR CHECKS
#####################################################################


print LOGHIST "Closing $VoxFile \n";

unlink $lock_file;

($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
$yearNow = $year;
$yearNow += 1900;

$mon = $mon + 1;
if( $mon < 10 ) {
	$mon = '0' . $mon;
}

if( $mday < 10 ) {
	$mday = '0' . $mday;
}
$monthNow = $mon;
$dayNow = $mday;
if( $hour < 10 ) {
	$hour = '0' . $hour;
}
if( $min < 10 ) {
	$min = '0' . $min;
}
if( $sec < 10 ) {
	$sec = '0' . $sec;
}

print LOGHIST "END RUN: " . $yearNow . $monthNow . $dayNow . ' ' . $hour . $min . $sec . "--------------------\n";

open( DATEFILE, "> $LOGDIR/NextProcessDate.txt" ) or die( pageMeOut( 'Couldnot open NextProcessDate.txt' ) );
$findDate = $findDate + 1;
if( $findDate > 2013 ) {
	$findDate = 2007;
}
print DATEFILE $findDate;
close( DATEFILE );

close( PROCESSED );
close NOCHANGEFILE;
close VOXFILE;
$return = system( '/usr/bin/uuencode ' . $LOGHIST . '/NOTCONVERTED.txt NOTCONVERTED.txt | mail -s "NOT CONVERTED" username@company.com' ); 
$return = system( '/usr/bin/uuencode ' . $LOGHIST . '/PROCESSED.txt PROCESSED.txt | mail -s "PROCESSED" username@company.com' );
$return = system( '/usr/bin/uuencode ' . $LOGHIST . '/vox_file_list.' . $yearNow . $monthNow . $dayNow.' vox_file_list.txt | mail -s "VOXFILE LIST" username@company.com' );

unlink "${LOGDIR}/NOTCONVERTED.txt";

#The run has finished and need to reset the counters.
$line = 0;
open( COUNTS, "> ${LOGDIR}/currentLoop" ) or print "NOPE";
print COUNTS $line;
close( COUNTS );
close( LOGHIST );

exit 0;
#####################################################################

sub pageMeOut() {
	$message = $_[ 0 ];
	print $message . "\n";
	open( MESSAGES, ">> ${LOGDIR}/message.txt" );

	if( $message =~ m/DBERROR/ ) {
		print MESSAGES "Subject 108: myClientName VOICE PROCESS DATABASE ERROR\n";
	}
	else {
		print MESSAGES "Subject 108: myClientName Voice Process\n";
	}
	print MESSAGES $message . "\n\n";
	close MESSAGES;
	if( ( $counterLoop % 10 ) != 0 && $message !~ m/DBERROR/ ) {
		$return = system( '/usr/sbin/sendmail -v username@company.com < ' . $LOGDIR . '/message.txt' );
	} else {
		$return = system( '/usr/sbin/sendmail -v phoneNumber@messaging.sprintpcs.com username@company.com < ' . $LOGDIR . '/message.txt' );
	}
	sleep 10;

	unlink( "$LOGDIR/message.txt" );
	if( -e $LOGDIR . '/message.txt' ) {
		`/usr/bin/rm ${LOGDIR}/message.txt`;
	}
}
#####################################################################
