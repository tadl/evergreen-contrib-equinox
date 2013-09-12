#!/usr/bin/perl -w
package main;
use POSIX;
use strict;


#get date info
my ($SEC, $MIN, $HOUR, $DAY,$MONTH,$YEAR) = (localtime(time))[0,1,2,3,4,5,6];
$YEAR+=1900;
$MONTH++;
if ($DAY < 10) {
   $DAY = "0".$DAY;
}
if ($MONTH < 10) {
   $MONTH = "0".$MONTH;
}

my $posF = "/tmp/eg_stats_position.log";
my $statsF = "/var/log/evergreen/prod/$YEAR/$MONTH/$DAY/eg_stats.log";
my $pos; 
my $loc;
my $status = 0; #status is OK!
my $info = "";

#if it exists open it and get the current position
#if not set the current position to 0
if (-e $posF) {
   open(DATA, "<$posF");
   my @values = <DATA>;
   if (@values != 2) { #make sure the array is the correct size
      $pos = 0;
   } else {
      chomp($loc = $values[0]);
      if ($loc ne $statsF) { #check to see that we are in the correct file
         $pos = 0;
      } else {
         chomp($pos = $values[1]); 
         #check to see if $pos is a valid positive integer(or 0), if not set to 0
         if (!( $pos =~ /^\d+$/ )) { 
            $pos = 0;
         }
      }
   }
   close DATA;
} else {
   $pos = 0;
}
#parse the file and output for Nagios if necessary
if (-e $statsF) {
   open(DATA, "<$statsF");
   seek DATA, $pos, 0;
   while(<DATA>) {
      my($line) = $_;
      chomp($line);
      #check for lost controller first
      if (($line =~ m/listener count: 0/) || ($line =~ m/controller count: 0/) || ($line =~ m/master count: 0/)){
         if ($line =~ m/listener count: 0/) {
            $info = $info."Lost a listener: $line - ";
         }
	 if ($line =~ m/master count: 0/) {
            $info = $info."Lost a master: $line - ";
         }
	 if ($line =~ m/controller count: 0/) {
            $info = $info."Lost a controller: $line - ";
         }
         $status = 2;
      } 
      #now check for drone ratio
      if ($line =~ m/SERVICE/) {
         my ($count) = $line =~ /drone count: (\d+\/\d+)/i;
	 my $ratio = eval($count);
	 my $pct = ceil($ratio * 100);
         if ($ratio >= 0.75) {
            $info = $info."Drone count is $pct % - $line - ";
            $status = 1;
         }
         if ($ratio >= 0.9) {
            $info = $info."Drone count is $pct % - $line - "; 
            $status = 2;
         }    
      }
      
   }
   $pos = tell DATA;
   close DATA;
} else {
   $pos = 0;
}

#update position info
open(DATA, ">$posF");
print DATA "$statsF\n$pos\n";
close DATA;

if ($info eq "") {
   $info = "EG-STATS-COLLECTOR STATUS: OK!";
}

print $info;
exit $status;
