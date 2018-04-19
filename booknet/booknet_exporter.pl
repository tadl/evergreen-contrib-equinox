#!/usr/bin/perl

# Copyright (c) 2018 Equinox Open Library Initiative
# Author: Rogan Hamby <rhamby@equinoxinitiative.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>


use strict;

use Getopt::Long;
use DBI;
use File::Copy;
use Net::FTP;
use Business::ISBN;

my $org;
my $db_host;
my $db_user;
my $db_database;
my $db_password;
my $db_port = '5432';
my $ftp_folder;
my $ftp_host;
my $ftp_user;
my $ftp_password;
my $ftp_port;
my $output;
my $dbh;

my $ret = GetOptions(
    'org:s'           	=> \$org,
    'db_host:s'  	    => \$db_host,
    'db_user:s'    	    => \$db_user,
    'db_database:s'     => \$db_database,
    'db_password:s'  	=> \$db_password,
    'db_port:s'	        => \$db_port,
    'ftp_folder:s'      => \$ftp_folder,
    'ftp_host:s'        => \$ftp_host,
    'ftp_user:s'        => \$ftp_user,
    'ftp_password:s'    => \$ftp_password,
    'ftp_port:s'        => \$ftp_port
);

abort('must specify --org') unless defined $org;

my ($date,$printdate) = format_date();

if ($db_host and $db_user and $db_password and $db_database) { 
        $dbh = connect_db($db_database,$db_user,$db_password,$db_host,$db_port) or abort("Cannot open database at $db_host $!"); 
    } else { 
        $dbh = connect_db_socket($db_database) or abort("Cannot open local socket database connection $!");
};

if (!defined $ftp_user and !defined $ftp_host) {
    print STDERR "Incomplete FTP settings.  No file will be transferred.\n"; }

my @org_name_desc;
push @org_name_desc, descendants($org, $dbh);

my $log = $org . '_export_log';
my $branchfile = 'BRANCHDATA_' . $org . '_' . $printdate . '.csv';
my $lendingfile = 'LENDINGDATA_' . $org . '_' . $printdate . '.csv';

open my $log_fh, '>>', $log or die "Can not open file.\n";
open my $branch_fh, '>', $branchfile or die "Can not open $branchfile.\n";
open my $lending_fh, '>', $lendingfile or die "Can not open $lendingfile.\n";

print_log($log_fh,"Export started.");
print $branch_fh "System_Code,Branch_Code,Branch_Name,Postal_Code,Address,User_Count,DateEnd\r\n";
print $lending_fh "BranchCode,CatalogueKey,EAN_ISBN13,Loans,Renewals,Holds,Copies,CopiesOut,CopiesOnOrder,DateEnd\r\n";

foreach my $org_name (@org_name_desc) {
    my $org_id = get_org_id($org_name);
   
    my $d = $date;
    $d =~ s|/|.|g; 
    my $full_name = get_full_name($org_id);
    my $post_code = get_post_code($org_id);
    my $address = get_address($org_id);
    my $patron_count = get_patron_count($org_id);
    print $branch_fh "$org,$org_name,$full_name,$post_code,$address,$patron_count,$date\r\n";
    print_log($log_fh,"$branchfile exported.");

    my @holdings = get_lending_data($org_id);
    foreach my $built_hash( @holdings ) {
        my $isbns = extract_isbns($built_hash->{isbns});
        print $lending_fh "$org_name,";
        print $lending_fh "$built_hash->{bib_id},";
        print $lending_fh "$isbns,";
        print $lending_fh "$built_hash->{circs},";
        print $lending_fh "$built_hash->{renewals},";
        print $lending_fh "$built_hash->{holds},";
        print $lending_fh "$built_hash->{copies},";
        print $lending_fh "$built_hash->{circs_out_now},";
        print $lending_fh "$built_hash->{onorder},";
        print $lending_fh "$date\r\n";
    }
}

close $lending_fh;
close $branch_fh;

print_log($log_fh,"$lendingfile exported.");
my $ftp;
if (defined $ftp_host and defined $ftp_user) {
    $ftp = connect_ftp($ftp_host,$ftp_user,$ftp_password,$ftp_port,$ftp_folder,$log_fh);
    put_file($branchfile,$ftp,$log_fh);
    put_file($lendingfile,$ftp,$log_fh);
}

print_log($log_fh,"All done.");
close $log_fh;

#######  ---- beyond here lay dragons, or at least subroutines

sub print_log {
    my $fh = shift;
    my $entry = shift;
    my ($a,$b) = format_date(); 
    my $time = format_time();
    print $fh "$a $time $entry \n";
    return();
}

sub put_file {
    my ($file,$ftp,$log_fh) = @_;
    $ftp->put($file) or abort("Can not transfer $file.\n");
    print_log($log_fh,"$file transferred.");
    return();
}

sub connect_ftp {
    my ($ftp_host,$ftp_user,$ftp_password,$ftp_port,$ftp_folder,$log_fh) = @_;
    my $ftp;

    if (!defined $ftp_port) { $ftp_port = '21' }

    my $ftp = Net::FTP->new($ftp_host, Debug=> 0, Passive => 1, Port=> $ftp_port) or abort("Failure to connect to FTP site.");
    print_log($log_fh,"connected to $ftp_host.");
    $ftp->login($ftp_user,$ftp_password) or abort("Failure to login to FTP site.");
    print_log($log_fh,"logged in to $ftp_host.");
    $ftp->binary();
    if (defined $ftp_folder) {
        $ftp->cwd($ftp_folder) or abort("Failure to load specified directory.");
        print_log($log_fh, "changed directory to $ftp_folder.");
    }

    return $ftp;
}

sub uniq {
    my %seen;
    grep !$seen{$_}++, @_;
}

sub descendants {
    my ($org_name, $dbh) = @_;
    my $sql = 'SELECT aou.shortname FROM (SELECT * FROM actor.org_unit_descendants((SELECT id FROM actor.org_unit WHERE shortname = \'' . $org_name . '\'))) x JOIN actor.org_unit aou ON aou.id = x.id JOIN actor.org_unit_type aout ON aout.id = aou.ou_type WHERE aout.can_have_vols IS TRUE;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my @valid_orgs;
    while (my @row = $sth->fetchrow_array) {
        push @valid_orgs, @row;
    }   
    return @valid_orgs; 
}

sub get_parent_name {
    my ($org_id) = @_;
    my $sql = 'SELECT shortname FROM actor.org_unit WHERE id = ' . $org_id . ';';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my $r;
    while (my @row = $sth->fetchrow_array) {
        $r = $row[0];
    }  
    return $r;
}

sub get_org_id {
    my ($org_name) = @_;
    my $sql = 'SELECT id FROM actor.org_unit WHERE shortname = \'' . $org_name . '\';';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my $r;
    while (my @row = $sth->fetchrow_array) {
        $r = $row[0];
    }  
    return $r;
}

sub get_full_name {
    my ($org_id) = @_;
    my $sql = 'SELECT name FROM actor.org_unit WHERE id = ' . $org_id . ';';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my $r;
    while (my @row = $sth->fetchrow_array) {
        $r = $row[0];
    } 
    $r = csv_protect_string($r);
    return $r;
}

sub format_date {
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
    $year = $year + 1900;
    $mon = $mon + 1;
    if (length($mday) < 2) {$mday = '0' . $mday;}
    if (length($mon) < 2) {$mon = '0' . $mon;}
    my $date = $mon . "/" . $mday . "/" . $year;
    my $printdate = $mon . $mday . $year;
    return ($date,$printdate);
}

sub format_time {
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
    my $time_stamp = "$hour:$min:$sec";
    return ($date,$time_stamp);
}


sub get_post_code {
    my ($org_id) = @_;
    my $sql = 'SELECT post_code FROM actor.org_address WHERE org_unit = ' . $org_id . ' AND post_code IS NOT NULL ORDER BY address_type = \'MAILING\' LIMIT 1;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my $r;
    while (my @row = $sth->fetchrow_array) {
        $r = $row[0];
    } 
    $r = csv_protect_string($r);
    return $r;
}

sub get_patron_count {
    my ($org_id) = @_;
    my $sql = 'SELECT COUNT(id) FROM actor.usr WHERE home_ou = ' . $org_id . ' AND deleted IS FALSE AND active IS TRUE;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my $r;
    while (my @row = $sth->fetchrow_array) {
        $r = $row[0];
    }
    return $r;
}

sub get_address {
    my ($org_id) = @_;
    my $sql = 'SELECT street1 || \' \' || street2 || \';\' || city || \';\' || state FROM actor.org_address WHERE org_unit = ' . $org_id . ' AND post_code IS NOT NULL ORDER BY address_type ~* \'MAILING\' LIMIT 1;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my $r;
    while (my @row = $sth->fetchrow_array) {
        $r = $row[0];
    } 
    $r = csv_protect_string($r);
    return $r;
}

sub get_lending_data {
    my ($org_id) = @_;
    my $sql = 
    'SELECT 
        ssr.id
        ,ARRAY_TO_STRING(ssr.isbn,\';\') 
        ,COUNT(circs.id)
        ,COUNT(renews.id)
        ,COUNT(holds.id)
        ,COUNT(ac.id)
        ,COUNT(circs_out.id)
        ,COUNT(onorder.id)
    FROM 
        (SELECT id, call_number FROM asset.copy WHERE circ_lib = ' . $org_id . ' AND deleted IS FALSE) ac 
    LEFT JOIN
        (SELECT id, target_copy FROM action.circulation WHERE xact_start > now() - interval \'1 week\') circs ON circs.target_copy = ac.id
    LEFT JOIN
        (SELECT id, target_copy FROM action.circulation WHERE checkin_time IS NULL AND xact_finish IS NULL) circs_out ON circs_out.target_copy = ac.id
    LEFT JOIN
        (SELECT id, target_copy FROM action.circulation WHERE xact_start > now() - interval \'1 week\' and parent_circ IS NOT NULL) renews ON renews.target_copy = ac.id 
    LEFT JOIN
        (SELECT id FROM asset.copy WHERE deleted IS FALSE AND status = 9) onorder ON onorder.id = ac.id 
    JOIN 
        asset.call_number acn ON acn.id = ac.call_number
    JOIN
        reporter.super_simple_record ssr ON ssr.id = acn.record 
    LEFT JOIN
        (SELECT id, current_copy FROM action.hold_request WHERE pickup_lib = ' . $org_id . ' AND capture_time IS NOT NULL AND fulfillment_time IS NULL) holds ON holds.current_copy = ac.id
    GROUP BY 1, 2 
   ;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my @holdings;
    while (my @row = $sth->fetchrow_array) {
        push @holdings, {
            bib_id        => $row[0],
            isbns         => $row[1],
            circs         => $row[2],
            renewals      => $row[3],
            holds         => $row[4],
            copies        => $row[5],
            circs_out_now => $row[6],
            onorder       => $row[7]
        };
    }
    return @holdings;
}


sub csv_protect_string {
    my $s = shift;
    $s =~ s/"/""/g;
    if ($s =~ m/[^a-zA-Z0-9]/) { $s = '"' . $s . '"'; }
    return $s;
}

sub connect_db {
    my ($db, $dbuser, $dbpw, $dbhost, $dbport) = @_;

    my $dsn = "dbi:Pg:host=$dbhost;dbname=$db;port=$dbport";

    my $attrs = {
        ShowErrorStatement => 1,
        RaiseError => 1,
        PrintError => 1,
        pg_enable_utf8 => 1,
    };
    my $dbh = DBI->connect($dsn, $dbuser, $dbpw, $attrs);

    return $dbh;
}

sub connect_db_socket {
    my $db = shift;

    my $attrs = {
        ShowErrorStatement => 1,
        RaiseError => 1,
        PrintError => 1,
        pg_enable_utf8 => 1,
        #pg_bool_tf => 1
    };
    my $dbh = DBI->connect("dbi:Pg:dbname=$db", "", "");;
    return $dbh;
}

sub extract_isbns {
    my $str = shift;
    return '' unless defined $str;
    my @isbns = split(/\;/,$str);
    my @cleaned_isbns;
    foreach my $maybe_isbn (@isbns) {
        my $cleaned = norm_isbn($maybe_isbn);
        if (defined $cleaned and $cleaned != '') { push @cleaned_isbns,$cleaned; }
    }
    my @uniq_isbns = uniq(@cleaned_isbns);
    my $r = join(';',@uniq_isbns);
    return $r;
}

sub norm_isbn {
    my $str = shift;
    my $norm = '';
    return '' unless defined $str;
    $str =~ s/-//g;
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;
    $str =~ s/\s+//g;
    $str = lc $str;
    my $isbn;
    if ($str =~ /^(\d{12}[0-9-x])/) {
        $isbn = $1;
        $norm = $isbn;
    } elsif ($str =~ /^(\d{9}[0-9x])/) {
        $isbn =  Business::ISBN->new($1);
        my $isbn13 = $isbn->as_isbn13;
        $norm = lc($isbn13->as_string);
        $norm =~ s/-//g;
    }
    return $norm;
}

sub abort {
    my $msg = shift;
    print STDERR "$0: $msg", "\n";
    print_usage();
    exit 1;
}

sub print_usage {
    print <<_USAGE_;

Switches:

  --org          - required
                 the short org unit name of the org unit to export,
                 exports into one file all descendants that can have 
                 volumes

  --db_host      - required with failover 
  --db_user      - required with failover
  --db_database  - required with failover
  --db_password  - required with failover
  --db_port      - optional, defaults to 5432

  Database failover behavior: if the required database parameters are 
  not sent it will attempt to use a local socket connection.

  --ftp_folder   - optional
  --ftp_host     - required with failover
  --ftp_user     - required with failover
  --ftp_password - optional
  --ftp_port     - optional, defaults to 21 

  FTP failover behavior: if there is no host and user it will generate the 
  file but not attempt to transfer it.  User is required even if it is set 
  to anonymous.

_USAGE_
}
