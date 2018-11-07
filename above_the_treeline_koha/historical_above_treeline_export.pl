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
use Net::FTP;
use Time::Local;

my $run_date;
my $db_host;
my $db_user;
my $db_database;
my $db_password;
my $db_port = '3306';
my $ftp_folder;
my $ftp_host;
my $ftp_user;
my $ftp_password;
my $ftp_port;
my $output;
my $dbh;

my $ret = GetOptions(
    'run_date:s'          => \$run_date,
    'db_host:s'  	      => \$db_host,
    'db_user:s'    	      => \$db_user,
    'db_database:s'       => \$db_database,
    'db_password:s'  	  => \$db_password,
    'db_port:s'	          => \$db_port,
    'ftp_folder:s'        => \$ftp_folder,
    'ftp_host:s'          => \$ftp_host,
    'ftp_user:s'          => \$ftp_user,
    'ftp_password:s'      => \$ftp_password,
    'ftp_port:s'          => \$ftp_port
);

my ($sql_date, $print_date) = format_date($run_date);

$dbh = connect_db($db_database,$db_user,$db_password,$db_host,$db_port) or abort("Cannot open database at $db_host $!"); 

if (!defined $ftp_user and !defined $ftp_host) { print STDERR "Incomplete FTP settings.  No file will be transferred.\n"; }

my $circ_file;
my $hold_file;
my $order_file;
my $bib_file;
my $item_file;

init_log($dbh,$sql_date);

#aggregate items 
aggregate_items($dbh,$sql_date);
aggregate_transactions($dbh,$sql_date);
aggregate_holds($dbh,$sql_date);
aggregate_orders($dbh,$sql_date);
aggregate_bibs($dbh,$sql_date);

#note that we're using the language in the onboarding doc for the serverice rather than Koha native language
$item_file = 'Items_historical.csv';
open my $fh, '>', $item_file or die "Can not open $item_file.\n";
generate_items_file($dbh,$fh);
close $fh;
log_event($dbh,'items file generated',$sql_date);

$circ_file = 'Circs_historical.csv';
open my $fh, '>', $circ_file or die "Can not open $circ_file.\n";
generate_circs_file($dbh,$fh);
close $fh;
log_event($dbh,'circs file generated',$sql_date);

$hold_file = 'Holds_historical.csv';
open my $fh, '>', $hold_file or die "Can not open $hold_file.\n";
generate_holds_file($dbh,$fh);
close $fh;
log_event($dbh,'holds file generated',$sql_date);

$order_file = 'Orders_historical.csv';
open my $fh, '>', $order_file or die "Can not open $order_file.\n";
generate_orders_file($dbh,$fh);
close $fh;
log_event($dbh,'orders file generated',$sql_date);

$bib_file = 'Bibs_historical.csv';
open my $fh, '>', $bib_file or die "Can not open $bib_file.\n";
generate_bibs_file($dbh,$fh);
close $fh;
log_event($dbh,"bib table file generated",$sql_date);

my $ftp; 
if (defined $ftp_host and defined $ftp_user) {
    $ftp = connect_ftp($ftp_host,$ftp_user,$ftp_password,$ftp_port,$ftp_folder);
    put_file($item_file,$ftp,$sql_date); 
    put_file($circ_file,$ftp,$sql_date); 
    put_file($hold_file,$ftp,$sql_date); 
    put_file($order_file,$ftp,$sql_date); 
    put_file($bib_file,$ftp,$sql_date); 
    log_event($dbh,'files transferred',$sql_date);
}

log_event($dbh,'process complete',$sql_date);

# ============ beyond here be functions  

sub init_log {
    my ($dbh, $sql_date) = @_;

    my $sql =  'CREATE TABLE IF NOT EXISTS edelweiss_log (id SERIAL, run_date TEXT, event TEXT, event_time TIMESTAMP DEFAULT NOW());';
    my $sth = $dbh->prepare($sql);
    $sth->execute();

    log_event($dbh,"process started",$sql_date);
    return;
}


sub put_file {
    my ($file,$ftp,$log_fh) = @_;
    $ftp->put($file) or abort("Can not transfer $file.\n");
}

sub log_event {
    my ($dbh, $str, $date) = @_;

    my $sql = 'INSERT INTO edelweiss_log (event, run_date) VALUES (\'' . $str  . '\',\'' . $date . '\');';
    my $sth = $dbh->prepare($sql);
    $sth->execute();

}

sub validate_date {
    my $str = shift;
    
    if (length($str) != 8) { abort("date should be exactly eight digits long in format YYYYMMDD"); }

    my ($year, $month, $day) = unpack "A4 A2 A2", $str;

    eval{ timelocal(0,0,0,$day, $month-1, $year); 
          1;  } or abort("invalid --date option");
}


sub aggregate_transactions {
    my ($dbh,$sql_date) = @_;

    my $sql = 'DROP TABLE IF EXISTS edelweiss_transactions;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'transactions dropped',$sql_date);

    $sql = 'CREATE TABLE edelweiss_transactions 
        (itemnumber INTEGER, barcode TEXT, biblio_id INTEGER, transaction_type TEXT, transaction_date TIMESTAMP, transaction_branch TEXT, due_date TIMESTAMP);';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'empty transactions table created',$sql_date);

    my $sql = 'INSERT INTO edelweiss_transactions 
        (itemnumber, barcode, biblio_id, transaction_type, transaction_date, transaction_branch, due_date)
        SELECT i.itemnumber, i.barcode, i.biblionumber, \'circ\', iss.issuedate, iss.branchcode, iss.date_due
        FROM items i
        JOIN (SELECT * FROM issues UNION ALL SELECT * FROM old_issues) iss ON iss.itemnumber = i.itemnumber';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'circs inserted to transactions table',$sql_date);

    my $sql = 'INSERT INTO edelweiss_transactions 
        (itemnumber, barcode, biblio_id, transaction_type, transaction_date, transaction_branch, due_date)
        SELECT i.itemnumber, i.barcode, i.biblionumber, \'renewal\', iss.lastreneweddate, iss.branchcode, iss.date_due
        FROM items i
        JOIN (SELECT * FROM issues UNION ALL SELECT * FROM old_issues) iss ON iss.itemnumber = i.itemnumber';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'renewals inserted to transactions table',$sql_date);

    my $sql = 'INSERT INTO edelweiss_transactions 
        (itemnumber, barcode, biblio_id, transaction_type, transaction_date, transaction_branch, due_date)
        SELECT i.itemnumber, i.barcode, i.biblionumber, \'checkin\', iss.returndate, iss.branchcode, NULL
        FROM items i
        JOIN (SELECT * FROM issues UNION ALL SELECT * FROM old_issues) iss ON iss.itemnumber = i.itemnumber';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'checkins inserted to transactions table',$sql_date);

    my $sql = 'INSERT INTO edelweiss_transactions 
        (itemnumber, barcode, biblio_id, transaction_type, transaction_date, transaction_branch, due_date)
        SELECT i.itemnumber, i.barcode, i.biblionumber, \'deleted\', i.timestamp, i.homebranch, NULL
        FROM items i';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'deletions inserted to transactions table',$sql_date);

    return;
}

sub aggregate_items {
    my ($dbh,$sql_date) = @_;

    my $sql = 'DROP TABLE IF EXISTS edelweiss_items;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'items dropped',$sql_date);

    $sql = 'CREATE TABLE edelweiss_items AS 
                SELECT itemnumber AS ac_id, barcode, dateaccessioned AS create_date, permanent_location AS copy_location, 
                    holdingbranch AS library, itemcallnumber as call_number, itype, biblionumber AS biblio_id,
                    CASE 
                        WHEN itemlost = 0 THEN "Not Lost"
                        WHEN itemlost = 1 THEN "Lost"
                        WHEN itemlost = 2 THEN "Long Overdue (lost)"
                        WHEN itemlost = 3 THEN "Lost & Paid For"
                        WHEN itemlost = 4 THEN "Missing"
                        ELSE "Not Lost"
                        END AS status 
                FROM items
                WHERE withdrawn = 0 AND (notforloan = 0 OR notforloan = -1);';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'initial items aggregated',$sql_date);

    $sql = 'CREATE INDEX ed_items_ac_idx ON edelweiss_items (ac_id);';
    $sth = $dbh->prepare($sql);
    $sth->execute();

    $sql = 'ALTER TABLE edelweiss_items ADD COLUMN fund TEXT, ADD COLUMN eans TEXT;';
    $sth = $dbh->prepare($sql);
    $sth->execute();

    $sql = 'UPDATE edelweiss_items i, aqbudgets aqb, aqorders aqo, aqorders_items aqi
            SET i.fund = aqb.budget_name 
            WHERE i.ac_id = aqi.itemnumber AND aqi.ordernumber = aqo.ordernumber
            AND aqo.budget_id = aqb.budget_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'fund names added',$sql_date);

    $sql = 'UPDATE edelweiss_items i, biblioitems bi
            SET i.eans = CONCAT_WS(\',\',bi.isbn,bi.ean)
            WHERE i.ac_id = bi.biblionumber;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'eans added to items',$sql_date);

    $sql = 'ALTER TABLE edelweiss_items ADD COLUMN last_circ DATE, ADD COLUMN last_checkin DATE, ADD COLUMN last_due DATE, 
                ADD COLUMN monthly_circs SMALLINT, ADD COLUMN annual_circs SMALLINT, ADD COLUMN all_circs SMALLINT;';
    $sth = $dbh->prepare($sql);
    $sth->execute();

    $sql = 'UPDATE edelweiss_items a, 
        (SELECT itemnumber AS ac_id, COUNT(issue_id) AS x_count FROM (SELECT issue_id, itemnumber, issuedate FROM issues UNION ALL SELECT issue_id, itemnumber, issuedate FROM old_issues) x 
            GROUP BY 1) b
        SET monthly_circs = b.x_count 
        WHERE b.ac_id = a.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'monthly circs added',$sql_date);
    
    $sql = 'UPDATE edelweiss_items a,
        (SELECT itemnumber AS ac_id, COUNT(issue_id) AS x_count FROM (SELECT issue_id, itemnumber, issuedate FROM issues UNION ALL SELECT issue_id, itemnumber, issuedate FROM old_issues) x 
            GROUP BY 1) b
        SET annual_circs = b.x_count 
        WHERE b.ac_id = a.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'annual circs added',$sql_date);

    $sql = 'UPDATE edelweiss_items a,
		(SELECT itemnumber AS ac_id, COUNT(issue_id) AS x_count FROM 
		(SELECT issue_id, itemnumber FROM issues UNION ALL SELECT issue_id, itemnumber FROM old_issues) x
		GROUP BY 1) b
		SET all_circs = b.x_count
		WHERE b.ac_id = a.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'annual circs added',$sql_date);
 
    $sql = 'UPDATE edelweiss_items a, 
        (SELECT itemnumber AS ac_id, MAX(DATE(returndate)) AS checkin_date FROM 
            (SELECT issue_id, itemnumber, issuedate, returndate FROM issues UNION ALL SELECT issue_id, itemnumber, issuedate, returndate FROM old_issues) q 
            WHERE returndate IS NOT NULL GROUP BY 1) b 
        SET a.last_checkin = b.checkin_date
        WHERE b.ac_id = a.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'most recent checkin added',$sql_date);
    
    $sql = 'UPDATE edelweiss_items a,
        (SELECT itemnumber AS ac_id, MAX(DATE(date_due)) AS due_date FROM (SELECT issue_id, itemnumber, issuedate, date_due FROM issues UNION ALL SELECT issue_id, itemnumber, issuedate, date_due FROM old_issues) x 
            GROUP BY 1) b
        SET a.last_due = b.due_date
        WHERE b.ac_id = a.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'most recent due date added',$sql_date);
    
    $sql = 'UPDATE edelweiss_items a, 
        (SELECT itemnumber AS ac_id, MAX(DATE(issuedate)) AS last_circ FROM (SELECT issue_id, itemnumber, issuedate FROM issues UNION ALL SELECT issue_id, itemnumber, issuedate FROM old_issues) x  
            GROUP BY 1) b
        SET a.last_circ = b.last_circ 
        WHERE b.ac_id = a.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'most recent circ added',$sql_date);

    return;
}

sub aggregate_orders {
    my ($dbh,$sql_date) = @_;

    my $sql = 'DROP TABLE IF EXISTS edelweiss_orders;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'edelweiss_orders dropped',$sql_date);       
              
    my $sql = 'CREATE TABLE edelweiss_orders AS 
        SELECT aq.biblionumber AS biblio_id, COUNT(r.reserve_id) AS hold_count, r.branchcode AS holds_branch
        FROM aqorders aq
        JOIN reserves r ON r.biblionumber = aq.biblionumber
        WHERE aq.datereceived IS NULL GROUP BY 1, 3;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'edelweiss_orders created',$sql_date); 
    
}

sub aggregate_holds {
    my ($dbh,$sql_date) = @_;

    my $sql = 'DROP TABLE IF EXISTS edelweiss_holds;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'edelweiss_holds dropped',$sql_date);

    $sql = 'CREATE TABLE edelweiss_holds
        (reserve_id INTEGER, biblio_id INTEGER, holds_branch TEXT);';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'empty holds table created',$sql_date);

    $sql = 'INSERT INTO edelweiss_holds 
        (reserve_id, biblio_id, holds_branch)
        SELECT reserve_id, biblionumber, branchcode
        FROM reserves 
		UNION ALL
        SELECT reserve_id, biblionumber, branchcode
        FROM old_reserves;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'holds table populated',$sql_date);
   
    return;
}

sub aggregate_bibs {
    my ($dbh,$sql_date) = @_;

    my $sql = 'DROP TABLE IF EXISTS edelweiss_bibs;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,"bibs table dropped if exists",$sql_date);

    $sql = 'CREATE TABLE IF NOT EXISTS edelweiss_bibs  
        (biblio_id INTEGER, eans TEXT, material_type TEXT, title TEXT
        ,author TEXT, series TEXT, pub_date TEXT, publisher_supplier TEXT, price TEXT
        ,last_update TEXT);';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,"bib table table created if not exists",$sql_date);

    $sql = 'INSERT INTO edelweiss_bibs (biblio_id, title, author, series, eans, pub_date, publisher_supplier, material_type, last_update) 
        SELECT b.biblionumber, b.title, b.author, b.seriestitle, CONCAT_WS(\',\',bi.isbn,bi.ean), 
            bi.publicationyear, bi.publishercode, bi.itemtype, DATE(b.timestamp)
        FROM biblio b
        JOIN biblioitems bi ON bi.biblionumber = b.biblionumber 
        ;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,"creating base entires in bibs table",$sql_date);

    $sql = 'UPDATE edelweiss_bibs eb, items i SET eb.price = i.price WHERE eb.biblio_id = i.biblionumber;';
    $sth = $dbh->prepare($sql);
    $sth->execute();

    $sql = 'UPDATE edelweiss_bibs eb, items i SET eb.material_type = i.itype WHERE eb.biblio_id = i.biblionumber AND eb.material_type IS NULL;';
    $sth = $dbh->prepare($sql);
    $sth->execute();

    return;
}
       
sub generate_items_file {
    my ($dbh, $fh) = @_;

    my $sql = 'SELECT
        ac_id
        ,barcode
        ,biblio_id
        ,eans
        ,itype
        ,call_number
        ,copy_location
        ,library
        ,create_date
        ,status
        ,last_circ
        ,last_checkin
        ,last_due
        ,monthly_circs
        ,annual_circs
        ,all_circs
        ,fund
    FROM edelweiss_items;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my @results;

    while (my @row = $sth->fetchrow_array) {
        push @results, {
            copy_id         => $row[0],
            barcode         => csv_protect_string($row[1]),
            biblio_id       => $row[2],
            eans            => csv_protect_string($row[3]),
            itype           => csv_protect_string($row[4]),
            call_number     => csv_protect_string($row[5]),
            copy_location   => csv_protect_string($row[6]),
            library         => csv_protect_string($row[7]),
            create_date     => $row[8],
            status          => csv_protect_string($row[9]),
            last_circ       => $row[10],
            last_checkin    => $row[11],
            last_due        => $row[12],
            monthly_circs   => $row[13],
            annual_circs    => $row[14],
            all_circs       => $row[15],
            fund            => csv_protect_string($row[16])
        };
    }

    print $fh "copy_id,barcode,biblio_id,eans,itype,call_number,copy_location,library,create_date,status,last_circ,last_checkin,last_due,monthly_circs,annual_circs,all_circs,fund\n";
    foreach my $built_hash( @results ) {
        print $fh "$built_hash->{copy_id},";
        print $fh "$built_hash->{barcode},";
        print $fh "$built_hash->{biblio_id},";
        print $fh "$built_hash->{eans},";
        print $fh "$built_hash->{itype},";
        print $fh "$built_hash->{call_number},";
        print $fh "$built_hash->{copy_location},";
        print $fh "$built_hash->{library},";
        print $fh "$built_hash->{create_date},";
        print $fh "$built_hash->{status},";
	    print $fh "$built_hash->{last_circ},";
        print $fh "$built_hash->{last_checkin},";
        print $fh "$built_hash->{last_due},";
        print $fh "$built_hash->{monthly_circs},";
        print $fh "$built_hash->{annual_circs},";
        print $fh "$built_hash->{all_circs},";
        print $fh "$built_hash->{fund}\n";
    }
    return;
}

sub generate_orders_file {
    my ($dbh, $fh) = @_;

    my $sql = 'SELECT biblio_id, hold_count, holds_branch FROM edelweiss_orders';
    my $sth = $dbh->prepare($sql);
    $sth->execute();

    my @results;
    while (my @row = $sth->fetchrow_array) {
        push @results, {
            biblio_id       => $row[0],
            order_count     => $row[1],
            branch          => csv_protect_string($row[2])
        };
    }

    print $fh "biblio_id,hold_count,holds_branch\n";
    foreach my $built_hash( @results ) {
        print $fh "$built_hash->{biblio_id},";
        print $fh "$built_hash->{order_count},";
        print $fh "$built_hash->{branch}\n";
    }
    return;
}

sub generate_holds_file {
    my ($dbh, $fh) = @_;

    my $sql = 'SELECT biblio_id, COUNT(reserve_id), holds_branch 
    FROM edelweiss_holds GROUP BY 1, 3';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    
    my @results;
    while (my @row = $sth->fetchrow_array) {
        push @results, {
            biblio_id       => $row[0],
            hold_count      => $row[1],
            holds_branch    => csv_protect_string($row[2])
        };
    }

    print $fh "biblio_id,hold_count,holds_branch\n";
    foreach my $built_hash( @results ) {
        print $fh "$built_hash->{biblio_id},";
        print $fh "$built_hash->{hold_count},";
        print $fh "$built_hash->{holds_branch}\n";
    }
    return;
}

sub generate_circs_file {
    my ($dbh, $fh) = @_;

    my $sql = 'SELECT
        t.itemnumber
        ,t.barcode
        ,t.biblio_id
        ,t.transaction_type
        ,DATE(t.transaction_date)
        ,t.transaction_branch
        ,DATE(t.due_date)
    FROM edelweiss_transactions t;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my @results;
    while (my @row = $sth->fetchrow_array) {
        push @results, {       
            copy_id         => $row[0],
            barcode         => csv_protect_string($row[1]),
            biblio_id       => $row[2],
            trans_type      => csv_protect_string($row[3]),
            trans_date      => $row[4],
            trans_branch    => csv_protect_string($row[5]),
            due_date        => $row[6]
        };
    }

    print $fh "copy_id,barcode,biblio_id,transaction_type,transaction_date,transaction_branch,due_date\n";
    foreach my $built_hash( @results ) {
        print $fh "$built_hash->{copy_id},";
        print $fh "$built_hash->{barcode},";
        print $fh "$built_hash->{biblio_id},";
        print $fh "$built_hash->{trans_type},";
        print $fh "$built_hash->{trans_date},";
        print $fh "$built_hash->{trans_branch},";
        print $fh "$built_hash->{due_date}\n"; 
    }
    return;
}

sub generate_bibs_file {
    my ($dbh, $fh) = @_;

    my $sql = 'SELECT biblio_id
                ,eans
                ,material_type
                ,title
                ,author
                ,series
                ,pub_date
                ,publisher_supplier
                ,price
    FROM edelweiss_bibs;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();

    my @results;
    while (my @row = $sth->fetchrow_array) {
        push @results, {
            biblio_id            => $row[0],
            eans                 => csv_protect_string($row[1]),
            material_type        => csv_protect_string($row[2]),
            title                => csv_protect_string($row[3]),
            author               => csv_protect_string($row[4]),
            series               => csv_protect_string($row[5]),
            pub_date             => csv_protect_string($row[6]),
            publisher_supplier   => csv_protect_string($row[7]),
            price                => csv_protect_string($row[8])
        };
    }

    print $fh "biblio_id,eans,material_type,title,author,series,pub_date,publisher_supplier,price\n";
    foreach my $built_hash( @results ) {
        print $fh "$built_hash->{biblio_id},";
        print $fh "$built_hash->{eans},";
        print $fh "$built_hash->{material_type},";
        print $fh "$built_hash->{title},";
        print $fh "$built_hash->{author},";
        print $fh "$built_hash->{series},";
        print $fh "$built_hash->{pub_date},";
        print $fh "$built_hash->{publisher_supplier},";
        print $fh "$built_hash->{price}\n";
    }
    return;
}

sub connect_ftp {
    my ($ftp_host,$ftp_user,$ftp_password,$ftp_port,$ftp_folder,$log_fh) = @_;
    my $ftp;

    if (!defined $ftp_port) { $ftp_port = '21' }

    my $ftp = Net::FTP->new($ftp_host, Debug=> 0, Passive => 1, Port=> $ftp_port) or abort("Failure to connect to FTP site.");
    $ftp->login($ftp_user,$ftp_password) or abort("Failure to login to FTP site.");
    log_event($dbh,'ftp server login successful');
    $ftp->binary();
    if (defined $ftp_folder) {
        $ftp->cwd($ftp_folder) or abort("Failure to load specified directory.");
    }
    return $ftp;
}

sub uniq {
    my %seen;
    grep !$seen{$_}++, @_;
}

sub format_date {
    my $run_date = shift;
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
    my $sql_date;
    my $print_date;
    if (!defined $run_date) {
        $year = $year + 1900;
        $mon = $mon + 1;
        if (length($mday) < 2) {$mday = '0' . $mday;}
        if (length($mon) < 2) {$mon = '0' . $mon;}
        $print_date = $year . $mon . $mday;
        $sql_date = $year . '-' . $mon . '-' . $mday;
    } else {
        validate_date($run_date);
        $print_date = $run_date;
        $sql_date = substr($run_date,0,4) . '-' . substr($run_date,4,2) . '-' . substr($run_date,6,2); 
    }

    return ($sql_date,$print_date);
}

sub csv_protect_string {
    my $s = shift;
    $s =~ s|/$||;
	$s =~ s/\s+$//;
    $s =~ s/"/""/g;
    if ($s =~ m/[^a-zA-Z0-9]/) { $s = '"' . $s . '"'; }
    return $s;
}

sub connect_db {
    my ($db, $dbuser, $dbpw, $dbhost, $dbport) = @_;

    my $dsn = "DBI:mysql:database=$db;host=$dbhost;port=$dbport";
    my $dbh = DBI->connect($dsn, $dbuser, $dbpw);

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

sub abort {
    my $msg = shift;
    print STDERR "$0: $msg", "\n";
    print_usage();
    exit 1;
}

sub print_usage {
    print <<_USAGE_;

Switches:

  --run_date    optional, used for generating files as if the script was 
                being run on a previous date, if not supplied it defaults to
                today, note that transactions run for the previous day 
                so if you want transactions for 2018-02-19 supply the date
                it would run as as the 20th, e.g. --run_date 20180220 

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
