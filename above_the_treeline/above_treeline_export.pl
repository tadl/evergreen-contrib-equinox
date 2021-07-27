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

my $org;
my $files = 'circ,order,hold,bib,item';
my $exclude_mods;
my $run_date;
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
    'org:s'          	  => \$org,
    'files:s'             => \$files,
    'exclude_mods:s'      => \$exclude_mods,
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

abort('must specify --org') unless defined $org;
$org = lc($org);
validate_files($files);
my ($sql_date, $print_date) = format_date($run_date);

if ($db_host and $db_user and $db_password and $db_database) { 
        $dbh = connect_db($db_database,$db_user,$db_password,$db_host,$db_port) or abort("Cannot open database at $db_host $!"); 
    } else { 
        $dbh = connect_db_socket($db_database) or abort("Cannot open local socket database connection $!");
};

if (!defined $ftp_user and !defined $ftp_host) {
    print STDERR "Incomplete FTP settings.  No file will be transferred.\n"; }

my $org_id = get_org_id($org);
my $desc_orgs = descendants($org_id,$dbh);

my $circ_file;
my $hold_file;
my $order_file;
my $bib_file;
my $item_file;
my $meta_file;

prep_schema($dbh,$sql_date);

#note that we're using the language in the onboarding doc which can be a bit misleading in Evergreen context
#notably, the item file contains circ and item data, while circ is an aggregate of sources including statuses 
if ($files =~ 'item') {
    $item_file = 'Items_' . $org . '_' . $print_date . '.csv';
    open my $fh, '>', $item_file or die "Can not open $item_file.\n";
    aggregate_items($dbh,$desc_orgs,$exclude_mods,$sql_date);
    aggregate_circs($dbh,$sql_date);
    generate_items_file($dbh,$fh);
    close $fh;
    log_event($dbh,'items file generated',$sql_date);
}

if ($files =~ 'circ') {
    $circ_file = 'Circs_' . $org . '_' . $print_date . '.csv';
    open my $fh, '>', $circ_file or die "Can not open $circ_file.\n";
    aggregate_transactions($dbh,$desc_orgs,$exclude_mods,$sql_date);   
    generate_circs_file($dbh,$fh);
    close $fh;
    log_event($dbh,'circs file generated',$sql_date);
}

if ($files =~ 'hold') {
    $hold_file = 'Holds_' . $org . '_' . $print_date . '.csv';
    $meta_file = 'Metarecords_' . $print_date . '.csv';
    open my $fh, '>', $hold_file or die "Can not open $hold_file.\n";
    open my $mfh, '>', $meta_file or die "Can not open $meta_file.\n";
    aggregate_holds($dbh,$desc_orgs,$exclude_mods,$sql_date);
    generate_holds_file($dbh,$fh);
    generate_metarecords_file($dbh,$mfh);
    close $fh;
    close $mfh;
    log_event($dbh,'holds file generated',$sql_date);
    log_event($dbh,'meta records file generated',$sql_date);
}

if ($files =~ 'order') {
    $order_file = 'Orders_' . $org . '_' . $print_date . '.csv';
    open my $fh, '>', $order_file or die "Can not open $order_file.\n";
    aggregate_orders($dbh,$desc_orgs,$exclude_mods,$sql_date);
    generate_orders_file($dbh,$fh);
    close $fh;
    log_event($dbh,'orders file generated',$sql_date);
}

if ($files =~ 'bib') {
    $bib_file = 'Bibs_' . $org . '_' . $print_date . '.csv';
    open my $fh, '>', $bib_file or die "Can not open $bib_file.\n";
    my $bib_table = aggregate_bibs($dbh,$desc_orgs,$exclude_mods,$sql_date,$org);
    generate_bibs_file($dbh,$fh,$bib_table);
    close $fh;
    log_event($dbh,"$bib_table file generated",$sql_date);
}

my $ftp; 
if (defined $ftp_host and defined $ftp_user) {
    $ftp = connect_ftp($ftp_host,$ftp_user,$ftp_password,$ftp_port,$ftp_folder);
    if ($files =~ 'item') { put_file($item_file,$ftp,$sql_date); }
    if ($files =~ 'circ') { put_file($circ_file,$ftp,$sql_date); }
    if ($files =~ 'hold') { put_file($hold_file,$ftp,$sql_date); }
    if ($files =~ 'order') { put_file($order_file,$ftp,$sql_date); }
    if ($files =~ 'bib') { put_file($bib_file,$ftp,$sql_date); }
    log_event($dbh,'files transferred',$sql_date);
}

log_event($dbh,'process complete',$sql_date);

# ============ beyond here the subs  

sub put_file {
    my ($file,$ftp,$log_fh) = @_;
    $ftp->put($file) or abort("Can not transfer $file.\n");
}

sub prep_schema {
    my ($dbh, $sql_date) = @_;

    #little awkward but some systems are still on older postgres versions without not exists create schema
    my $sql =  'DO $$
                DECLARE
                    x   BOOLEAN;
                BEGIN
                SELECT EXISTS (SELECT * FROM pg_catalog.pg_namespace WHERE nspname = \'edelweiss\') INTO x;
                IF x = FALSE THEN CREATE SCHEMA edelweiss;
                    END IF;
                END $$;
                CREATE TABLE IF NOT EXISTS edelweiss.log (id SERIAL, run_date TEXT, event TEXT, event_time TIMESTAMP DEFAULT NOW());';
    my $sth = $dbh->prepare($sql);
    $sth->execute();

    log_event($dbh,"process started",$sql_date); 

    return;
}

sub log_event {
    my ($dbh, $str, $date) = @_;

    my $sql = 'INSERT INTO edelweiss.log (event, run_date) VALUES (\'' . $str  . '\',\'' . $date . '\');';
    my $sth = $dbh->prepare($sql);
    $sth->execute();

}

sub validate_files {
    my $files = shift;

    my @validfiles = ('order','circ','item','hold','bib');
    my @filelist = split(',',$files);
    foreach my $f (@filelist) {
        if ("@validfiles" =~ /$f/) { next; }
            else { abort('invalid --files option') };
    }
    return;
}

sub validate_date {
    my $str = shift;
    
    if (length($str) != 8) { abort("date should be exactly eight digits long in format YYYYMMDD"); }

    my ($year, $month, $day) = unpack "A4 A2 A2", $str;

    eval{ timelocal(0,0,0,$day, $month-1, $year); 
          1;  } or abort("invalid --date option");
}


sub aggregate_circs {
    my ($dh, $sql_date) = @_;

    my $sql = 'DROP TABLE IF EXISTS edelweiss.circs;'; 
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'circs dropped',$sql_date);

    $sql = 'CREATE UNLOGGED TABLE edelweiss.circs AS SELECT target_copy AS ac_id, COUNT(id) AS all_circs 
                FROM (SELECT id, target_copy FROM action.circulation WHERE xact_start::DATE < \'' . $sql_date .  '\'::DATE 
                    UNION ALL SELECT id, target_copy FROM action.aged_circulation WHERE xact_start::DATE < \'' . $sql_date .  '\'::DATE ) c 
                WHERE target_copy IN (SELECT ac_id FROM edelweiss.items) GROUP BY 1;
            CREATE INDEX edelweiss_circs_acidx ON edelweiss.circs(ac_id);
            ALTER TABLE edelweiss.circs ADD COLUMN monthly_circs INTEGER, ADD COLUMN annual_circs INTEGER, 
                ADD COLUMN last_checkin TIMESTAMP, ADD COLUMN last_due TIMESTAMP, ADD COLUMN last_circ TIMESTAMP;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'initial circs aggregated',$sql_date);

    $sql = 'UPDATE edelweiss.circs a SET monthly_circs = b.x_count FROM 
        (SELECT target_copy AS ac_id, COUNT(id) AS x_count 
            FROM (SELECT id, target_copy FROM action.circulation WHERE xact_start::DATE > \'' . $sql_date .  '\'::DATE - interval \'1 month\' 
                UNION ALL SELECT id, target_copy FROM action.aged_circulation WHERE xact_start::DATE > \'' . $sql_date .  '\'::DATE - interval \'1 month\') c 
            GROUP BY 1) b
        WHERE b.ac_id = a.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'monthly circs added',$sql_date);

    $sql = 'UPDATE edelweiss.circs a SET annual_circs = b.x_count FROM 
        (SELECT target_copy AS ac_id, COUNT(id) AS x_count 
            FROM (SELECT id, target_copy FROM action.circulation WHERE xact_start::DATE > \'' . $sql_date .  '\'::DATE - interval \'1 year\' 
                UNION ALL SELECT id, target_copy FROM action.aged_circulation WHERE xact_start::DATE > \'' . $sql_date .  '\'::DATE - interval \'1 year\') c 
            GROUP BY 1) b
        WHERE b.ac_id = a.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'annual circs added',$sql_date);

    $sql = 'UPDATE edelweiss.circs a SET last_checkin = b.checkin_date FROM 
        (SELECT target_copy AS ac_id, MAX(DATE(checkin_time)) AS checkin_date 
            FROM (SELECT id, target_copy, checkin_time FROM action.circulation WHERE checkin_time IS NOT NULL AND xact_start::DATE < \'' . $sql_date .  '\'::DATE
                UNION ALL SELECT id, target_copy, checkin_time FROM action.aged_circulation WHERE checkin_time IS NOT NULL AND xact_start::DATE < \'' . $sql_date .  '\'::DATE) c
            GROUP BY 1) b
        WHERE b.ac_id = a.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'most recent checkin added',$sql_date);

    $sql = 'UPDATE edelweiss.circs a SET last_due = b.due_date FROM 
        (SELECT target_copy AS ac_id, MAX(DATE(due_date)) AS due_date FROM (SELECT id, target_copy, due_date FROM action.circulation WHERE xact_start::DATE < \'' . $sql_date .  '\'::DATE
            UNION ALL SELECT id, target_copy, due_date FROM action.aged_circulation WHERE xact_start::DATE < \'' . $sql_date .  '\'::DATE) c 
            GROUP BY 1) b
        WHERE b.ac_id = a.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'most recent due date added',$sql_date);

    $sql = 'UPDATE edelweiss.circs a SET last_circ = b.last_circ FROM 
        (SELECT target_copy AS ac_id, MAX(DATE(xact_start)) AS last_circ 
                FROM (SELECT id, target_copy, xact_start FROM action.circulation WHERE xact_start::DATE < \'' . $sql_date .  '\'::DATE
                UNION ALL SELECT id, target_copy, xact_start FROM action.aged_circulation WHERE xact_start::DATE < \'' . $sql_date .  '\'::DATE) c 
            GROUP BY 1) b
        WHERE b.ac_id = a.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'most recent circ added',$sql_date);

    return;
}

sub aggregate_items {
    my ($dbh,$desc_orgs,$exclude_mods,$sql_date) = @_;

    if (!defined $exclude_mods) {
            $exclude_mods = '\'\'';
        } else {
            my @mods = split(/,/,$exclude_mods);
            my @str_mods = map {'\'' . $_ . '\''} @mods;
            $exclude_mods = join(",",@str_mods); 
        }

    my $sql = 'DROP TABLE IF EXISTS edelweiss.items;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'items dropped',$sql_date);

    #item status may not be correct when running on past dates as the auditor look up would be prohibitive for the value
    $sql = 'CREATE UNLOGGED TABLE edelweiss.items AS 
                SELECT ac.id AS ac_id, ac.barcode, ac.create_date, acl.name AS copy_location, aou.shortname AS library, 
                    ac.call_number AS acn_id, ac.circ_modifier, ccs.name AS status
                FROM asset.copy ac 
                JOIN asset.copy_location acl ON acl.id = ac.location 
                JOIN actor.org_unit aou ON aou.id = ac.circ_lib 
                JOIN config.copy_status ccs ON ccs.id = ac.status
                WHERE ac.deleted IS FALSE AND acl.circulate IS TRUE AND ac.circulate IS TRUE 
                AND ac.circ_lib IN (' . $desc_orgs  . ') AND ac.circ_modifier NOT IN (' . $exclude_mods . ') 
                AND create_date::DATE < \'' . $sql_date .  '\'::DATE;
                CREATE INDEX edelweiss_items_acidx ON edelweiss.items(ac_id);
                ALTER TABLE edelweiss.items ADD COLUMN fund TEXT, ADD COLUMN call_number TEXT, 
                    ADD COLUMN biblio_id INTEGER, ADD COLUMN eans TEXT;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'initial items aggregated',$sql_date);

    $sql = 'UPDATE edelweiss.items a SET call_number = BTRIM(CONCAT_WS(\' \',b.pre, b.label, b.suf)), biblio_id = b.record 
            FROM (select acn.id, acn.record, acn.label, pre.label AS pre, suf.label AS suf 
                    FROM asset.call_number acn 
                    LEFT JOIN asset.call_number_prefix pre ON pre.id = acn.prefix
                    LEFT JOIN asset.call_number_suffix suf ON suf.id = acn.suffix
                  ) b WHERE b.id = a.acn_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'volume information added',$sql_date);

    $sql = 'CREATE INDEX edelweiss_items_biblioidx ON edelweiss.items(biblio_id);'; 
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'biblio index added',$sql_date);

    $sql = 'UPDATE edelweiss.items a SET fund = b.fund_name
            FROM (SELECT acql.eg_copy_id, f.name AS fund_name FROM acq.lineitem_detail acql JOIN acq.fund f ON f.id = acql.fund) b
            WHERE b.eg_copy_id = a.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'fund names added',$sql_date);

    $sql = 'UPDATE edelweiss.items a SET eans = ARRAY_TO_STRING(b.isbn,\',\') 
            FROM reporter.super_simple_record b WHERE a.biblio_id = b.id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'eans added',$sql_date);

    return;
}

sub aggregate_orders {
    my ($dbh,$desc_orgs,$exclude_mods,$sql_date) = @_;

    if (!defined $exclude_mods) {
            $exclude_mods = '\'\'';
        } else {
            my @mods = split(/,/,$exclude_mods);
            my @str_mods = map {'\'' . $_ . '\''} @mods;
            $exclude_mods = join(",",@str_mods);
        }
        
    my $sql = 'DROP TABLE IF EXISTS edelweiss.orders;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'edelweiss.orders dropped',$sql_date);       
              
    my $sql = 'CREATE TABLE edelweiss.orders AS 
        SELECT l.eg_bib_id AS biblio_id, COUNT(ld.id) AS order_count, aou.shortname AS branch
        FROM acq.purchase_order po 
        JOIN acq.lineitem l ON l.purchase_order = po.id 
        JOIN acq.lineitem_detail ld ON ld.lineitem = l.id 
        JOIN actor.org_unit aou ON aou.id = ld.owning_lib
        WHERE po.ordering_agency IN (' . $desc_orgs  . ')
        AND l.state IN (\'new\',\'on-order\') GROUP BY 1, 3;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'edelweiss.orders created',$sql_date); 
    
}

sub aggregate_holds {
    my ($dbh,$desc_orgs,$exclude_mods,$sql_date) = @_;

    if (!defined $exclude_mods) {
            $exclude_mods = '\'\'';
        } else {
            my @mods = split(/,/,$exclude_mods);
            my @str_mods = map {'\'' . $_ . '\''} @mods;
            $exclude_mods = join(",",@str_mods);
        }

    my $sql = 'DROP TABLE IF EXISTS edelweiss.holds_items;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'edelweiss.holds_items dropped',$sql_date);

    $sql = 'DROP TABLE IF EXISTS edelweiss.holds;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'edelweiss.holds dropped',$sql_date);

    $sql = 'CREATE UNLOGGED TABLE edelweiss.holds_items AS 
                SELECT ac.id AS ac_id, ac.barcode, ac.call_number AS acn_id
                FROM asset.copy ac 
                JOIN asset.copy_location acl ON acl.id = ac.location 
                WHERE acl.circulate IS TRUE AND ac.circulate IS TRUE 
                AND ac.circ_lib IN (' . $desc_orgs  . ') AND ac.circ_modifier NOT IN (' . $exclude_mods . ') 
                AND create_date::DATE < \'' . $sql_date .  '\'::DATE;
                CREATE INDEX edelweiss_holdsitems_acidx ON edelweiss.holds_items(ac_id);
                ALTER TABLE edelweiss.holds_items ADD COLUMN biblio_id INTEGER;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'initial transaction items aggregated',$sql_date);

    $sql = 'CREATE UNLOGGED TABLE edelweiss.holds
        (id SERIAL, hold_type TEXT, target INTEGER, biblio_id INTEGER, 
         holds_branch TEXT, current_copy INTEGER);';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'empty holds table created',$sql_date);

    $sql = 'UPDATE edelweiss.holds_items a SET biblio_id = b.record FROM asset.call_number b WHERE b.id = a.acn_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'bib ids added to holds item list',$sql_date);

    $sql = 'INSERT INTO edelweiss.holds 
        (hold_type, target, holds_branch, current_copy)
        SELECT ahr.hold_type, ahr.target, aou.shortname, ahr.current_copy
        FROM action.hold_request ahr
        JOIN actor.org_unit aou ON aou.id = ahr.pickup_lib
        WHERE ahr.request_time < \'' . $sql_date .  '\'::DATE
        AND ahr.pickup_lib IN (' . $desc_orgs  . ')
        AND (ahr.cancel_time > \'' . $sql_date .  '\'::DATE OR ahr.cancel_time IS NULL)
        AND (ahr.capture_time > \'' . $sql_date .  '\'::DATE OR ahr.capture_time IS NULL)
        AND (ahr.fulfillment_time > \'' . $sql_date .  '\'::DATE OR ahr.fulfillment_time IS NULL)
        ;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'holds table populated',$sql_date);

    $sql = 'UPDATE edelweiss.holds SET biblio_id = target WHERE hold_type = \'T\' AND 
        target IN (SELECT DISTINCT biblio_id FROM edelweiss.holds_items);';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'holds biblio_id set for title holds',$sql_date);

    $sql = 'UPDATE edelweiss.holds a SET biblio_id = b.biblio_id 
                FROM edelweiss.holds_items b WHERE a.hold_type = \'V\' 
                AND a.target = b.acn_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'holds biblio_id set for volume holds',$sql_date);

    $sql = 'UPDATE edelweiss.holds a SET biblio_id = b.biblio_id 
                FROM edelweiss.holds_items b WHERE a.hold_type IN (\'C\',\'F\',\'R\') 
                AND a.target = b.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'holds biblio_id set for copy holds',$sql_date);

    $sql = 'UPDATE edelweiss.holds a SET biblio_id = b.biblio_id 
                FROM edelweiss.holds_items b, asset.copy_part_map p 
                WHERE a.hold_type = \'P\'  AND p.part = a.target
                AND p.target_copy = b.ac_id;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'holds biblio_id set for part holds',$sql_date);

    #grab lowest id for metarecord holds with no specific format 
    $sql = 'UPDATE edelweiss.holds a SET biblio_id = b.biblio_id
                FROM edelweiss.holds_items b 
                WHERE a.hold_type = \'M\' AND a.current_copy IS NOT NULL
                AND b.ac_id = a.current_copy;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'holds biblio_id set for metarecord holds with current copy',$sql_date);

    #now metarecords without a current copy, gives a value and could shift
    $sql = 'UPDATE edelweiss.holds a SET biblio_id = b.source
                FROM metabib.metarecord_source_map b
                WHERE a.hold_type = \'M\' AND a.current_copy IS NULL
                AND b.metarecord = a.target;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'holds biblio_id set for metarecord holds without current copy',$sql_date);

    $sql = 'DELETE FROM edelweiss.holds WHERE biblio_id IS NULL;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'delete holds with no valid targets',$sql_date);

    $sql = 'DELETE FROM edelweiss.holds WHERE hold_type = \'M\' AND target NOT IN 
            (SELECT DISTINCT metarecord FROM metabib.metarecord_source_map);';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'delete invalid metarecord holds',$sql_date);

    #we are skipping issuance holds because serials are outside the scope
    
    return;
}

sub aggregate_bibs {
    my ($dbh,$desc_orgs,$exclude_mods,$sql_date,$org) = @_;
    my $norm_org = $org;
    $norm_org =~ s/[^a-zA-Z]//;  
    my $bib_table = 'edelweiss.bibs_' . $norm_org;
    my $bib_items_table = 'edelweiss.bib_items_' . $norm_org;

    if (!defined $exclude_mods) {
            $exclude_mods = '\'\'';
        } else {
            my @mods = split(/,/,$exclude_mods);
            my @str_mods = map {'\'' . $_ . '\''} @mods;
            $exclude_mods = join(",",@str_mods);
        }

    my $sql = 'CREATE UNLOGGED TABLE IF NOT EXISTS ' . $bib_table . '  
        (biblio_id INTEGER, eans TEXT[], material_type TEXT[], title TEXT
        ,author TEXT, series TEXT[], pub_date TEXT[], publisher_supplier TEXT[], price TEXT[]
        ,last_update TIMESTAMP);';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,"$bib_table  table created if not exists",$sql_date);

    my $sql = 'DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN (\'i\',\'\') AND c.relname = \'edelweiss_bibs_' . $norm_org . '_bibidx\' AND n.nspname = \'edelweiss\'
            ) THEN
                CREATE INDEX edelweiss_bibs_' . $norm_org . '_bibidx ON edelweiss.bibs_' . $norm_org . ' (biblio_id);
            END IF;
        END$$;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,"$bib_table index created if not exists",$sql_date);

    $sql = 'DROP TABLE IF EXISTS ' . $bib_items_table . ' ;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,"$bib_items_table dropped if exists",$sql_date);

    #generate item list independent of items since we don't care about dates and they want to run just this separate
    $sql = 'CREATE UNLOGGED TABLE ' . $bib_items_table . '  AS 
                SELECT DISTINCT acn.record AS biblio_id
                FROM asset.copy ac 
                JOIN asset.copy_location acl ON acl.id = ac.location 
                JOIN asset.call_number acn ON acn.id = ac.call_number
                WHERE ac.deleted IS FALSE AND acl.circulate IS TRUE AND ac.circulate IS TRUE 
                AND ac.circ_lib IN (' . $desc_orgs  . ') AND ac.circ_modifier NOT IN (' . $exclude_mods . ');
                CREATE INDEX edelweiss_bibitems_' . $norm_org . '_bibidx ON ' . $bib_items_table . ' (biblio_id);';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,"create $bib_items_table as list of bibs with active items",$sql_date);


    $sql = 'DELETE FROM ' . $bib_table . ' WHERE biblio_id IN (SELECT id FROM biblio.record_entry WHERE deleted IS TRUE);';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,"delete $bib_table entries where bibs are now deleted",$sql_date);

    $sql = 'INSERT INTO ' . $bib_table . ' (biblio_id) 
        SELECT DISTINCT a.biblio_id FROM ' . $bib_items_table . '  a
        LEFT JOIN ' . $bib_table . ' b ON a.biblio_id = b.biblio_id
        WHERE b.biblio_id IS NULL;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,"add missing rows to $bib_table based on entries in $bib_items_table",$sql_date);

    $sql = 'UPDATE ' . $bib_table . ' a SET last_update = NULL 
            FROM biblio.record_entry bre 
            WHERE a.biblio_id = bre.id 
            AND a.last_update IS NOT NULL AND a.last_update < bre.edit_date;'; 
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,"$bib_table set to no last update for inclusion in refresh list",$sql_date);
    
    my $i = get_update_count($dbh,$sql_date,$bib_table);
    while ($i > 0) {
        $i = update_bib_info($dbh,$bib_table);
    }
    
    log_event($dbh,"$bib_table refreshed",$run_date);

    return $bib_table;
}

sub update_bib_info {
    my ($dbh,$bib_table) = @_;

    my $sql = 'DO $$
            DECLARE
                ta          TEXT;
                tb          TEXT;
                tp          TEXT;
                xtitle      TEXT;
                xeans       TEXT[];
                xmats       TEXT[];
                xauthor     TEXT;
                xseries     TEXT[];
                xdate       TEXT[];
                xpub        TEXT[];
                xprice      TEXT[];
                x           INTEGER;
                bre_marc    TEXT;
            BEGIN
                SELECT biblio_id FROM ' . $bib_table . '  WHERE last_update IS NULL LIMIT 1 INTO x;
                SELECT marc FROM biblio.record_entry WHERE id = x INTO bre_marc;
                SELECT oils_xpath_string( \'//*[@tag="245"]/*[@code="a"]\', bre_marc) INTO ta;
                SELECT oils_xpath_string( \'//*[@tag="245"]/*[@code="b"]\', bre_marc) INTO tb;
                SELECT oils_xpath_string( \'//*[@tag="245"]/*[@code="p"]\', bre_marc) INTO tp;
                xtitle = BTRIM(CONCAT_WS(\' \',ta,tb,tp));
                SELECT isbn, author FROM reporter.super_simple_record WHERE id = x INTO xeans, xauthor;
                SELECT ARRAY_AGG(raf.value) FROM metabib.record_attr_flat raf WHERE raf.attr = \'search_format\' AND  id = x INTO xmats;
                SELECT ARRAY_AGG(oils_xpath_string( \'//*[@tag="490"]/*[@code="a"]\', bre_marc)) INTO xseries;
                SELECT ARRAY_AGG(oils_xpath_string( \'//*[@tag="260" or @tag="264"]/*[@code="c"]\', bre_marc)) INTO xdate;
                SELECT ARRAY_AGG(oils_xpath_string( \'//*[@tag="260" or @tag="264"]/*[@code="b"]\', bre_marc)) INTO xpub;
                SELECT ARRAY_AGG(oils_xpath_string( \'//*[@tag="020" or @tag="024"]/*[@code="c"]\', bre_marc)) INTO xprice;
                UPDATE ' . $bib_table . ' SET title = xtitle, eans = xeans, material_type = xmats, author = xauthor
                   ,pub_date = xdate, series = xseries, publisher_supplier = xpub, price = xprice ,last_update = NOW() WHERE biblio_id = x;
            END $$;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    
    my $i = get_update_count($dbh,$run_date,$bib_table);      
    return $i;
}
        
sub get_update_count {
    my ($dbh,$sql_date,$bib_table) = @_;
 
    my $sql = 'SELECT COUNT(biblio_id) FROM ' . $bib_table . '  WHERE last_update IS NULL;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    
    my $i = 0;
    while (my @row = $sth->fetchrow_array) {
        $i = $row[0];
    } 

    if ( ($i%500) != 0 ) { } else {
        print "$i bibs remaining to be updated\n";    
        log_event($dbh,"$bib_table $i bibs remaining to be updated",$sql_date);
    }
    return $i;
}

sub aggregate_transactions {
    my ($dbh,$desc_orgs,$exclude_mods,$sql_date) = @_;

    if (!defined $exclude_mods) {
            $exclude_mods = '\'\'';
        } else {
            my @mods = split(/,/,$exclude_mods);
            my @str_mods = map {'\'' . $_ . '\''} @mods;
            $exclude_mods = join(",",@str_mods); 
        }

    my $sql = 'DROP TABLE IF EXISTS edelweiss.trans_items;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'transaction items dropped',$sql_date);

    my $sql = 'DROP TABLE IF EXISTS edelweiss.transactions;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'transactions dropped',$sql_date);

    $sql = 'CREATE UNLOGGED TABLE edelweiss.trans_items AS 
                SELECT ac.id AS ac_id, ac.barcode, ac.call_number AS acn_id, ac.edit_date, ac.deleted, ac.circ_lib, ac.status
                FROM asset.copy ac 
                JOIN asset.copy_location acl ON acl.id = ac.location 
                WHERE acl.circulate IS TRUE AND ac.circulate IS TRUE 
                AND ac.circ_lib IN (' . $desc_orgs  . ') AND ac.circ_modifier NOT IN (' . $exclude_mods . ') 
                AND create_date::DATE < \'' . $sql_date .  '\'::DATE;
                CREATE INDEX edelweiss_transitems_acidx ON edelweiss.trans_items(ac_id);
                ALTER TABLE edelweiss.trans_items ADD COLUMN biblio_id INTEGER;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'initial transaction items aggregated',$sql_date);

    $sql = 'CREATE UNLOGGED TABLE edelweiss.transactions 
        (id SERIAL, ac_id INTEGER, barcode TEXT, biblio_id INTEGER, transaction_type TEXT, transaction_date TIMESTAMP, transaction_branch TEXT, due_date TIMESTAMP)';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'empty transactions table created',$sql_date);
    
    $sql = 'UPDATE edelweiss.trans_items a SET biblio_id = b.record FROM asset.call_number b WHERE b.id = a.acn_id ;';
    $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'bib ids added to transaction item list',$sql_date);

    my $sql = 'INSERT INTO edelweiss.transactions 
        (ac_id, barcode, biblio_id, transaction_type, transaction_date, transaction_branch, due_date)
        SELECT i.ac_id, i.barcode, i.biblio_id, CASE WHEN parent_circ IS NOT NULL THEN \'renewal\' ELSE \'circ\' END, 
            acirc.xact_start, aou.shortname, acirc.due_date
        FROM edelweiss.trans_items i
        JOIN action.circulation acirc ON acirc.target_copy = i.ac_id
        JOIN actor.org_unit aou ON aou.id = acirc.circ_lib 
        WHERE acirc.xact_start > \'' . $sql_date .  '\'::DATE - interval \'1 day\' 
        AND acirc.xact_start < \'' . $sql_date .  '\'::DATE;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'circs inserted to transactions table',$sql_date);
    
    my $sql = 'INSERT INTO edelweiss.transactions 
        (ac_id, barcode, biblio_id, transaction_type, transaction_date, transaction_branch, due_date)
        SELECT i.ac_id, i.barcode, i.biblio_id, \'checkin\', 
            acirc.checkin_time, aou.shortname, NULL
        FROM edelweiss.trans_items i
        JOIN action.circulation acirc ON acirc.target_copy = i.ac_id
        JOIN actor.org_unit aou ON aou.id = acirc.circ_lib 
        WHERE acirc.checkin_time > \'' . $sql_date .  '\'::DATE - interval \'1 day\' 
        AND acirc.checkin_time < \'' . $sql_date .  '\'::DATE;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'checkins inserted to transactions table',$sql_date);

    my $sql = 'INSERT INTO edelweiss.transactions 
        (ac_id, barcode, biblio_id, transaction_type, transaction_date, transaction_branch, due_date)
        SELECT i.ac_id, i.barcode, i.biblio_id, \'deleted\', i.edit_date, aou.shortname, NULL
        FROM edelweiss.trans_items i
        JOIN actor.org_unit aou ON aou.id = i.circ_lib 
        WHERE i.edit_date > \'' . $sql_date .  '\'::DATE - interval \'1 day\' 
        AND i.edit_date < \'' . $sql_date .  '\'::DATE AND i.deleted = TRUE;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'deletions inserted to transactions table',$sql_date);
    
    my $sql = 'INSERT INTO edelweiss.transactions 
        (ac_id, barcode, biblio_id, transaction_type, transaction_date, transaction_branch, due_date)
        SELECT i.ac_id, i.barcode, i.biblio_id, 
            CASE
                WHEN i.status =  2 THEN \'Bindery\'
                WHEN i.status =  3 THEN \'Lost\'
                WHEN i.status =  4 THEN \'Missing\'
                WHEN i.status =  5 THEN \'In Process\'
                WHEN i.status =  6 THEN \'In Transit\'
                WHEN i.status =  8 THEN \'On Holds Shelf\'
                WHEN i.status =  9 THEN \'On Order\'
                WHEN i.status = 11 THEN \'Cataloging\'
                WHEN i.status = 13 THEN \'Discard/Weed\'
                WHEN i.status = 14 THEN \'Damaged\'
            END  
            ,i.edit_date, aou.shortname, NULL
        FROM edelweiss.trans_items i
        JOIN actor.org_unit aou ON aou.id = i.circ_lib 
        WHERE i.edit_date > \'' . $sql_date .  '\'::DATE - interval \'1 day\' 
        AND i.edit_date < \'' . $sql_date .  '\'::DATE AND i.status IN (2,3,4,5,6,8,9,11,13,14);';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    log_event($dbh,'status changes inserted to transactions table',$sql_date);

    return;
}

sub generate_items_file {
    my ($dbh, $fh) = @_;

    print $fh "copy_id,barcode,biblio_id,eans,circ_modifier,call_number,copy_location,library,create_date,status,last_circ,last_checkin,last_due,monthly_circs,annual_circs,all_circs,fund\n";
    my $sql = 'SELECT
        i,
        i.ac_id
        ,i.barcode
        ,i.biblio_id
        ,i.eans
        ,i.circ_modifier
        ,i.call_number
        ,i.copy_location
        ,i.library
        ,i.create_date::DATE
        ,i.status
        ,ec.last_circ::DATE
        ,ec.last_checkin::DATE
        ,ec.last_due::DATE
        ,ec.monthly_circs
        ,ec.annual_circs
        ,ec.all_circs
        ,i.fund
    FROM edelweiss.items i
    LEFT JOIN edelweiss.circs ec ON ec.ac_id = i.ac_id;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    while (my @row = $sth->fetchrow_array) {
        my $copy_id         = $row[0];
        my $barcode         = csv_protect_string($row[1]);
        my $biblio_id       = $row[2];
        my $eans            = csv_protect_string($row[3]);
        my $circ_modifier   = csv_protect_string($row[4]);
        my $call_number     = csv_protect_string($row[5]);
        my $copy_location   = csv_protect_string($row[6]);
        my $library         = csv_protect_string($row[7]);
        my $create_date     = $row[8];
        my $status          = csv_protect_string($row[9]);
        my $last_circ       = $row[10];
        my $last_checkin    = $row[11];
        my $last_due        = $row[12];
        my $monthly_circs   = $row[13];
        my $annual_circs    = $row[14];
        my $all_circs       = $row[15];
        my $fund            = csv_protect_string($row[16]);
        print $fh "$copy_id,$barcode,$biblio_id,$eans,$circ_modifier,$call_number,$copy_location,$library,$create_date,$status,$last_circ,$last_checkin,$last_due,$monthly_circs,$annual_circs,$all_circs,$fund\n";
    }
    return;
}

sub generate_orders_file {
    my ($dbh, $fh) = @_;

    my $sql = 'SELECT biblio_id, order_count, branch FROM edelweiss.orders';
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

    print $fh "biblio_id,hold_count,holds_branch\n";
    my $sql = 'SELECT biblio_id, hold_type, COUNT(id), holds_branch 
    FROM edelweiss.holds GROUP BY 1, 2, 4';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    
    while (my @row = $sth->fetchrow_array) {
        my $biblio_id       = $row[0];
        my $hold_type       = $row[1];
        my $hold_count      = $row[2];
        my $holds_branch    = csv_protect_string($row[3]);
        print $fh "$biblio_id,$hold_type,$hold_count,$holds_branch\n";
    }
    return;
}

sub generate_circs_file {
    my ($dbh, $fh) = @_;

    print $fh "copy_id,barcode,biblio_id,transaction_type,transaction_date,transaction_branch,due_date\n";
    my $sql = 'SELECT
        t.ac_id
        ,t.barcode
        ,t.biblio_id
        ,t.transaction_type
        ,t.transaction_date::DATE
        ,t.transaction_branch
        ,t.due_date::DATE
    FROM edelweiss.transactions t;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    while (my @row = $sth->fetchrow_array) {
        my $copyid = $row[0];
        my $barcode = csv_protect_string($row[1]);
        my $biblio_id = $row[2];
        my $trans_type = csv_protect_string($row[3]);
        my $trans_date = $row[4];
        my $trans_branch = csv_protect_string($row[5]);
        my $due_date = $row[6];
        print $fh "$copyid,$barcode,$biblio_id,$trans_type,$trans_date,$trans_branch,$due_date\n";
    }
    return;
}

sub generate_bibs_file {
    my ($dbh, $fh, $bib_table) = @_;

    print $fh "biblio_id,eans,material_type,title,author,series,pub_date,publisher_supplier,price\n";
    my $sql = 'SELECT biblio_id
                ,ARRAY_TO_STRING(eans,\',\')
                ,ARRAY_TO_STRING(material_type,\',\')
                ,title
                ,author
                ,ARRAY_TO_STRING(series,\',\')
                ,ARRAY_TO_STRING(pub_date,\',\')
                ,ARRAY_TO_STRING(publisher_supplier,\',\')
                ,ARRAY_TO_STRING(price,\',\')
    FROM ' . $bib_table . ';';
    my $sth = $dbh->prepare($sql);
    $sth->execute();

    while (my @row = $sth->fetchrow_array) {
        my $biblio_id            = $row[0];
        my $eans                 = csv_protect_string($row[1]);
        my $material_type        = csv_protect_string($row[2]);
        my $title                = csv_protect_string($row[3]);
        my $author               = csv_protect_string($row[4]);
        my $series               = csv_protect_string($row[5]);
        my $pub_date             = csv_protect_string($row[6]);
        my $publisher_supplier   = csv_protect_string($row[7]);
        my $price                = csv_protect_string($row[8]);
        print $fh "$biblio_id,$eans,$material_type,$title,$author,$series,$pub_date,$publisher_supplier,$price\n";
    }
    return;
}

sub generate_metarecords_file {
    my ($dbh, $fh) = @_;

    print $fh "metarecord,biblio_id\n";
    my $sql = 'SELECT DISTINCT metarecord, source FROM metabib.metarecord_source_map 
    WHERE metarecord IN (SELECT DISTINCT target FROM edelweiss.holds WHERE hold_type = \'M\' AND biblio_id IS NOT NULL);';
    my $sth = $dbh->prepare($sql);
    $sth->execute();

    while (my @row = $sth->fetchrow_array) {
        my $metarecord      = $row[0];
        my $biblio_id       = $row[1];
        print $fh "$metarecord,$biblio_id\n";
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

sub get_org_id {
    my ($org_name) = @_;
    my $sql = 'SELECT id FROM actor.org_unit WHERE lower(shortname) = \'' . $org_name . '\';';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my $r;
    while (my @row = $sth->fetchrow_array) {
        $r = $row[0];
    }  
    return $r;
}

sub descendants {
    my ($org_id, $dbh) = @_;
    my $sql = 'SELECT id FROM actor.org_unit_descendants(' . $org_id . ');';
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    my @orgs;
    while (my @row = $sth->fetchrow_array) {
        push @orgs, @row;
    }
    my $str = join(",",@orgs);
    return $str;
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

sub abort {
    my $msg = shift;
    print STDERR "$0: $msg", "\n";
    print_usage();
    exit 1;
}

sub print_usage {
    print <<_USAGE_;

Switches:

  --org         - required
                the short org unit name of the org unit to export, could be a single 
                system or a consortium

  --files       optional, if not defined all will be created
                accepts item|circ|hold|bib|order
                more than one may be specified by using commas 
                e.g.  --files circ,bib

  --exclude_mods - optional, excludes circulation modifiers from export
                   e.g. --exclude_mods "EQUIPMENT,E READER"

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
