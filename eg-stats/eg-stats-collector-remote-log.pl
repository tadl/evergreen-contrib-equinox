#!/usr/bin/perl
use Getopt::Long;
use XML::LibXML;
use Sys::Syslog;

my ( $config_file, $timeout, $run_once, $cpu_mode, $mem_mode, $drone_mode,
    $help, $services )
  = ( '/openils/conf/opensrf.xml', 15, 0 );
GetOptions(
    'config=s'  => \$config_file,
    '1'         => \$run_once,
    'cpu'       => \$cpu_mode,
    'ram'       => \$mem_mode,
    'opensrf'   => \$drone_mode,
    'service=s' => \$services,
    'delay=i'   => \$timeout,
    'help'      => \$help
);

help() && exit if ($help);

$cpu_mode = $mem_mode = $drone_mode = 1
  if ( !$cpu_mode && !$mem_mode && !$drone_mode );

my @prev_data;

openlog( 'eg-stats', 'ndelay', 'local0' );

# gather data ...
if ($cpu_mode) {
    open PROCSTAT, '/proc/stat';
    my $line = <PROCSTAT>;
    close PROCSTAT;

    chomp $line;

    @prev_data = split /\s+/, $line;

    sleep($timeout);
}

$services = { map { $_ => 1 } split ',', $services } if ($services);

my @apps;
my @activeapps;
if ($drone_mode) {
    my $parser = XML::LibXML->new();

    # Return an XML::LibXML::Document object
    my $config = $parser->parse_file($config_file);

    @activeapps = $config->findnodes('/opensrf/hosts/*/activeapps/*');
    @apps = $config->findnodes('/opensrf/default/apps/*');

    unless(%$services) {
        %$services = map { $_->textContent => 1 } @activeapps;
    }

}

do {

    if ($cpu_mode) {
        open PROCSTAT, '/proc/stat';
        my $line = <PROCSTAT>;
        close PROCSTAT;

        chomp $line;

        my @current_data = split /\s+/, $line;
        pop @current_data;

        if (@prev_data) {
            my @delta;
            for my $i ( 0 .. 8 ) {
                $delta[$i] = $current_data[$i] - $prev_data[$i];
            }

            my $total = 0;
            $total += $_ for (@delta);

            my $res = sprintf(
                'CPU : ' . "user:"
                  . sprintf( '%0.2f', ( $delta[1] / $total ) * 100 ) . ', '
                  . "idle:"
                  . sprintf( '%0.2f', ( $delta[4] / $total ) * 100 ) . ', '
                  . "iow:"
                  . sprintf( '%0.2f', ( $delta[5] / $total ) * 100 ) . ', '
                  . "steal:"
                  . sprintf( '%0.2f', ( $delta[8] / $total ) * 100 ) . "\n"
            );
            syslog( LOG_INFO, $res );
        }

        @prev_data = @current_data;
    }

    if ($drone_mode) {
        my @data = split /\n/s, `ps ax|grep OpenSRF`;
        my %service_data;
        for (@data) {
            if (/OpenSRF (\w+) \[([^\]]+)\]/) {
                my ( $s, $t ) = ( $2, lc($1) );
                next unless exists $services->{$s};
                if ( !exists( $service_data{$s}{$t} ) ) {
                    $service_data{$s}{$t} = 1;
                }
                else {
                    $service_data{$s}{$t}++;
                }
            }
        }

        for my $s (sort keys %$services) {
            my ($node) = grep { $_->nodeName eq $s } @apps;
            next unless ($node);

            my $max_kids = $node->findvalue('unix_config/max_children');

            my $lcount = $service_data{$s}{listener} || 0;
            my $dcount = $service_data{$s}{drone}    || 0;
            $res = sprintf( "SERVICE ($s) : "
                  . "listener count: $lcount, drone count: $dcount/$max_kids" );

            syslog( LOG_INFO, $res );
        }
    }

    if ($mem_mode) {
        my @memdata = split /\n/s, `cat /proc/meminfo`;
        my %memparts;
        for (@memdata) {
            if (/^(\w+):\s+(\d+)/) {
                $memparts{$1} = $2;
            }
        }

        my $total     = $memparts{MemTotal};
        my $free      = $memparts{MemFree};
        my $buffers   = $memparts{Buffers};
        my $cached    = $memparts{Cached};
        my $available = $free + $buffers + $cached;

        $res = sprintf(
            'RAM : '
              . "total:$total kB, "
              . "free:$free kB, "
              . "buffers:$buffers kB, "
              . "cached:$cached kB, "
              . "available:$available kB, "
              . 'free%:'
              . sprintf( '%0.2f', ( $free / $total ) * 100 ) . ', '
              . 'buffers%:'
              . sprintf( '%0.2f', ( $free / $total ) * 100 ) . ', '
              . 'cached%:'
              . sprintf( '%0.2f', ( $cached / $total ) * 100 ) . ', '
              . 'available%:'
              . sprintf( '%0.2f', ( $available / $total ) * 100 ) . "\n"
        );
        syslog( LOG_INFO, $res );
    }

    $run_once--;
} while ( $run_once != 0 && sleep($timeout) );

sub help {
    print <<HELP;

Evergreen Server Health Monitor

    --config=<config_file>
        OpenSRF configuration file for Evergreen.
        Default: /openils/conf/opensrf.xml
    
    --1
        Run once and stop

    --cpu
        Collect CPU usage stats via /proc/stat

    --ram
        Collect RAM usage stats via /proc/meminfo

    --opensrf
        Collect Evergreen service status info from the output of ps

    --service=<service name>
        Comma separated list of services to report on. If not supplied, all
        services are reported.

    --delay=<seconds>
        Delay time for collecting CPU stats.
        Default: 5

    --help 
        Print this help message

HELP
}

