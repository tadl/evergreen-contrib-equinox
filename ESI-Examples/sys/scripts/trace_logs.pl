#!/usr/bin/perl

# Copyright (C) 2014-2015 Equinox Software Inc.
# Jason Etheridge <jason@esilibrary.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

use Getopt::Long;
my $help = 0;
my $man = 0;
my $follow_pid = 0;
my $follow_auth = 0;
my $follow_trace = 0;
my $include_errors = 0;
my $search = '';
GetOptions(
        'follow-pid|follow_pid|pid' => \$follow_pid,
        'follow-auth|follow_auth|auth' => \$follow_auth,
        'follow-trace|follow_trace|trace' => \$follow_trace,
        'include-errors|errors' => \$include_errors,
        'search=s' => \$search,
        'help|?' => \$help);
if ($help || ((@ARGV == 0) && (-t STDIN))) {
    print qq^$0 [--follow-pid] [--follow-auth] [--follow-trace] [--include-errors] [--search="substring"] [logfile1] [logfile2] [...]
or $0 [-p] [-a] [-t] [-e] [-s "substring"] [logfile1] [logfile2] [...]
This script searches the specified (or piped) logfiles and spits out lines containing the "substring".
It optionally parses the logfiles and prints out related lines based on PID, threadtrace, and authtoken
values that it encounters in the matching lines.  --include-errors will pull in lines containing [ERR\n^;
    exit 0;
}
 
my %search_hash = ();
$search_hash{$search} = 1;
if ($include_errors) {
    $search_hash{'[ERR'} = 1;
}
 
while($line = <>) {
    my $found = 0;
    for $term (keys %search_hash) {
        if (index($line,$term) > -1) {
            $found = 1;
            last;
        }
    }
    if ($found) {
        if ($follow_pid) {
            if ($line =~ /^\[.+?\] \S+ \[.+?:(\d+):.+/) {
                $search_hash{$1} = 1;
            }
        }
        if ($follow_auth) {
            if ($line =~ /([0123456789abcdef]{32})/) {
                $search_hash{$1} = 1;
            }
        }
        if ($follow_trace) {
            if ($line =~ /^\[.+?\] \S+ \[.+?:\d+:.+?:\d+:(\d+)\]/) {
                $search_hash{$1} = 1;
            }
        }
        print $line;
    }
}
