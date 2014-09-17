#!/usr/bin/perl

# Copyright (C) 2013-2014 Equinox Software Inc.
# Galen Charlton <gmc@esilibrary.com>
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

# This is a simple CGI script for linking to a list of new items in
# an Evergreen catalog.
#
# It accepts a list of one or more Evergreen location IDs (passed via
# the "loc") CGI parameter and an optional count.  It then fetches
# the list of bib IDs of the $count newest items in those locations,
# then generates an HTTP redirect to a TPAC search results page.
#
# To use, place it on the Evergreen web server in a directory that's
# configured to run CGI scripts.
#
# An example of a URL using this script:
#
# http://catalog.example.org/cgi-bin/newitems.cgi?count=30&loc=123&loc=124

use strict;
use warnings;
use CGI;
use LWP::UserAgent;

my $cgi = CGI->new();

my $count = $cgi->param('count') // 10;
$count = 10 unless $count =~ /^\d+$/;
my @locs = $cgi->param('loc');

my $url = 'http://localhost/opac/extras/browse/rss2/item-age/-/?count=' . $count;
if (@locs) {
    $url .= '&' . join('&', map { "copyLocation=$_" } @locs);
}

my $ua = LWP::UserAgent->new();
$ua->timeout(10);
my $resp = $ua->get($url);

my $xml = $resp->decoded_content;
my @ids = ($xml =~ m!biblio-record_entry/(\d+)!g);

my $redirect  = '/eg/opac/results?query=record_list(';
$redirect    .= join(',', @ids);
$redirect    .= ')%20sort(edit_date)#descending&amp;locg=1';
print $cgi->redirect($redirect);
