#!/usr/bin/perl -w

#    Copyright (C) 2011-2012 Equinox Software Inc.
#    Ben Ostrowsky <ben@esilibrary.com>
#    Galen Charlton <gmc@esilibrary.com>
#
#    Original version sponsored by the King County Library System
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, write to the Free Software
#    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

use strict;
use Email::Sender::Simple qw(sendmail);
use Email::Simple;
use Email::Simple::Creator;
use Email::Sender::Transport::SMTP qw(); 
use Getopt::Long;

my $to = 'esi@localhost.localdomain';
my $from = $to;
my $subject = "";
my $body = "";

my $result = GetOptions (
	"to=s"   => \$to,
	"from=s" => \$from,
	"subject=s" => \$subject
);

my $transport = Email::Sender::Transport::SMTP->new ({
	host => 'smtp.example.org',
	port => 25
});

while (<>) { $body .= $_; }

my $email = Email::Simple->create(
	header => [
		To      => $to,
		From    => $from,
		Subject => $subject,
	],
	body => $body,
);

sendmail($email, { transport => $transport });

=head1 NAME

send-email.pl

=head1 USAGE

echo Hello world! | \

	send-email.pl \

		--from '"Cron" <collectionHQ@example.org>' \

		--to '"User" <user@example.com>, "Someone Else" <someone@else>' \

		--subject "Stuff"
