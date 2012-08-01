#!/usr/bin/perl -w

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
