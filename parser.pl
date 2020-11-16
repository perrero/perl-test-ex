#!/usr/bin/perl
use strict;
use warnings;
use utf8;
use feature qw(say);

use constant {
    DBHOST => 'localhost',
    DBPORT => 5432,
    DBNAME => '',
    DBUSER => '',
    DBPASS => '',
};

use Encode::Locale;
if (-t) {
    binmode(STDIN, ":encoding(console_in)");
    binmode(STDOUT, ":encoding(console_out)");
    binmode(STDERR, ":encoding(console_out)");
}

use DBI;
use DateTime::Format::Pg;


my @KNOWN_FLAGS = (
	'<=', '=>', '->', '**', '==',
);
my %KNOWN_FLAGS = map {$_ => 1} @KNOWN_FLAGS;
sub parse_line {
	my $line = shift;
	
	# дата, время
	$line =~ s/^([^\s]+)\s+([^\s]+)\s+//;
	my ($date, $time) = ($1, $2);
	die 'wrong line format: no date or time' unless ($date and $time);
	my $dt = eval {
		return DateTime::Format::Pg->parse_datetime(join(' ', $date, $time));
	};
	if ($@) {
		die 'wrong date or time format:' . join(' ', grep {$_} ($date, $time));
	}

	my @line = split(/\s/, $line);

	# внутренний id сообщения
	# флаг
	# адрес получателя (либо отправителя)
	# другая информация

	my $internal_id = $line[0];
	my $flag = $line[1];

	unless ($flag and exists $KNOWN_FLAGS{$flag}) {
		warn sprintf('Undefined or unknown flag, skip line "%s"', join(' ', $date, $time, $line)) . "\n";
		return undef;
	}

	my $address = $line[2];
	if ($address and $address eq ':blackhole:' and defined $line[3]) {
		($address) = ($line[3] =~ m/^<([^>]+)>$/);
		if ($address) {
			warn sprintf('Using %s instead of :blackhole:', $address) . "\n";
		}
	}

	my $rec = {
		datetime => DateTime::Format::Pg->format_datetime($dt),
		internal_id => $internal_id,
		flag => $flag,
		address => $address,
		line => $line,
	};

	if ($rec->{flag} and $rec->{flag} eq '<=') {
		my ($id) = ($line =~ m/\sid=([^\s]+)/);
		if (defined $id) {
			$rec->{id} = $id;
		}
		else {
			warn sprintf('Can\'t find id field on "%s %s"', $rec->{datetime}, $rec->{line}) . "\n";
			return undef;
		}
	}

	return $rec;
}


sub make_dbh {
    my %dbi_attrs = (
        RaiseError => 1,
    );

    my $dbh = DBI->connect(
        'dbi:Pg:dbname='. DBNAME .';host='. DBHOST .';port='. DBPORT .';',
        DBUSER, DBPASS, \%dbi_attrs
    ) or die $DBI::errstr;

    $dbh->{pg_enable_utf8} = 1;
    $dbh->{AutoCommit} = 1;

    $dbh->do(qq{SET CLIENT_ENCODING = 'UNICODE'}) or die $dbh->errstr;

    return $dbh;
}

# создать дескриптор БД и подготовить пару запросов
my $dbh = make_dbh();
$dbh->do("DELETE FROM message");
$dbh->do("DELETE FROM log");
my $sth_msg = $dbh->prepare(q{
	INSERT INTO message (created, id, int_id, str)
	VALUES (?, ?, ?, ?)
}) or die $dbh->errstr;
my $sth_log = $dbh->prepare(q{
	INSERT INTO log (created, int_id, str, address)
	VALUES (?, ?, ?, ?)
}) or die $dbh->errstr;

open (my $fh, '<encoding(UTF-8)', 'out') or die "Can't open file: $!";
while (<$fh>) {
	chomp;
	my $rec = eval {
		return parse_line($_);
	};
	if ($@) {
		die sprintf("error while parse %s:", $_) . @$;
	}

	if ($rec->{internal_id} and $rec->{flag} and $rec->{flag} eq '<=') {
		if ($rec->{id}) {
			# запись в таблицу messages:
			# created - timestamp строки лога
			# id - значение поля id=xxxx из строки лога
			# int_id - внутренний id сообщения
			# str - строка лога (без временной метки)
			$sth_msg->execute(
				$rec->{datetime},
				$rec->{id},
				$rec->{internal_id},
				$rec->{line},
			) or die $dbh->errstr;
		}
		else {
			# строки без id в таблицу messages не попадают
		}
	}
	elsif ($rec->{internal_id} and $rec->{address}) {
		# запись в таблицу log:
		# created - timestamp строки лога
		# int_id - внутренний id сообщения
		# str - строка лога (без временной метки)
		# address - адрес получателя
		$sth_log->execute(
			$rec->{datetime},
			$rec->{internal_id},
			$rec->{line},
			$rec->{address},
		) or die $dbh->errstr;
	}
	else {
		# строки без адреса (с флагом=Completed) пропускаются, в таблицу log не попадают
	}
}
close($fh) or die "Can't close: $!";

$sth_msg->finish; # don't worry
$sth_log->finish; # don't worry
$dbh->disconnect;

