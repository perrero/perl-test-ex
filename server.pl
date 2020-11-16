#!/usr/bin/perl
use strict;
use warnings;
use utf8;

use Mojolicious::Lite;
use Mojo::Pg;

use constant {
    DBHOST => 'localhost',
    DBPORT => 5432,
    DBNAME => '',
    DBUSER => '',
    DBPASS => '',
};

helper pg => sub {
	state $pg = Mojo::Pg->new(
		sprintf('postgresql://%s:%s@%s:%s/%s', DBUSER, DBPASS, DBHOST, DBPORT, DBNAME)
	)
};

any '/' => sub {
	my ($c) = @_;

	my $table = [];
	my $address = $c->param('address');
	if ($address) {
		$table = $c->pg->db->query(q{
			WITH internal_ids AS (
				SELECT int_id FROM log WHERE address = ?
			)
			SELECT int_id, created, str FROM log WHERE int_id IN(SELECT int_id FROM internal_ids)
			UNION
			SELECT int_id, created, str FROM message WHERE int_id IN(SELECT int_id FROM internal_ids)

			ORDER BY int_id ASC, created ASC
			LIMIT 101},
			$address,
		)->hashes->to_array;
	}

	my $has_over100 = 0;
	if (scalar @$table > 100) {
		pop @$table;
		$has_over100 = 1;
	}

	$c->render(template => 'index', table => $table, has_over100 => $has_over100);
};

app->start;

__DATA__
@@ index.html.ep
<html>
	<head>
		<title>search page</title>
	</head>
	<body>
		<form method="POST">
			<input type="text" name="address" /><button type="submit">SEARCH</button>
		</form>
		% if (scalar @$table) {
			<pre><table>
			% foreach my $row (@$table) {
				<tr>
					<td><%= $row->{created} %></td>
					<td><%= $row->{str} %></td>
				</tr>
			% }
			</table></pre>
			% if ($has_over100) {
				<p>has over 100 records</p>
			% }
		% }
	</body>
</html>