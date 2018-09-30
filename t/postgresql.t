use strict;
use warnings;

use IO::Async::Loop;
use Database::Async;
use Database::Async::Engine::PostgreSQL;

use Test::More;
use Test::Fatal;
use Test::Deep;
use Test::PostgreSQL;
use Log::Any::Adapter qw(TAP);

my $pg = eval {
    Test::PostgreSQL->new
} or plan skip_all => $@;

note $pg->dsn;
my $uri = Database::Async::Engine::PostgreSQL->uri_for_dsn($pg->dsn);

my $loop = IO::Async::Loop->new;

$loop->add(
    $_
) for my $db = Database::Async->new(
        uri => $uri
    );

subtest 'query as a source' => sub {
    my @items = $db->query(q{
        select 'x' as "field"
        union all
        select 'y' as "field"
        order by "field"
    })
        ->row_hashrefs
        ->map(sub { $_->{field} })
        ->as_list
        ->get;
    cmp_deeply(\@items, [qw(x y)], 'have expected items');
    done_testing;
};

subtest 'connection handling' => sub {
    isa_ok(my $f = $db->do(q{select 1}), 'Future');
    ok(!$f->is_ready, 'query starts off pending');
    is(exception {
        $f->get;
    }, undef, 'simple query completes safely');
    ok($db->connected->is_ready, 'we think we are connected');
    is(exception {
        $db->connected->get;
    }, undef, 'we think the connection succeeded');

    is($db->do(q{select 2})->get, $db, '->do returns the database instance');
    done_testing;
};

done_testing;

