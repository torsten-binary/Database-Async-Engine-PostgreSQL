use strict;
use warnings;

use feature qw(state);
no indirect;

use Test::More;
use Test::Fatal;
use Future::AsyncAwait;
use IO::Async::Loop;
use Database::Async::Engine::PostgreSQL;
use Log::Any::Adapter qw(TAP);

my $pg = eval {
    require Test::PostgreSQL;
    Test::PostgreSQL->new;
} or plan skip_all => $@;

my $uri = URI->new($pg->uri);

my $loop = IO::Async::Loop->new;

my $db;
is(exception {
    $uri->query_param(sslmode => 'prefer');
    $loop->add(
        $db = Database::Async::Engine::PostgreSQL->new(
            uri => $uri,
        )
    );
}, undef, 'can safely add to the loop');

subtest 'can connect and run a query' => sub {
    my $expected_state = '';
    $db->ready_for_query
       ->subscribe(my $code = sub {
            is(shift, $expected_state, 'readiness state matched');
        });
    my $expect_ready = sub {
        $expected_state = shift;
    };
    is(exception {
        $db->connect->get;
        $expect_ready->('I');
        note 'Await authenticated status';
        $db->authenticated->get;
        note 'Authentication complete';
        $expect_ready->('');
        $db->simple_query(q{select 1})
            ->each(sub {
                is($_, '1', 'had expected result');
            });
        note 'Awaiting idle';
        $expect_ready->('I');
        $db->idle->get;
    }, undef, 'connection works');
    $db->ready_for_query->unsubscribe($code);
};
subtest 'can do more queries' => sub {
    is(exception {
        $db->simple_query(q{select 'example'})
            ->each(sub {
                is($_, 'example', 'had expected result');
            });
        $db->idle->get;
        note 'try for output';
        $db->query(q{select 'output' where 1 = $1}, '1')
            ->each(sub {
                is($_, 'output', 'had expected result');
            })->await;
        note 'try for more output';
        $db->query(q{select 'more output' where 1 = $1}, '0')
            ->each(sub {
                fail('was not expecting anything');
            })->await;

        note 'try for no output';
        $db->query(q{})
            ->each(sub {
                fail('was not expecting anything');
            })->await;

        note 'invalid query';
        $db->query(q{select missing_column from table_not_found})
            ->each(sub {
                fail('was not expecting anything');
            })->completed
            ->on_ready(sub {
                my ($f) = @_;
                note explain [ $f->failure ];
                is($f->state, 'failed', 'failed correctly');
                isa_ok($f->failure, 'Protocol::Database::PostgreSQL::Error');
            })->await;
        note 'in at the end';
    }, undef, 'connection works');
};

done_testing;

