package Database::Async::Engine::PostgreSQL;
# ABSTRACT: PostgreSQL support for Database::Async

use strict;
use warnings;

our $VERSION = '0.001';

use parent qw(Database::Async::Engine);

=head1 NAME

Database::Async::Engine::PostgreSQL - support for PostgreSQL databases in L<Database::Async>

=head1 DESCRIPTION

=cut

use curry;
use Scalar::Util ();
use Protocol::PostgreSQL::Client '0.008';

use Log::Any qw($log);

use overload
    '""' => sub { ref(shift) },
    bool => sub { 1 },
    fallback => 1;

Database::Async::Engine->register_class(
    postgresql => __PACKAGE__
);

sub connect {
    my ($self) = @_;

    # Initial connection is made directly through the URI
    # parameters. Eventually we also want to support UNIX
    # socket and other types.
    my $uri = $self->uri;
    my $endpoint = join ':', $uri->host, $uri->port; 
    $log->tracef('Will connect to %s', $endpoint);
    $self->{connection} //= $self->loop->connect(
        service		=> $uri->port,
        host		=> $uri->host,
        socktype	=> 'stream',
    )->then(sub {
        my ($sock) = @_;
        # Once we have a TCP connection, we'd usually do
        # some form of TLS here. For now, plain text is good
        # enough.
        $log->tracef('Connected to %s', $endpoint);
        $self->add_child(
            my $stream = IO::Async::Stream->new(
                handle => $sock,
                on_read => $self->curry::weak::on_read,
            )
        );
        Scalar::Util::weaken($self->{stream} = $stream);
        $log->tracef('Send initial request with user %s', $uri->user);
        $self->protocol->initial_request(
            application_name => 'whatever',
            # replication      => 'database',
            user             => $uri->user,
        );
        Future->done($stream)
    });
}

sub uri_for_dsn {
    my ($class, $dsn) = @_;
    die 'invalid DSN, expecting DBI:Pg:...' unless $dsn =~ s/^DBI:Pg://i;
    my %args = split /[=;]/, $dsn;
    my $uri = URI->new('postgresql://postgres@localhost/postgres');
    $uri->host(delete $args{host}) if exists $args{host};
    $uri->user(delete $args{user}) if exists $args{user};
    $uri->password(delete $args{password}) if exists $args{password};
    $uri->dbname(delete $args{dbname}) if exists $args{dbname};
    $uri
}

sub stream { shift->{stream} }

sub on_read {
	my ($self, $stream, $buffref, $eof) = @_;

    $log->tracef('Have server message of length %d', length $$buffref);
    while(1) {
        # All PG messages are at least 5 bytes long.
        return 0 unless length($$buffref) >= 5;

        # We do the message extraction ourselves here, which feels
        # a bit wrong. Maybe this should be in Protocol::PostgreSQL
        # itself.
        my ($code, $size) = unpack('C1N1', $$buffref);
        return 0 unless length($$buffref) >= $size+1;
        $self->protocol->handle_message(
            substr $$buffref, 0, $size+1, ''
        );
    }
}

sub protocol {
    my ($self) = @_;
    $self->{protocol} //= do {
        my $pg = Protocol::PostgreSQL::Client->new(
            database => $self->uri->dbname
        );
        $pg->bus->subscribe_to_event(
            password => $self->$curry::weak(sub {
                my ($self, $ev, %args) = @_;
                $log->tracef('Auth request received: %s', \%args);
                $self->protocol->{user} = $self->uri->user;
                $self->protocol->send_message('PasswordMessage', password => $self->uri->password);
                $ev->unsubscribe; # single-shot event
            }),
            parameter_status => $self->$curry::weak(sub {
                my ($self, $ev, %args) = @_;
                $log->tracef('Parameter received: %s', $args{status});
            }),
            row_description => $self->$curry::weak(sub {
                my ($self, $ev, %args) = @_;
                $log->tracef('Row description %s', \%args);
                $self->active_query->row_description($args{description});
            }),
            data_row => $self->$curry::weak(sub {
                my ($self, $ev, %args) = @_;
                $log->tracef('Have row data %s', \%args);
                $self->active_query->row($args{row});
            }),
            command_complete => $self->$curry::weak(sub {
                my ($self, $ev, %args) = @_;
                my $query = delete $self->{active_query};
                $log->tracef('Completed query %s', $query);
                $query->done
            }),
            send_request => $self->$curry::weak(sub {
                my ($self, $ev, $msg) = @_;
                $log->tracef('Send request for %s', $msg);
                $self->stream->write($msg);
            }),
            ready_for_query => $self->$curry::weak(sub {
                my ($self) = @_;
                $log->tracef('Ready for query');
                $self->db->engine_ready($self);
            })
        );
        $pg
    }
}

=head2 next_query


=cut

sub handle_query {
    my ($self, $query) = @_;
    die 'already have active query' if $self->{active_query};
    $self->{active_query} = $query;
    $self->protocol->simple_query($query->sql);
    Future->done
}

sub active_query { shift->{active_query} }

1;

__END__

=head1 Implementation notes

Query sequence is essentially:

 - < ReadyForQuery
 - > frontend_query
 - < Row Description
 - < Data Row
 - < Command Complete
 - < ReadyForQuery

The DB creates an engine.
The engine does whatever connection handling required, and eventually
should reach a "ready" state.
Once this happens, it'll notify DB to say "this engine is ready for queries".
If there are any pending queries, the next in the queue is immediately assigned
to this engine.
Otherwise, the engine is pushed into the pool of available engines, awaiting
query requests.

On startup, the pool `min` count of engine instances will be instantiated.
They start in the pending state.

Any of the following:

- tx
- query
- copy etc.

is treated as "queue request". It indicates that we're going to send one or
more commands over a connection.

`next_engine` resolves with an engine instance:

- check for engines in `available` queue - these are connected and waiting,
and can be assigned immediately
- next look for engines in `unconnected` - these are instantiated but need
a ->connection first

=cut

=head1 AUTHOR

Tom Molesworth C<< <TEAM@cpan.org> >>

=head1 LICENSE

Copyright Tom Molesworth 2011-2018. Licensed under the same terms as Perl itself.


