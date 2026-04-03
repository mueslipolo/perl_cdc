package DBIx::DataModel::Plugin::CDC;

use strict;
use warnings;
use Carp qw(croak);
use Cpanel::JSON::XS ();
use namespace::clean;

our $VERSION = '1.01';

my $JSON_DECODE = Cpanel::JSON::XS->new->canonical->allow_nonref;

# Package-level registry: schema_class => \%config
my %REGISTRY;

# ---------------------------------------------------------------
# setup($schema_class, %args)
# ---------------------------------------------------------------
sub setup {
    my ($class, $schema_class, %args) = @_;
    croak 'setup() requires a schema class name' unless $schema_class;
    croak 'handlers arrayref required'
        unless ref $args{handlers} eq 'ARRAY' && @{$args{handlers}};

    my $tables_arg = $args{tables} // 'all';
    my %tracked;

    if ($tables_arg eq 'all') {
        for my $meta ($schema_class->metadm->tables) {
            $tracked{ $meta->name } = 1;
        }
    } elsif (ref $tables_arg eq 'ARRAY') {
        $tracked{$_} = 1 for @$tables_arg;
    } else {
        croak "tables must be 'all' or an arrayref";
    }

    $REGISTRY{$schema_class} = {
        tracked  => \%tracked,
        handlers => $args{handlers},
    };

    return $class;
}

sub config_for {
    my ($class, $schema_class) = @_;
    return $REGISTRY{$schema_class};
}

sub is_tracked {
    my ($class, $schema_class, $table_name) = @_;
    my $cfg = $REGISTRY{$schema_class} or return 0;
    return $cfg->{tracked}{$table_name} ? 1 : 0;
}

# ---------------------------------------------------------------
# _find_dbi_handler(\%config) -> $handler | undef
#
# Locates the DBI handler in the handler list, including inside
# Multi wrappers.  Single lookup used by all query helpers.
# ---------------------------------------------------------------
sub _find_dbi_handler {
    my ($class, $cfg) = @_;
    for my $h (@{ $cfg->{handlers} }) {
        return $h if $h->isa('DBIx::DataModel::Plugin::CDC::Handler::DBI');
        if ($h->isa('DBIx::DataModel::Plugin::CDC::Handler::Multi')) {
            for my $sub (@{ $h->{handlers} }) {
                return $sub
                    if $sub->isa('DBIx::DataModel::Plugin::CDC::Handler::DBI');
            }
        }
    }
    return undef;
}

# ---------------------------------------------------------------
# dispatch($schema_class, $schema_obj, $event)
# ---------------------------------------------------------------
sub dispatch {
    my ($class, $schema_class, $schema_obj, $event) = @_;
    my $cfg = $REGISTRY{$schema_class} or return;

    my (@sync, @async);
    for my $h (@{ $cfg->{handlers} }) {
        if ($h->isa('DBIx::DataModel::Plugin::CDC::Handler::Multi')) {
            $h->dispatch_event($event, $schema_obj);
            if ($h->has_post_commit_handlers) {
                push @async, sub { $h->dispatch_post_commit($event, $schema_obj) };
            }
        } elsif ($h->phase eq 'in_transaction') {
            $h->dispatch_event($event, $schema_obj);
        } else {
            push @async, sub { $h->dispatch_event($event, $schema_obj) };
        }
    }

    if (@async) {
        if ($schema_obj->{transaction_dbhs}) {
            $schema_obj->do_after_commit(sub {
                $_->() for @async;
            });
        } else {
            $_->() for @async;
        }
    }
}

# ---------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------

sub _validated_table_name {
    my ($class, $handler) = @_;
    my $tbl = $handler->{table_name};
    croak "Invalid table name: $tbl" unless $tbl =~ /\A[a-zA-Z_]\w*\z/;
    return $tbl;
}

sub events_for {
    my ($class, $schema_class, %args) = @_;
    croak 'events_for: "table" argument required' unless $args{table};
    my $cfg = $REGISTRY{$schema_class} or croak 'CDC not configured';

    my $dbi_handler = $class->_find_dbi_handler($cfg)
        or croak 'No DBI handler configured — cannot query events';

    my $schema_obj = $schema_class->singleton;
    my $dbh        = $schema_obj->dbh
        or croak 'No active database connection';
    my $tbl        = $class->_validated_table_name($dbi_handler);
    my $table      = uc $args{table};
    my $op         = defined $args{operation} ? uc $args{operation} : undef;

    my $sql  = "SELECT * FROM $tbl WHERE table_name = ?";
    my @bind = ($table);
    if (defined $op) {
        $sql .= ' AND operation = ?';
        push @bind, $op;
    }
    $sql .= ' ORDER BY event_id ASC';

    return $dbh->selectall_arrayref($sql, { Slice => {} }, @bind);
}

sub count_events {
    my ($class, $schema_class, %args) = @_;
    return scalar @{ $class->events_for($schema_class, %args) };
}

sub latest_event {
    my ($class, $schema_class, %args) = @_;
    my $events = $class->events_for($schema_class, %args);
    return @$events ? $events->[-1] : undef;
}

sub clear_events {
    my ($class, $schema_class) = @_;
    my $cfg = $REGISTRY{$schema_class} or return;
    my $h = $class->_find_dbi_handler($cfg) or return;
    my $tbl = $class->_validated_table_name($h);
    $schema_class->singleton->dbh->do("DELETE FROM $tbl");
}

sub clear_events_for {
    my ($class, $schema_class, %args) = @_;
    croak 'table required' unless $args{table};
    my $cfg = $REGISTRY{$schema_class} or return;
    my $h = $class->_find_dbi_handler($cfg) or return;
    my $tbl = $class->_validated_table_name($h);
    $schema_class->singleton->dbh->do(
        "DELETE FROM $tbl WHERE table_name = ?",
        undef, uc $args{table},
    );
}

sub event_pairs {
    my ($class, $schema_class, %args) = @_;
    $args{operation} = 'UPDATE';
    my $events = $class->events_for($schema_class, %args);
    return [
        map {
            my $old = $_->{old_data};
            my $new = $_->{new_data};
            $old = $JSON_DECODE->decode($old) if defined $old && !ref $old;
            $new = $JSON_DECODE->decode($new) if defined $new && !ref $new;
            [$old // {}, $new // {}]
        } @$events
    ];
}

1;

__END__

=head1 NAME

DBIx::DataModel::Plugin::CDC - Change Data Capture for DBIx::DataModel

=head1 SYNOPSIS

    use DBIx::DataModel::Plugin::CDC;
    use DBIx::DataModel::Plugin::CDC::Handler::DBI;
    use DBIx::DataModel::Plugin::CDC::Handler::Callback;

    DBIx::DataModel->Schema('App::Schema',
        table_parent => 'DBIx::DataModel::Plugin::CDC::Table',
    );
    App::Schema->Table(Department => 'departments', 'id');

    DBIx::DataModel::Plugin::CDC->setup('App::Schema',
        tables   => 'all',
        handlers => [
            DBIx::DataModel::Plugin::CDC::Handler::DBI->new(
                table_name => 'cdc_events',
            ),
            DBIx::DataModel::Plugin::CDC::Handler::Callback->new(
                on_event => sub { my ($event) = @_; ... },
                phase    => 'post_commit',
            ),
        ],
    );

=cut
