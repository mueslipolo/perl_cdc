package DBIx::DataModel::Plugin::CDC;

use strict;
use warnings;
use Carp qw(croak);

our $VERSION = '1.00';

# Package-level registry: schema_class => \%config
my %REGISTRY;

# ---------------------------------------------------------------
# setup($schema_class, %args)
#
#   tables   => 'all' | \@table_names
#   handlers => \@handler_objects
#
# Registers CDC configuration for a schema.  Must be called
# after Table() declarations and before DML operations.
# ---------------------------------------------------------------
sub setup {
    my ($class, $schema_class, %args) = @_;
    croak 'setup() requires a schema class name' unless $schema_class;
    croak 'handlers arrayref required'
        unless ref $args{handlers} eq 'ARRAY' && @{$args{handlers}};

    # Resolve table list
    my $tables_arg = $args{tables} // 'all';
    my %tracked;

    if ($tables_arg eq 'all') {
        # Track all tables registered in the schema's metadm
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

# ---------------------------------------------------------------
# config_for($schema_class) -> \%config | undef
# ---------------------------------------------------------------
sub config_for {
    my ($class, $schema_class) = @_;
    return $REGISTRY{$schema_class};
}

# ---------------------------------------------------------------
# is_tracked($schema_class, $table_name) -> bool
# ---------------------------------------------------------------
sub is_tracked {
    my ($class, $schema_class, $table_name) = @_;
    my $cfg = $REGISTRY{$schema_class} or return 0;
    return $cfg->{tracked}{$table_name} ? 1 : 0;
}

# ---------------------------------------------------------------
# dispatch($schema_class, $schema_obj, $event)
#
# Dispatches an event to all registered handlers.
# in_transaction handlers run immediately.
# post_commit handlers are deferred via do_after_commit (if in a
# transaction) or run immediately after sync handlers (AutoCommit).
# ---------------------------------------------------------------
sub dispatch {
    my ($class, $schema_class, $schema_obj, $event) = @_;
    my $cfg = $REGISTRY{$schema_class} or return;

    my (@sync, @async);
    for my $h (@{ $cfg->{handlers} }) {
        if ($h->isa('DBIx::DataModel::Plugin::CDC::Handler::Multi')) {
            # Multi handler manages its own phase dispatch
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

    # Schedule post_commit handlers
    if (@async) {
        if ($schema_obj->{transaction_dbhs}) {
            # Inside do_transaction — defer to after commit
            $schema_obj->do_after_commit(sub {
                $_->() for @async;
            });
        } else {
            # AutoCommit mode — transaction already committed, run now
            $_->() for @async;
        }
    }
}

# ---------------------------------------------------------------
# Query helpers (convenience, delegates to DBI handler's table)
# ---------------------------------------------------------------
sub events_for {
    my ($class, $schema_class, %args) = @_;
    croak 'events_for: "table" argument required' unless $args{table};
    my $cfg = $REGISTRY{$schema_class} or croak 'CDC not configured';

    # Find the DBI handler to get its table name
    my $dbi_handler;
    for my $h (@{ $cfg->{handlers} }) {
        if ($h->isa('DBIx::DataModel::Plugin::CDC::Handler::DBI')) {
            $dbi_handler = $h;
            last;
        }
        if ($h->isa('DBIx::DataModel::Plugin::CDC::Handler::Multi')) {
            for my $sub (@{ $h->{handlers} }) {
                if ($sub->isa('DBIx::DataModel::Plugin::CDC::Handler::DBI')) {
                    $dbi_handler = $sub;
                    last;
                }
            }
        }
    }
    croak 'No DBI handler configured — cannot query events'
        unless $dbi_handler;

    my $schema_obj = $schema_class->singleton;
    my $dbh        = $schema_obj->dbh;
    my $tbl        = $dbi_handler->{table_name};
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

    for my $h (@{ $cfg->{handlers} }) {
        if ($h->isa('DBIx::DataModel::Plugin::CDC::Handler::DBI')) {
            $schema_class->singleton->dbh->do(
                "DELETE FROM " . $h->{table_name});
            return;
        }
        if ($h->isa('DBIx::DataModel::Plugin::CDC::Handler::Multi')) {
            for my $sub (@{ $h->{handlers} }) {
                if ($sub->isa('DBIx::DataModel::Plugin::CDC::Handler::DBI')) {
                    $schema_class->singleton->dbh->do(
                        "DELETE FROM " . $sub->{table_name});
                    return;
                }
            }
        }
    }
}

sub clear_events_for {
    my ($class, $schema_class, %args) = @_;
    croak 'table required' unless $args{table};
    my $cfg = $REGISTRY{$schema_class} or return;

    for my $h (@{ $cfg->{handlers} }) {
        if ($h->isa('DBIx::DataModel::Plugin::CDC::Handler::DBI')) {
            $schema_class->singleton->dbh->do(
                "DELETE FROM " . $h->{table_name} . " WHERE table_name = ?",
                undef, uc $args{table});
            return;
        }
    }
}

sub event_pairs {
    my ($class, $schema_class, %args) = @_;
    $args{operation} = 'UPDATE';
    my $events = $class->events_for($schema_class, %args);
    return [
        map {
            my $old = $_->{old_data};
            my $new = $_->{new_data};
            # Parse JSON if strings
            if (defined $old && !ref $old) {
                $old = Cpanel::JSON::XS->new->utf8->decode($old);
            }
            if (defined $new && !ref $new) {
                $new = Cpanel::JSON::XS->new->utf8->decode($new);
            }
            [$old // {}, $new // {}]
        } @$events
    ];
}

1;

__END__

=head1 NAME

DBIx::DataModel::Plugin::CDC — Change Data Capture for DBIx::DataModel

=head1 SYNOPSIS

    use DBIx::DataModel::Plugin::CDC;
    use DBIx::DataModel::Plugin::CDC::Handler::DBI;
    use DBIx::DataModel::Plugin::CDC::Handler::Callback;

    # 1. Declare schema with CDC table parent
    DBIx::DataModel->Schema('App::Schema',
        table_parent => 'DBIx::DataModel::Plugin::CDC::Table',
    );
    App::Schema->Table(Department => 'departments', 'id');

    # 2. Configure handlers
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
