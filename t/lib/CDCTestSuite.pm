package CDCTestSuite;

# =============================================================
# Shared e2e test logic for DBIx::DataModel::Plugin::CDC.
#
# Backend-specific test files (SQLite, Oracle, ...) create an
# instance with a $dbh, then call $suite->run_common_suite().
# =============================================================

use strict;
use warnings;
use Test::More;
use Test::Exception;
use Cpanel::JSON::XS ();
use DBIx::DataModel::Plugin::CDC;

my $CDC  = 'DBIx::DataModel::Plugin::CDC';
my $JSON = Cpanel::JSON::XS->new->canonical->allow_nonref;

# ---------------------------------------------------------------
# Constructor and accessors
# ---------------------------------------------------------------

sub new {
    my ($class, %args) = @_;
    return bless {
        dbh       => $args{dbh} || die('dbh required'),
        cb_events => [],
    }, $class;
}

sub dbh       { $_[0]->{dbh} }
sub cb_events { $_[0]->{cb_events} }
sub schema    { 'CDCTestSuite::Schema' }

# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------

sub setup_cdc {
    my ($self, %opts) = @_;
    my $capture_old = $opts{capture_old} // 1;
    @{ $self->{cb_events} } = ();
    my $events_ref = $self->{cb_events};
    $CDC->setup($self->schema,
        tables      => 'all',
        capture_old => $capture_old,
        force       => 1,
    )->log_to_dbi($self->schema, 'cdc_events')
     # in_transaction so the callback fires reliably inside
     # _cdc_ensure_atomic's mini-transaction (post_commit would
     # not fire until after commit, which hasn't happened yet).
     ->on($self->schema, '*' => sub {
         push @$events_ref, $_[0];
     }, { phase => 'in_transaction' });
}

sub clean {
    my ($self) = @_;
    my $dbh = $self->dbh;
    $dbh->do('DELETE FROM employees');
    $dbh->do('DELETE FROM departments');
    $CDC->clear_events($self->schema);
    @{ $self->{cb_events} } = ();
}

sub parse_json {
    my ($self, $str) = @_;
    return undef unless defined $str;
    return $JSON->decode($str);
}

# Insert a department and return its ID (extracted from CDC event,
# works identically on all backends).
sub insert_dept {
    my ($self, %args) = @_;
    $self->schema->table('Department')->insert({
        name     => $args{name}     // 'TestDept',
        location => $args{location} // 'Geneva',
    });
    my $ev = $CDC->latest_event($self->schema,
        table => 'departments', operation => 'INSERT')
        or die 'No INSERT event for department';
    return $self->parse_json($ev->{new_data})->{ID};
}

# ---------------------------------------------------------------
# run_common_suite — runs all backend-agnostic tests
# ---------------------------------------------------------------

sub run_common_suite {
    my ($self) = @_;

    $self->test_insert_department;
    $self->test_insert_employee_fk;
    $self->test_insert_callback;
    $self->test_update_instance;
    $self->test_update_class_method;
    $self->test_delete_instance;
    $self->test_delete_class_method;
    $self->test_atomicity_rollback;
    $self->test_capture_old_off_instance;
    $self->test_capture_old_off_class;
    $self->test_query_helpers;
    $self->test_clear_events_for;
    $self->test_event_pairs;
    $self->test_event_envelope;
    $self->test_changed_columns;
    $self->test_listener_in_transaction;
    $self->test_error_policy_warn;
    $self->test_full_lifecycle;
    $self->test_selective_tracking;
    $self->test_composition_subtree;
    $self->test_composition_cascaded_delete;
    $self->test_composition_insert_into;
    $self->test_null_values;
    $self->test_zero_match_update;
    $self->test_zero_match_delete;
    $self->test_double_update_history;
    $self->test_db_state_insert;
    $self->test_db_state_update;
    $self->test_multiple_listeners;
    $self->test_operation_specific_listeners;
    $self->test_event_pairs_capture_old_off;
}

# ---------------------------------------------------------------
# Individual test methods
# ---------------------------------------------------------------

sub test_insert_department {
    my ($self) = @_;
    subtest 'INSERT – Department' => sub {
        plan tests => 6;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'Engineering', location => 'Geneva',
        });

        my $events = $CDC->events_for($self->schema,
            table => 'departments', operation => 'INSERT');
        is(scalar @$events, 1, 'one INSERT event in DB');
        is($events->[0]{operation}, 'INSERT', 'operation field');
        ok(!defined $events->[0]{old_data}, 'old_data is NULL');
        ok(defined  $events->[0]{new_data}, 'new_data populated');

        my $new = $self->parse_json($events->[0]{new_data});
        is($new->{NAME},     'Engineering', 'NAME captured');
        is($new->{LOCATION}, 'Geneva',      'LOCATION captured');
    };
}

sub test_insert_employee_fk {
    my ($self) = @_;
    subtest 'INSERT – Employee with FK' => sub {
        plan tests => 4;
        $self->clean;

        my $dept_id = $self->insert_dept(name => 'HR', location => 'Zurich');
        $CDC->clear_events($self->schema);

        $self->schema->table('Employee')->insert({
            department_id => $dept_id, first_name => 'Alice',
            last_name => 'Dupont', email => 'alice@example.com', salary => 90000,
        });

        my $ev = $CDC->latest_event($self->schema,
            table => 'employees', operation => 'INSERT');
        ok($ev, 'INSERT event captured');
        my $new = $self->parse_json($ev->{new_data});
        is($new->{FIRST_NAME}, 'Alice',             'first_name');
        is($new->{EMAIL},      'alice@example.com', 'email');
        is($new->{SALARY},     90000,               'salary');
    };
}

sub test_insert_callback {
    my ($self) = @_;
    subtest 'INSERT – callback listener receives event' => sub {
        plan tests => 4;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'Legal', location => 'Bern',
        });

        my $events = $self->cb_events;
        is(scalar @$events, 1, 'callback fired once');
        my $ev = $events->[0];
        is($ev->{operation},  'INSERT',      'operation');
        is($ev->{table_name}, 'DEPARTMENTS', 'table_name');
        ok(defined $ev->{row_id},            'row_id present');
    };
}

sub test_update_instance {
    my ($self) = @_;
    subtest 'UPDATE – instance method with capture_old' => sub {
        plan tests => 5;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'Sales', location => 'Lausanne',
        });
        $CDC->clear_events($self->schema);
        @{ $self->{cb_events} } = ();

        my $dept = $self->schema->table('Department')
            ->select(-where => { name => 'Sales' })->[0];
        $dept->update({ location => 'Montreux' });

        my $ev = $CDC->latest_event($self->schema,
            table => 'departments', operation => 'UPDATE');
        ok($ev, 'UPDATE event in DB');
        is($ev->{operation}, 'UPDATE', 'operation');

        my $old = $self->parse_json($ev->{old_data});
        my $new = $self->parse_json($ev->{new_data});
        is($old->{LOCATION}, 'Lausanne', 'old location');
        is($new->{LOCATION}, 'Montreux', 'new location');

        is(scalar @{ $self->cb_events }, 1, 'callback fired');
    };
}

sub test_update_class_method {
    my ($self) = @_;
    subtest 'UPDATE – class method (-set/-where)' => sub {
        plan tests => 5;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'Ops', location => 'Bern',
        });
        $self->schema->table('Department')->insert({
            name => 'Dev', location => 'Bern',
        });
        $CDC->clear_events($self->schema);
        @{ $self->{cb_events} } = ();

        $self->schema->table('Department')->update(
            -set   => { location => 'Zurich' },
            -where => { location => 'Bern' },
        );

        my $events = $CDC->events_for($self->schema,
            table => 'departments', operation => 'UPDATE');
        is(scalar @$events, 2, 'one event per affected row');

        for my $ev (@$events) {
            my $old = $self->parse_json($ev->{old_data});
            my $new = $self->parse_json($ev->{new_data});
            is($old->{LOCATION}, 'Bern',   "old location");
            is($new->{LOCATION}, 'Zurich', "new location");
        }
    };
}

sub test_delete_instance {
    my ($self) = @_;
    subtest 'DELETE – instance method' => sub {
        plan tests => 4;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'Temp', location => 'Nowhere',
        });
        $CDC->clear_events($self->schema);

        my $dept = $self->schema->table('Department')
            ->select(-where => { name => 'Temp' })->[0];
        $dept->delete;

        my $ev = $CDC->latest_event($self->schema,
            table => 'departments', operation => 'DELETE');
        ok($ev, 'DELETE event in DB');
        is($ev->{operation}, 'DELETE', 'operation');
        ok(defined $ev->{old_data}, 'old_data present (capture_old=1)');
        ok(!defined $ev->{new_data}, 'new_data is NULL');
    };
}

sub test_delete_class_method {
    my ($self) = @_;
    subtest 'DELETE – class method (-where)' => sub {
        plan tests => 2;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'A', location => 'X',
        });
        $self->schema->table('Department')->insert({
            name => 'B', location => 'X',
        });
        $CDC->clear_events($self->schema);

        $self->schema->table('Department')->delete(
            -where => { location => 'X' },
        );

        my $events = $CDC->events_for($self->schema,
            table => 'departments', operation => 'DELETE');
        is(scalar @$events, 2, 'one DELETE event per row');

        my $remaining = $self->dbh->selectrow_array(
            'SELECT COUNT(*) FROM departments');
        is($remaining, 0, 'rows deleted from DB');
    };
}

sub test_atomicity_rollback {
    my ($self) = @_;
    subtest 'Atomicity – error rolls back DML and CDC event' => sub {
        plan tests => 3;
        $self->clean;

        $CDC->setup($self->schema, tables => 'all', capture_old => 1, force => 1)
            ->log_to_dbi($self->schema, 'cdc_events')
            ->on($self->schema, 'INSERT' => sub {
                die "forced abort";
            }, { phase => 'in_transaction', on_error => 'abort' });

        throws_ok {
            $self->schema->table('Department')->insert({
                name => 'Ghost', location => 'Void',
            });
        } qr/forced abort/, 'insert dies on listener abort';

        my $row_count = $self->dbh->selectrow_array(
            'SELECT COUNT(*) FROM departments');
        is($row_count, 0, 'department row rolled back');

        my $event_count = $self->dbh->selectrow_array(
            'SELECT COUNT(*) FROM cdc_events');
        is($event_count, 0, 'cdc_events row rolled back too');

        $self->setup_cdc(capture_old => 1);
    };
}

sub test_capture_old_off_instance {
    my ($self) = @_;
    subtest 'capture_old => 0 – no old_data, row_id present' => sub {
        plan tests => 5;
        $self->clean;
        $self->setup_cdc(capture_old => 0);

        $self->schema->table('Department')->insert({
            name => 'Light', location => 'Fast',
        });

        my $dept = $self->schema->table('Department')
            ->select(-where => { name => 'Light' })->[0];
        $dept->update({ location => 'Faster' });

        my $ev = $CDC->latest_event($self->schema,
            table => 'departments', operation => 'UPDATE');
        ok($ev, 'UPDATE event captured');
        ok(!defined $ev->{old_data}, 'old_data is NULL (capture_old=0)');
        ok(defined $ev->{new_data}, 'new_data present');

        my $cb_ev = $self->cb_events->[-1];
        ok(defined $cb_ev->{row_id}, 'row_id present in callback');
        ok(!defined $cb_ev->{old_data}, 'old_data undef in callback');

        $self->setup_cdc(capture_old => 1);
    };
}

sub test_capture_old_off_class {
    my ($self) = @_;
    subtest 'capture_old => 0 – class-method update' => sub {
        plan tests => 3;
        $self->clean;
        $self->setup_cdc(capture_old => 0);

        $self->schema->table('Department')->insert({
            name => 'A', location => 'Here',
        });
        $self->schema->table('Department')->insert({
            name => 'B', location => 'Here',
        });
        $CDC->clear_events($self->schema);
        @{ $self->{cb_events} } = ();

        $self->schema->table('Department')->update(
            -set   => { location => 'There' },
            -where => { location => 'Here' },
        );

        my $events = $CDC->events_for($self->schema,
            table => 'departments', operation => 'UPDATE');
        is(scalar @$events, 2, 'one event per row');

        for my $ev (@$events) {
            ok(!defined $ev->{old_data}, 'old_data NULL in light mode');
        }

        $self->setup_cdc(capture_old => 1);
    };
}

sub test_query_helpers {
    my ($self) = @_;
    subtest 'Query helpers – events_for, count_events, latest_event' => sub {
        plan tests => 5;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'Q1', location => 'A',
        });
        $self->schema->table('Department')->insert({
            name => 'Q2', location => 'B',
        });

        my $all = $CDC->events_for($self->schema, table => 'departments');
        is(scalar @$all, 2, 'events_for returns all');

        my $count = $CDC->count_events($self->schema,
            table => 'departments');
        is($count, 2, 'count_events');

        my $count_ins = $CDC->count_events($self->schema,
            table => 'departments', operation => 'INSERT');
        is($count_ins, 2, 'count_events with operation filter');

        my $latest = $CDC->latest_event($self->schema,
            table => 'departments');
        ok($latest, 'latest_event returns something');
        my $new = $self->parse_json($latest->{new_data});
        is($new->{NAME}, 'Q2', 'latest_event is the last insert');
    };
}

sub test_clear_events_for {
    my ($self) = @_;
    subtest 'Query helpers – clear_events_for' => sub {
        plan tests => 2;
        $self->clean;

        my $dept_id = $self->insert_dept(name => 'Keep', location => 'A');
        $self->schema->table('Employee')->insert({
            department_id => $dept_id, first_name => 'Bob',
            last_name => 'X', email => 'bob@x.com', salary => 50000,
        });

        $CDC->clear_events_for($self->schema, table => 'departments');

        my $dept_count = $CDC->count_events($self->schema,
            table => 'departments');
        is($dept_count, 0, 'department events cleared');

        my $emp_count = $CDC->count_events($self->schema,
            table => 'employees');
        is($emp_count, 1, 'employee events preserved');
    };
}

sub test_event_pairs {
    my ($self) = @_;
    subtest 'Query helpers – event_pairs' => sub {
        plan tests => 3;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'Pair', location => 'Before',
        });
        my $dept = $self->schema->table('Department')
            ->select(-where => { name => 'Pair' })->[0];
        $dept->update({ location => 'After' });

        my $pairs = $CDC->event_pairs($self->schema,
            table => 'departments');
        is(scalar @$pairs, 1, 'one update pair');
        is($pairs->[0][0]{LOCATION}, 'Before', 'old value');
        is($pairs->[0][1]{LOCATION}, 'After',  'new value');
    };
}

sub test_event_pairs_capture_old_off {
    my ($self) = @_;
    subtest 'Query helpers – event_pairs with capture_old=0' => sub {
        plan tests => 3;
        $self->clean;
        $self->setup_cdc(capture_old => 0);

        $self->schema->table('Department')->insert({
            name => 'PairLight', location => 'Start',
        });
        my $dept = $self->schema->table('Department')
            ->select(-where => { name => 'PairLight' })->[0];
        $dept->update({ location => 'End' });

        my $pairs = $CDC->event_pairs($self->schema,
            table => 'departments');
        is(scalar @$pairs, 1, 'one update pair');
        is_deeply($pairs->[0][0], {}, 'old is empty hash (no capture_old)');
        ok(defined $pairs->[0][1]{LOCATION}, 'new has data');

        $self->setup_cdc(capture_old => 1);
    };
}

sub test_event_envelope {
    my ($self) = @_;
    subtest 'Event envelope – all fields present' => sub {
        plan tests => 7;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'Envelope', location => 'Test',
        });

        my $ev = $self->cb_events->[0];
        ok(defined $ev->{cdc_event_id}, 'cdc_event_id');
        ok(defined $ev->{occurred_at}, 'occurred_at');
        like($ev->{occurred_at}, qr/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/,
            'occurred_at is ISO 8601');
        is($ev->{table_name}, 'DEPARTMENTS', 'table_name');
        is($ev->{operation},  'INSERT',      'operation');
        ok(defined $ev->{primary_key},       'primary_key present');
        ok(defined $ev->{row_id},            'row_id present');
    };
}

sub test_changed_columns {
    my ($self) = @_;
    subtest 'Event envelope – changed_columns on UPDATE' => sub {
        plan tests => 2;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'CC', location => 'Old',
        });
        @{ $self->{cb_events} } = ();

        my $dept = $self->schema->table('Department')
            ->select(-where => { name => 'CC' })->[0];
        $dept->update({ location => 'New' });

        my $ev = $self->cb_events->[0];
        ok(defined $ev->{changed_columns}, 'changed_columns present');
        ok(grep({ $_ eq 'LOCATION' } @{ $ev->{changed_columns} }),
            'LOCATION in changed_columns');
    };
}

sub test_listener_in_transaction {
    my ($self) = @_;
    subtest 'Listener phase – in_transaction has DB access' => sub {
        plan tests => 2;
        $self->clean;

        my $saw_row = 0;
        $CDC->setup($self->schema, tables => 'all', capture_old => 1, force => 1)
            ->log_to_dbi($self->schema, 'cdc_events')
            ->on($self->schema, 'INSERT' => sub {
                my ($event, $schema) = @_;
                my $count = $schema->dbh->selectrow_array(
                    'SELECT COUNT(*) FROM departments WHERE name = ?',
                    undef, 'Visible');
                $saw_row = $count;
            }, { phase => 'in_transaction' });

        $self->schema->table('Department')->insert({
            name => 'Visible', location => 'Here',
        });

        is($saw_row, 1, 'in_transaction listener can see the inserted row');

        my $count = $self->dbh->selectrow_array(
            'SELECT COUNT(*) FROM departments WHERE name = ?',
            undef, 'Visible');
        is($count, 1, 'row committed to DB');

        $self->setup_cdc(capture_old => 1);
    };
}

sub test_error_policy_warn {
    my ($self) = @_;
    subtest 'Error policy – warn does not abort DML' => sub {
        plan tests => 2;
        $self->clean;

        $CDC->setup($self->schema, tables => 'all', capture_old => 1, force => 1)
            ->log_to_dbi($self->schema, 'cdc_events')
            ->on($self->schema, 'INSERT' => sub {
                die "should warn, not abort";
            }, { phase => 'in_transaction', on_error => 'warn' });

        my $warned = 0;
        local $SIG{__WARN__} = sub { $warned++ if $_[0] =~ /should warn/ };

        lives_ok {
            $self->schema->table('Department')->insert({
                name => 'WarnTest', location => 'OK',
            });
        } 'insert succeeds despite warn listener';
        ok($warned, 'warning was emitted');

        $self->setup_cdc(capture_old => 1);
    };
}

sub test_full_lifecycle {
    my ($self) = @_;
    subtest 'Full lifecycle – INSERT, UPDATE, DELETE' => sub {
        plan tests => 4;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'Lifecycle', location => 'Start',
        });

        my $dept = $self->schema->table('Department')
            ->select(-where => { name => 'Lifecycle' })->[0];
        $dept->update({ location => 'Middle' });
        $dept->delete;

        my $all = $CDC->events_for($self->schema,
            table => 'departments');
        is(scalar @$all, 3, 'three events total');

        my @ops = map { $_->{operation} } @$all;
        is($ops[0], 'INSERT', 'first is INSERT');
        is($ops[1], 'UPDATE', 'second is UPDATE');
        is($ops[2], 'DELETE', 'third is DELETE');
    };
}

sub test_selective_tracking {
    my ($self) = @_;
    subtest 'Selective tracking – untracked table has no events' => sub {
        plan tests => 2;
        $self->clean;

        $CDC->setup($self->schema,
            tables      => ['Department'],
            capture_old => 1,
            force       => 1,
        )->log_to_dbi($self->schema, 'cdc_events');

        $self->schema->table('Department')->insert({
            name => 'Tracked', location => 'Yes',
        });
        $self->schema->table('Employee')->insert({
            department_id => 1, first_name => 'Ghost',
            last_name => 'None', email => 'ghost@x.com', salary => 0,
        });

        my $dept_events = $CDC->count_events($self->schema,
            table => 'departments');
        is($dept_events, 1, 'tracked table has events');

        my $emp_count = $self->dbh->selectrow_array(
            'SELECT COUNT(*) FROM cdc_events WHERE table_name = ?',
            undef, 'EMPLOYEES');
        is($emp_count, 0, 'untracked table has no events');

        $self->setup_cdc(capture_old => 1);
    };
}

sub test_composition_subtree {
    my ($self) = @_;
    subtest 'Composition – subtree insert' => sub {
        plan tests => 5;
        $self->clean;

        $self->schema->table('Department')->insert({
            name      => 'R&D',
            location  => 'Geneva',
            employees => [
                { first_name => 'Eve', last_name => 'A', email => 'eve@x.com', salary => 80000 },
                { first_name => 'Fay', last_name => 'B', email => 'fay@x.com', salary => 70000 },
            ],
        });

        my $dept_events = $CDC->count_events($self->schema,
            table => 'departments', operation => 'INSERT');
        is($dept_events, 1, 'one department INSERT');

        my $emp_events = $CDC->count_events($self->schema,
            table => 'employees', operation => 'INSERT');
        is($emp_events, 2, 'two employee INSERTs');

        my $events = $CDC->events_for($self->schema,
            table => 'employees', operation => 'INSERT');
        for my $ev (@$events) {
            my $new = $self->parse_json($ev->{new_data});
            ok(defined $new->{DEPARTMENT_ID}, 'FK populated in child');
        }

        my $emp_count = $self->dbh->selectrow_array(
            'SELECT COUNT(*) FROM employees');
        is($emp_count, 2, 'two employees in DB');
    };
}

sub test_composition_cascaded_delete {
    my ($self) = @_;
    subtest 'Composition – cascaded delete' => sub {
        plan tests => 4;
        $self->clean;

        $self->schema->table('Department')->insert({
            name      => 'Doomed',
            location  => 'Void',
            employees => [
                { first_name => 'X', last_name => 'Y', email => 'x@y.com', salary => 10000 },
            ],
        });
        $CDC->clear_events($self->schema);
        @{ $self->{cb_events} } = ();

        my $dept = $self->schema->table('Department')
            ->select(-where => { name => 'Doomed' })->[0];
        $dept->expand('employees');
        $dept->delete;

        my $del_events = $CDC->events_for($self->schema,
            table => 'employees', operation => 'DELETE');
        is(scalar @$del_events, 1, 'one employee DELETE event');

        my $dept_del = $CDC->events_for($self->schema,
            table => 'departments', operation => 'DELETE');
        is(scalar @$dept_del, 1, 'one department DELETE event');

        my $emp_count = $self->dbh->selectrow_array(
            'SELECT COUNT(*) FROM employees');
        is($emp_count, 0, 'no employees left');
        my $dept_count = $self->dbh->selectrow_array(
            'SELECT COUNT(*) FROM departments');
        is($dept_count, 0, 'no departments left');
    };
}

sub test_composition_insert_into {
    my ($self) = @_;
    subtest 'Composition – insert_into_employees' => sub {
        plan tests => 3;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'Parent', location => 'Here',
        });
        $CDC->clear_events($self->schema);
        @{ $self->{cb_events} } = ();

        my $dept = $self->schema->table('Department')
            ->select(-where => { name => 'Parent' })->[0];
        $dept->insert_into_employees({
            first_name => 'Child', last_name => 'Row',
            email => 'child@x.com', salary => 55000,
        });

        my $ev = $CDC->latest_event($self->schema,
            table => 'employees', operation => 'INSERT');
        ok($ev, 'INSERT event captured');
        my $new = $self->parse_json($ev->{new_data});
        is($new->{FIRST_NAME}, 'Child', 'child name correct');
        ok(defined $new->{DEPARTMENT_ID}, 'FK auto-populated');
    };
}

sub test_null_values {
    my ($self) = @_;
    subtest 'Edge case – NULL values' => sub {
        plan tests => 3;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'NoLocation', location => undef,
        });

        my $ev = $CDC->latest_event($self->schema,
            table => 'departments', operation => 'INSERT');
        ok($ev, 'event captured');
        my $new = $self->parse_json($ev->{new_data});
        is($new->{NAME}, 'NoLocation', 'name present');
        ok(!defined $new->{LOCATION}, 'NULL location preserved in CDC');
    };
}

sub test_zero_match_update {
    my ($self) = @_;
    subtest 'Edge case – update zero rows' => sub {
        plan tests => 2;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'Exists', location => 'Here',
        });
        $CDC->clear_events($self->schema);
        @{ $self->{cb_events} } = ();

        $self->schema->table('Department')->update(
            -set   => { location => 'Nowhere' },
            -where => { name => 'DoesNotExist' },
        );

        my $events = $CDC->events_for($self->schema,
            table => 'departments', operation => 'UPDATE');
        is(scalar @$events, 0, 'no UPDATE events for zero-match');
        is(scalar @{ $self->cb_events }, 0, 'no callbacks for zero-match');
    };
}

sub test_zero_match_delete {
    my ($self) = @_;
    subtest 'Edge case – delete zero rows' => sub {
        plan tests => 2;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'Survivor', location => 'Safe',
        });
        $CDC->clear_events($self->schema);
        @{ $self->{cb_events} } = ();

        $self->schema->table('Department')->delete(
            -where => { name => 'NonExistent' },
        );

        my $events = $CDC->events_for($self->schema,
            table => 'departments', operation => 'DELETE');
        is(scalar @$events, 0, 'no DELETE events for zero-match');

        my $count = $self->dbh->selectrow_array(
            'SELECT COUNT(*) FROM departments WHERE name = ?',
            undef, 'Survivor');
        is($count, 1, 'original row untouched');
    };
}

sub test_double_update_history {
    my ($self) = @_;
    subtest 'Edge case – double update history' => sub {
        plan tests => 5;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'Evolving', location => 'V1',
        });

        my $dept = $self->schema->table('Department')
            ->select(-where => { name => 'Evolving' })->[0];
        $dept->update({ location => 'V2' });

        $dept = $self->schema->table('Department')
            ->select(-where => { name => 'Evolving' })->[0];
        $dept->update({ location => 'V3' });

        my $events = $CDC->events_for($self->schema,
            table => 'departments', operation => 'UPDATE');
        is(scalar @$events, 2, 'two UPDATE events');

        my $first  = $self->parse_json($events->[0]{old_data});
        my $second = $self->parse_json($events->[1]{old_data});
        is($first->{LOCATION},  'V1', 'first update: old=V1');
        is($second->{LOCATION}, 'V2', 'second update: old=V2');

        my $last_new = $self->parse_json($events->[1]{new_data});
        is($last_new->{LOCATION}, 'V3', 'last update: new=V3');

        my ($loc) = $self->dbh->selectrow_array(
            'SELECT location FROM departments WHERE name = ?',
            undef, 'Evolving');
        is($loc, 'V3', 'DB has final value');
    };
}

sub test_db_state_insert {
    my ($self) = @_;
    subtest 'DB state – INSERT matches event' => sub {
        plan tests => 3;
        $self->clean;

        $self->schema->table('Employee')->insert({
            department_id => undef, first_name => 'Verify',
            last_name => 'Me', email => 'verify@x.com', salary => 42000,
        });

        my $ev = $CDC->latest_event($self->schema,
            table => 'employees', operation => 'INSERT');
        my $cdc_data = $self->parse_json($ev->{new_data});

        my $row = $self->dbh->selectrow_hashref(
            'SELECT * FROM employees WHERE email = ?',
            undef, 'verify@x.com');

        is($cdc_data->{FIRST_NAME}, $row->{first_name}, 'first_name matches DB');
        is($cdc_data->{LAST_NAME},  $row->{last_name},  'last_name matches DB');
        is($cdc_data->{SALARY},     $row->{salary},      'salary matches DB');
    };
}

sub test_db_state_update {
    my ($self) = @_;
    subtest 'DB state – UPDATE reflects final value' => sub {
        plan tests => 3;
        $self->clean;

        $self->schema->table('Department')->insert({
            name => 'BeforeAfter', location => 'Old',
        });
        my $dept = $self->schema->table('Department')
            ->select(-where => { name => 'BeforeAfter' })->[0];
        $dept->update({ location => 'New' });

        my $ev = $CDC->latest_event($self->schema,
            table => 'departments', operation => 'UPDATE');
        my $old = $self->parse_json($ev->{old_data});
        my $new = $self->parse_json($ev->{new_data});

        is($old->{LOCATION}, 'Old', 'old_data captured before state');
        is($new->{LOCATION}, 'New', 'new_data captured after state');

        my ($loc) = $self->dbh->selectrow_array(
            'SELECT location FROM departments WHERE name = ?',
            undef, 'BeforeAfter');
        is($loc, 'New', 'DB matches new_data');
    };
}

sub test_multiple_listeners {
    my ($self) = @_;
    subtest 'Multiple listeners – all fire in order' => sub {
        plan tests => 2;
        $self->clean;

        my @order;
        $CDC->setup($self->schema, tables => 'all', capture_old => 1, force => 1)
            ->log_to_dbi($self->schema, 'cdc_events')
            ->on($self->schema, '*' => sub { push @order, 'first' },
                { phase => 'in_transaction' })
            ->on($self->schema, '*' => sub { push @order, 'second' },
                { phase => 'in_transaction' });

        $self->schema->table('Department')->insert({
            name => 'Multi', location => 'Here',
        });

        is(scalar @order, 2, 'both custom listeners fired');
        is_deeply(\@order, ['first', 'second'], 'listener order preserved');

        $self->setup_cdc(capture_old => 1);
    };
}

sub test_operation_specific_listeners {
    my ($self) = @_;
    subtest 'Operation-specific listeners with real DML' => sub {
        plan tests => 3;
        $self->clean;

        my @insert_seen;
        my @update_seen;
        my @delete_seen;

        $CDC->setup($self->schema, tables => 'all', capture_old => 1, force => 1)
            ->log_to_dbi($self->schema, 'cdc_events')
            ->on($self->schema, 'INSERT' => sub { push @insert_seen, 1 },
                { phase => 'in_transaction' })
            ->on($self->schema, 'UPDATE' => sub { push @update_seen, 1 },
                { phase => 'in_transaction' })
            ->on($self->schema, 'DELETE' => sub { push @delete_seen, 1 },
                { phase => 'in_transaction' });

        $self->schema->table('Department')->insert({
            name => 'Filter', location => 'A',
        });
        my $dept = $self->schema->table('Department')
            ->select(-where => { name => 'Filter' })->[0];
        $dept->update({ location => 'B' });
        $dept->delete;

        is(scalar @insert_seen, 1, 'INSERT listener fired once');
        is(scalar @update_seen, 1, 'UPDATE listener fired once');
        is(scalar @delete_seen, 1, 'DELETE listener fired once');

        $self->setup_cdc(capture_old => 1);
    };
}

1;

__END__

=head1 NAME

CDCTestSuite - Shared e2e tests for DBIx::DataModel::Plugin::CDC

=head1 SYNOPSIS

    use lib 't/lib';
    use CDCTestSuite::Schema;
    use CDCTestSuite;

    CDCTestSuite::Schema->dbh($dbh);
    my $suite = CDCTestSuite->new(dbh => $dbh);
    $suite->setup_cdc(capture_old => 1);
    $suite->run_common_suite;

=head1 DESCRIPTION

Contains 30 backend-agnostic subtests that exercise the full CDC
pipeline.  Backend-specific test files provide the C<$dbh> and DDL,
then call C<run_common_suite()>.

=cut
