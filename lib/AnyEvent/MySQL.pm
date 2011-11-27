package AnyEvent::MySQL;

use 5.006;
use strict;
use warnings;

=head1 NAME

AnyEvent::MySQL - The great new AnyEvent::MySQL!

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';


=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use AnyEvent::MySQL;

    my $foo = AnyEvent::MySQL->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 SUBROUTINES/METHODS

=cut

use AE;
use AnyEvent::Socket;
use AnyEvent::Handle;
use Scalar::Util qw(weaken dualvar);

use AnyEvent::MySQL::Imp;

use constant {
    BUSY => 1,
    IDLE => 2,
    ZOMBIE => 3,
    ZOMBIE_DB => 4,
    ZOMBIE_ST => 5,
    DONE => 6,
};

sub _empty_cb {}

=head2 $dbh = AnyEvent::MySQL->connect($data_source, $username, [$auth, [\%attr,]] $cb->($dbh, 1))

=cut
sub connect {
    shift;
    return AnyEvent::MySQL::db->new(@_);
}

package AnyEvent::MySQL::db;

use strict;
use warnings;

use AE;
use AnyEvent::Socket;
use AnyEvent::Handle;
use Scalar::Util qw(weaken dualvar);

use constant {
    BUSY => 1,
    IDLE => 2,
    ZOMBIE => 3,
};

use constant {
    DSNi => 0,
    USERNAMEi => 1,
    AUTHi => 2,
    ATTRi => 3,
    HDi => 4,
    STATEi => 5,
    TASKi => 6,
    STi => 7,
    RENEWi => 8,
};

*errstr = *AnyEvent::MySQL::errstr;
*errno = *AnyEvent::MySQL::errno;
our $errstr;
our $errno;

sub _warn_when_verbose {
    my($dbh, $level) = @_;
    $level ||= 1;
    if( $dbh->[ATTRi]{Verbose} ) {
        my($package, $filename, $line) = caller($level+1);
        warn "$errstr ($errno) at $filename line $line\n";
    }
}

sub _consume_task {
    my $dbh = shift;
    my $loop_sub; $loop_sub = sub {
        shift @{$dbh->[STi]} while( @{$dbh->[STi]} && !$dbh->[STi][0] );
        pop @{$dbh->[STi]} while( @{$dbh->[STi]} && !$dbh->[STi][-1] );
        if( $dbh->[RENEWi] ) {
            undef $loop_sub;
            _reconnect($dbh);
            return;
        }
        if( $dbh->[STATEi]==ZOMBIE ) {
            undef $loop_sub;
            _zombie_flush($dbh);
            return;
        }
        if( my $task = shift @{$dbh->[TASKi]} ) {
            $dbh->[STATEi] = BUSY;
            $task->[0]($loop_sub);
        }
        else {
            $dbh->[STATEi] = IDLE;
            undef $loop_sub;
        }
    }; $loop_sub->();
}

sub _check_and_act {
    my($dbh, $act, $cb) = @_;
    if( $dbh->[STATEi]==ZOMBIE ) {
        local $errno = 2006;
        local $errstr = 'MySQL server has gone away';
        $cb->();
        return;
    }
    push @{$dbh->[TASKi]}, [$act, $cb];
    _consume_task($dbh) if( $dbh->[STATEi]==IDLE );
}

sub _zombie_flush {
    my($dbh) = @_;
    $dbh->[STATEi] = ZOMBIE;
    local $errno = 2006;
    local $errstr = 'MySQL server has gone away';

    my $tasks = $dbh->[TASKi];
    $dbh->[TASKi] = [];
    my $sts = $dbh->[STi];
    $dbh->[STi] = [];

    for my $task (@$tasks) {
        $task->[1]();
    }

    for my $st (@$sts) {
        AnyEvent::MySQL::st::_zombie_parent_flush($st) if $st;
    }
}

sub _reconnect {
    my $dbh = shift;
    $dbh->[STATEi] = BUSY;
    $dbh->[RENEWi] = 0;
    my $retry; $retry = AE::timer .1, 0, sub {
        undef $retry;
        _connect($dbh);
    };
}

sub _after_connect {
    my $dbh = shift;
    $dbh->[STi] = [ grep { $_ } @{$dbh->[STi]} ];
    weaken $_ for(@{$dbh->[STi]});
    my $sts = [ @{$dbh->[STi]} ];

    my $prepare_sub; $prepare_sub = sub {
        shift @$sts while( @$sts && !$sts->[0] );
        if( @$sts ) {
            my $st = shift @$sts;
            AnyEvent::MySQL::st::_prepare($st, $prepare_sub, \&AnyEvent::MySQL::_empty_cb);
        }
        else {
            undef $prepare_sub;
            _consume_task($dbh);
        }
    }; $prepare_sub->();
}

sub _connect {
    my $dbh = shift;
    my $cb = shift || \&AnyEvent::MySQL::_empty_cb;
    if( $dbh->[DSNi] =~ /^DBI:mysql:(.*)$/ ) {
        my $param = $1;
        my $database;
        if( index($param, '=')>=0 ) {
            $param = {
                map { split /=/, $_, 2 } split /;/, $param
            };
            if( $param->{host} =~ /(.*):(.*)/ ) {
                $param->{host} = $1;
                $param->{port} = $2;
            }
        }
        else {
            $param = { database => $param };
        }

        $param->{port} ||= 3306;

        if( $param->{host} eq '' || $param->{host} eq 'localhost' ) { # unix socket
            local $errno = 2054;
            local $errstr = "unix socket not implement yet";
            $cb->();
            _zombie_flush($dbh);
        }
        else {
            tcp_connect($param->{host}, $param->{port}, sub {
                my $fh = shift;
                if( !$fh ) {
                    warn "Connect to $param->{host}:$param->{port} fail: $!  retry later.";

                    _reconnect($dbh);
                    return;
                }

                weaken( my $wdbh = $dbh );
                $dbh->[HDi] = AnyEvent::Handle->new(
                    fh => $fh,
                    on_error => sub {
                        $wdbh->[RENEWi] = 1; 
                    },
                );

                AnyEvent::MySQL::Imp::do_auth($dbh->[HDi], $dbh->[USERNAMEi], $dbh->[AUTHi], $param->{database}, sub {
                    my($success, $err_num_and_msg) = @_;
                    if( $success ) {
                        if( $dbh->[ATTRi]{on_connect} ) {
                            $dbh->[ATTRi]{on_connect}($dbh, sub {
                                $cb->($dbh);
                                _after_connect($dbh);
                            });
                        }
                        else {
                            $cb->($dbh);
                            _after_connect($dbh);
                        }
                    }
                    else {
                        warn "MySQL auth error: $err_num_and_msg  retry later.";
                        _reconnect($dbh);
                    }
                });
            });
        }
    }
    else {
        local $errno = 2054;
        local $errstr = "data_source should be begin with 'DBI:mysql:'";
        $cb->();
        _zombie_flush($dbh);
    }
}

=head2 $dbh = AnyEvent::MySQL::db->new($dsn, $username, [$auth, [\%attr,]] $cb->($dbh))

    If failed, the $dbh in the $cb's args will be undef.

    Additional attr:

    on_connect => $cb->($dbh, $next->())

=cut
sub new {
    my $cb = pop;
    my($class, $dsn, $username, $auth, $attr) = @_;

    my $dbh = bless [], $class;
    $dbh->[DSNi] = $dsn;
    $dbh->[USERNAMEi] = $username;
    $dbh->[AUTHi] = $auth;
    $dbh->[ATTRi] = +{ Verbose => 1, %{ $attr || {} } };
    $dbh->[STATEi] = BUSY;
    $dbh->[TASKi] = [];
    $dbh->[STi] = [];

    _connect($dbh);

    return $dbh;
}

=head2 $dbh->do($statement, [\%attr, [@bind_values,]] [$cb->($rv)])

=cut
sub do {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($dbh, $statement, $attr, @bind_values) = @_;

    my $act = sub {
        my $next_act = shift;
        AnyEvent::MySQL::Imp::send_packet($dbh->[HDi], 0, AnyEvent::MySQL::Imp::COM_QUERY, $statement);
        AnyEvent::MySQL::Imp::recv_response($dbh->[HDi], sub {
            if( $_[0]==AnyEvent::MySQL::Imp::RES_OK ) {
                $cb->($_[1]);
            }
            elsif( $_[0]==AnyEvent::MySQL::Imp::RES_ERROR ) {
                local $errno = $_[1];
                local $errstr = $_[3];
                $cb->();
            }
            else {
                $cb->(0+@{$_[2]});
            }
            $next_act->();
        });
    };

    if( $attr && $attr->{Tx} ) {
        AnyEvent::MySQL::tx::_check_and_act($attr->{Tx}, $act, $cb);
    }
    else {
        _check_and_act($dbh, $act, $cb);
    }
}

=head2 $dbh->selectall_arrayref($statement, [\%attr,] $cb->($ary_ref))

=cut
sub selectall_arrayref {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($dbh, $statement, $attr) = @_;

    my $act = sub {
        my $next_act = shift;
        AnyEvent::MySQL::Imp::send_packet($dbh->[HDi], 0, AnyEvent::MySQL::Imp::COM_QUERY, $statement);
        AnyEvent::MySQL::Imp::recv_response($dbh->[HDi], sub {
            if( $_[0]==AnyEvent::MySQL::Imp::RES_OK ) {
                $cb->([]);
            }
            elsif( $_[0]==AnyEvent::MySQL::Imp::RES_ERROR ) {
                local $errno = $_[1];
                local $errstr = $_[3];
                $cb->();
            }
            else {
                $cb->($_[2]);
            }
            $next_act->();
        });
    };

    if( $attr && $attr->{Tx} ) {
        AnyEvent::MySQL::tx::_check_and_act($attr->{Tx}, $act, $cb);
    }
    else {
        _check_and_act($dbh, $act, $cb);
    }
}

=head2 $dbh->selectall_hashref($statement, ($key_field|\@key_field), [\%attr,] $cb->($hash_ref))

=cut
sub selectall_hashref {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my $attr = ref($_[-1]) eq 'HASH' ? pop : {};
    my($dbh, $statement, $key_field) = @_;

    my @key_field;
    if( ref($key_field) eq 'ARRAY' ) {
        @key_field = @$key_field;
    }
    elsif( defined($key_field) ) {
        @key_field = ($key_field);
    }
    else {
        @key_field = ();
    }

    my $act = sub {
        my $next_act = shift;
        AnyEvent::MySQL::Imp::send_packet($dbh->[HDi], 0, AnyEvent::MySQL::Imp::COM_QUERY, $statement);
        AnyEvent::MySQL::Imp::recv_response($dbh->[HDi], sub {
            if( $_[0]==AnyEvent::MySQL::Imp::RES_OK ) {
                if( @key_field ) {
                    $cb->({});
                }
                else {
                    $cb->([]);
                }
            }
            elsif( $_[0]==AnyEvent::MySQL::Imp::RES_ERROR ) {
                local $errno = $_[1];
                local $errstr = $_[3];
                $cb->();
            }
            else {
                my $res;
                if( @key_field ) {
                    $res = {};
                }
                else {
                    $res = [];
                }
                for(my $i=$#{$_[2]}; $i>=0; --$i) {
                    my %record;
                    for(my $j=$#{$_[2][$i]}; $j>=0; --$j) {
                        $record{$_[1][$j][4]} = $_[2][$i][$j];
                    }
                    if( @key_field ) {
                        my $h = $res;
                        for(@key_field[0..$#key_field-1]) {
                            $h->{$record{$_}} ||= {};
                            $h = $h->{$record{$_}};
                        }
                        $h->{$record{$key_field[-1]}} = \%record;
                    }
                    else {
                        push @$res, \%record;
                    }
                }
                $cb->($res);
            }
            $next_act->();
        });
    };

    if( $attr && $attr->{Tx} ) {
        AnyEvent::MySQL::tx::_check_and_act($attr->{Tx}, $act, $cb);
    }
    else {
        _check_and_act($dbh, $act, $cb);
    }
}

=head2 $dbh->selectcol_arrayref($statement, [\%attr,] $cb->($ary_ref))

=cut
sub selectcol_arrayref {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($dbh, $statement, $attr) = @_;
    $attr ||= {};
    my @columns = map { $_-1 } @{ $attr->{Columns} || [1] };

    my $act = sub {
        my $next_act = shift;
        AnyEvent::MySQL::Imp::send_packet($dbh->[HDi], 0, AnyEvent::MySQL::Imp::COM_QUERY, $statement);
        AnyEvent::MySQL::Imp::recv_response($dbh->[HDi], sub {
            if( $_[0]==AnyEvent::MySQL::Imp::RES_OK ) {
                $cb->([]);
            }
            elsif( $_[0]==AnyEvent::MySQL::Imp::RES_ERROR ) {
                local $errno = $_[1];
                local $errstr = $_[3];
                $cb->();
            }
            else {
                my @res = map {
                    my $r = $_;
                    map { $r->[$_] } @columns
                } @{$_[2]};
                $cb->(\@res);
            }
            $next_act->();
        });
    };

    if( $attr && $attr->{Tx} ) {
        AnyEvent::MySQL::tx::_check_and_act($attr->{Tx}, $act, $cb);
    }
    else {
        _check_and_act($dbh, $act, $cb);
    }
}

=head2 $sth = $dbh->prepare($statement, [$cb->($sth)])

=cut
sub prepare {
    my $dbh = $_[0];

    if( $dbh->[STATEi]==ZOMBIE ) {
        return AnyEvent::MySQL::st->new_zombie_db(@_);
    }
    else {
        my $sth = AnyEvent::MySQL::st->new(@_);
        push @{$dbh->[STi]}, $sth;
        weaken($dbh->[STi][-1]);
        return $sth;
    }
}

=head2 $txh = $dbh->begin_work([$cb->($txh)])

=cut
sub begin_work {
    my $dbh = $_[0];

    if( $dbh->[STATEi]==ZOMBIE ) {
        return AnyEvent::MySQL::tx->new_zombie_db(@_);
    }
    else {
        return AnyEvent::MySQL::tx->new(@_);
    }
}

package AnyEvent::MySQL::tx;

use strict;
use warnings;

use constant {
    BUSY => 1,
    IDLE => 2,
    ZOMBIE => 3,
    DONE => 4,
    NEW => 5,
};

use constant {
    DBHi => 0,
    STATEi => 1,
    TASKi => 2,
    NEXTi => 3,
};

sub _consume_task {
    my $txh = shift;
    my $loop_sub;
    my $fail_sub = sub {
        undef $loop_sub;
        _zombie_flush($txh, $_[0]);
        delete($txh->[NEXTi])->();
    };
    $loop_sub = sub {
        if( my $task = shift @{$txh->[TASKi]} ) {
            $txh->[STATEi] = BUSY;
            $task->[0]($loop_sub, $fail_sub);
        }
        else {
            $txh->[STATEi] = IDLE;
            undef $loop_sub;
        }
    }; $loop_sub->();
}

sub _check_and_act {
    my($txh, $act, $cb) = @_;
    if( $txh->[STATEi]==ZOMBIE ) {
        local $errno = 2000;
        local $errstr = 'This transaction has been aborted';
        $cb->();
        return;
    }
    if( $txh->[STATEi]==DONE ) {
        local $errno = 2000;
        local $errstr = 'This transaction has been committed';
        $cb->();
        return;
    }
    push @{$txh->[TASKi]}, [$act, $cb];
    _consume_task($txh) if( $txh->[STATEi]==IDLE );
}

sub _zombie_flush {
    my($txh, $state) = @_;
    $txh->[STATEi] = ZOMBIE;

    my $tasks = $txh->[TASKi];
    $txh->[TASKi] = [];

    for my $task (@$tasks) {
        $task->[1]();
    }
}

sub DESTROY {
    my $txh = shift;
    if( $txh->[NEXTi] ) {
        my $dbh = $txh->[DBHi];
        my $hd = $dbh->[AnyEvent::MySQL::db::HDi];
        AnyEvent::MySQL::Imp::send_packet($hd, 0, AnyEvent::MySQL::Imp::COM_QUERY, 'rollback');
        AnyEvent::MySQL::Imp::recv_response($hd, \&AnyEvent::MySQL::_empty_cb);
    }
    _zombie_flush($txh, $txh->[STATEi]==DONE ? DONE : ZOMBIE);
    $txh->[NEXTi]() if( $txh->[NEXTi] );
}

=head2 $txh = AnyEvent::MySQL::tx->new($dbh, [$cb->($rv)])

=cut
sub new {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($class, $dbh) = @_;
    my $txh = bless [], $class;
    $txh->[DBHi] = $dbh;
    $txh->[STATEi] = NEW;
    $txh->[TASKi] = [];

    AnyEvent::MySQL::db::_check_and_act($dbh, sub {
        my $next_act = shift;
        $txh->[NEXTi] = $next_act;
        AnyEvent::MySQL::Imp::send_packet($dbh->[AnyEvent::MySQL::db::HDi], 0, AnyEvent::MySQL::Imp::COM_QUERY, 'begin');
        AnyEvent::MySQL::Imp::recv_response($dbh->[AnyEvent::MySQL::db::HDi], sub {
            if( $_[0]==AnyEvent::MySQL::Imp::RES_OK ) {
                $cb->($txh);
                _consume_task($txh);
                return;
            }
            if( $_[0]==AnyEvent::MySQL::Imp::RES_ERROR ) {
                local $errno = $_[1];
                local $errstr = $_[3];
                $cb->();
            }
            else {
                local $errno = 2000;
                local $errstr = "Unexpected result: $_[0]";
                $cb->();
            }
            _zombie_flush($txh, ZOMBIE);
            $next_act->();
        });
    }, $cb);

    return $txh;
}

=head2 $txh->commit($cb->($rv))

=cut
sub commit {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my $txh = shift;

    _check_and_act($txh, sub {
        my $dbh = $txh->[DBHi];
        my $hd = $dbh->[AnyEvent::MySQL::db::HDi];
        AnyEvent::MySQL::Imp::send_packet($hd, 0, AnyEvent::MySQL::Imp::COM_QUERY, 'commit');
        AnyEvent::MySQL::Imp::recv_response($hd, sub {
            if( $_[0]==AnyEvent::MySQL::Imp::RES_OK ) {
                _zombie_flush($txh, DONE);
                $cb->(1);
            }
            else {
                if( $_[0]==AnyEvent::MySQL::Imp::RES_ERROR ) {
                    local $errno = $_[1];
                    local $errstr = $_[3];
                }
                else {
                    local $errno = 2000;
                    local $errstr = "Unexpected result: $_[0]";
                }
                _zombie_flush($txh, ZOMBIE);
                $cb->();
            }
            delete($txh->[NEXTi])->();
        });
    }, $cb);
}

=head2 $txh->rollback($cb->($rv))

=cut
sub rollback {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my $txh = shift;

    _check_and_act($txh, sub {
        my $dbh = $txh->[DBHi];
        my $hd = $dbh->[AnyEvent::MySQL::db::HDi];
        AnyEvent::MySQL::Imp::send_packet($hd, 0, AnyEvent::MySQL::Imp::COM_QUERY, 'rollback');
        AnyEvent::MySQL::Imp::recv_response($hd, sub {
            if( $_[0]==AnyEvent::MySQL::Imp::RES_OK ) {
                _zombie_flush($txh, ZOMBIE);
                $cb->(1);
            }
            else {
                if( $_[0]==AnyEvent::MySQL::Imp::RES_ERROR ) {
                    local $errno = $_[1];
                    local $errstr = $_[3];
                }
                else {
                    local $errno = 2000;
                    local $errstr = "Unexpected result: $_[0]";
                }
                _zombie_flush($txh, ZOMBIE);
                $cb->();
            }
            delete($txh->[NEXTi])->();
        });
    }, $cb);
}

package AnyEvent::MySQL::st;

use strict;
use warnings;

use Scalar::Util qw(weaken);

use constant {
    BUSY => 1,
    IDLE => 2,
    ZOMBIE => 3,
    NEW => 4,
};

use constant {
    DBHi => 0,
    IDi => 1,
    STATEi => 2,
    TASKi => 3,
    FTi => 4,
    PARAMi => 5,
    FIELDi => 6,
    STATEMENTi => 7,
};

*errstr = *AnyEvent::MySQL::errstr;
*errno = *AnyEvent::MySQL::errno;
our $errstr;
our $errno;

sub _consume_task {
    my $sth = shift;
    if( $sth->[STATEi]==ZOMBIE ) {
        _zombie_flush($sth);
        return;
    }

    my $dbh = $sth->[DBHi];
    my $tasks = $sth->[TASKi];
    $sth->[TASKi] = [];
    for my $task (@$tasks) {
        AnyEvent::MySQL::db::_check_and_act($dbh, $task->[0], $task->[1]);
    }
}

sub _check_and_act {
    my($sth, $act, $cb) = @_;
    if( $sth->[STATEi]==ZOMBIE ) {
        local $errno = 2030;
        local $errstr = 'Statement not prepared';
        $cb->();
        return;
    }
    push @{$sth->[TASKi]}, [$act, $cb];
    _consume_task($sth) if( $sth->[STATEi]==IDLE );
}

sub _zombie_flush_common {
    my($sth) = @_;

    my $tasks = $sth->[TASKi];
    $sth->[TASKi] = [];
    my $fts = $sth->[FTi];
    $sth->[FTi] = [];

    for my $task (@$tasks) {
        $task->[1]();
    }

    for my $ft (@$fts) {
        AnyEvent::MySQL::ft::_zombie_parent_flush($ft) if $ft;
    }
}

sub _zombie_flush {
    my($sth) = @_;
    $sth->[STATEi] = ZOMBIE;
    local $errno = 2030;
    local $errstr = 'Statement not prepared';

    _zombie_flush_common($sth);
}

sub _zombie_parent_flush {
    my($sth) = @_;
    $sth->[STATEi] = ZOMBIE;
    local $errno = 2006;
    local $errstr = 'MySQL server has gone away';

    _zombie_flush_common($sth);
}

sub _prepare {
    my($sth, $next_act, $cb) = @_;
    if( $sth->[STATEi]==NEW ) {
        $next_act->();
        return;
    }
    my $dbh = $sth->[DBHi];

    my $hd = $dbh->[AnyEvent::MySQL::db::HDi];
    AnyEvent::MySQL::Imp::send_packet($hd, 0, AnyEvent::MySQL::Imp::COM_STMT_PREPARE, $sth->[STATEMENTi]);
    AnyEvent::MySQL::Imp::recv_response($hd, prepare => 1, sub {
        if( $_[0]==AnyEvent::MySQL::Imp::RES_PREPARE ) {
            $sth->[IDi] = $_[1];
            $sth->[PARAMi] = $_[2];
            $sth->[FIELDi] = $_[3];

            $cb->($sth);
        }
        else {
            if( $_[0]==AnyEvent::MySQL::Imp::RES_ERROR ) {
                local $errno = $_[1];
                local $errstr = $_[3];
                $cb->();
            }
            else {
                local $errno = 2000;
                local $errstr = "Unexpected response: $_[0]";
                $cb->();
            }
            _zombie_flush($sth);
        }
        $next_act->();
    });
}

sub DESTROY {
    my $sth = shift;
    my $dbh = $sth->[DBHi];
    my $hd = $dbh->[AnyEvent::MySQL::db::HDi];
    AnyEvent::MySQL::Imp::send_packet($hd, 0, AnyEvent::MySQL::Imp::COM_STMT_CLOSE, pack('V', $sth->[IDi]));
    AnyEvent::MySQL::Imp::response($hd, \&AnyEvent::MySQL::_empty_cb);
}

=head2 $sth_zombie = AnyEvent::MySQL::st->new_zombie_db($dbh, $statement, [$cb->($sth)])

=cut
sub new_zombie_db {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($class, $dbh, $statement) = @_;
    my $sth = bless [], $class;
    $sth->[STATEi] = ZOMBIE;
    local $errno = 2006;
    local $errstr = 'MySQL server has gone away';
    $cb->();
    return $sth;
}

=head2 $sth = AnyEvent::MySQL::st->new($dbh, $statement, [$cb->($sth)])

=cut
sub new {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($class, $dbh, $statement) = @_;
    my $sth = bless [], $class;
    $sth->[DBHi] = $dbh;
    $sth->[TASKi] = [];
    $sth->[STATEi] = NEW;
    $sth->[FTi] = [];
    $sth->[STATEMENTi] = $statement;

    AnyEvent::MySQL::db::_check_and_act($dbh, sub {
        my $next_act = shift;
        $sth->[STATEi] = BUSY;
        _prepare($sth, sub {
            _consume_task($sth);
            $next_act->();
        }, $cb);
    }, sub {
        $sth->[STATEi] = ZOMBIE;
        _zombie_flush($sth);
    });
    return $sth;
}

=head2 $fth = $sth->execute(@bind_values, [\%attr,] [$cb->($fth, $rv)])

=cut
sub execute {
    my $sth = $_[0];

    if( $sth->[STATEi]==ZOMBIE ) {
        return AnyEvent::MySQL::ft->new_zombie_st(@_);
    }
    else {
        my $fth = AnyEvent::MySQL::ft->new(@_);
        push @{$sth->[FTi]}, $fth;
        weaken($sth->[FTi][-1]);
        return $fth;
    }
}

package AnyEvent::MySQL::ft;

use strict;
use warnings;

use constant {
    BUSY => 1,
    IDLE => 2,
    ZOMBIE => 3,
};

use constant {
    STHi => 0,
    STATEi => 1,
    TASKi => 2,
    DATAi => 3,
    BINDi => 4,
};

sub _consume_task {
    my $fth = shift;
    my $loop_sub; $loop_sub = sub {
        if( $fth->[STATEi]==ZOMBIE ) {
            undef $loop_sub;
            _zombie_flush($fth);
            return;
        }
        if( my $task = shift @{$fth->[TASKi]} ) {
            $fth->[STATEi] = BUSY;
            $task->[0]($loop_sub);
        }
        else {
            $fth->[STATEi] = IDLE;
            undef $loop_sub;
        }
    }; $loop_sub->();
}

sub _check_and_act {
    my($fth, $act, $cb) = @_;
    if( $fth->[STATEi]==ZOMBIE ) {
        local $errno = 2030;
        local $errstr = 'Statement not prepared';
        $cb->();
        return;
    }
    push @{$fth->[TASKi]}, [$act, $cb];
    _consume_task($fth) if( $fth->[STATEi]==IDLE );
}

sub _zombie_flush {
    my($fth) = @_;
    local $errno = 2030;
    local $errstr = 'Statement not prepared';
    _zombie_parent_flush($fth);
}

sub _zombie_parent_flush {
    my($fth) = @_;
    $fth->[STATEi] = ZOMBIE;

    my $tasks = $fth->[TASKi];
    $fth->[TASKi] = [];

    for my $task (@$tasks) {
        $task->[1]();
    }
}

=head2 $fth = AnyEvent::MySQL::ft->new_zombie_st($sth, @bind_values, [$cb->($fth, $rv)])

=cut
sub new_zombie_st {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($class, $sth) = @_;

    my $fth = bless [], $class;
    $fth->[STATEi] = ZOMBIE;

    local $errno = 2030;
    local $errstr = 'Statement not prepared';
    $cb->();

    return $fth;
}

=head2 $fth = AnyEvent::MySQL::ft->new($sth, @bind_values, [\%attr,] [$cb->($fth, $rv)])

=cut
sub new {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my $attr = ref($_[-1]) eq 'HASH' ? pop : {};
    my($class, $sth, @bind_values) = @_;

    my $fth = bless [], $class;
    $fth->[STHi] = $sth;
    $fth->[STATEi] = BUSY;
    $fth->[TASKi] = [];

    AnyEvent::MySQL::st::_check_and_act($sth, sub {
        my $next_act = shift;
        my $dbh = $sth->[AnyEvent::MySQL::st::DBHi];
        my $hd = $dbh->[AnyEvent::MySQL::db::HDi];
        my $id = $sth->[AnyEvent::MySQL::st::IDi];

#        AnyEvent::MySQL::Imp::do_reset_stmt($hd, $id);
#        my $packet_num = 0;
#        my $null_bit_map = "\0" x ( 7 + @bind_values >> 3 );
#        my $i = 0;
#        for(@bind_values) {
#            if( defined($_) ) {
#                AnyEvent::MySQL::Imp::do_long_data_packet($hd, $id, $i, $sth->[AnyEvent::MySQL::st::PARAMi][$i][8], $_, $sth->[AnyEvent::MySQL::st::PARAMi][$i][7], $sth->[AnyEvent::MySQL::st::PARAMi][$i][9], $packet_num);
#                #++$packet_num;
#            }
#            else {
#                vec($null_bit_map, $i, 1) = 1;
#            }
#            ++$i;
#        }
#        AnyEvent::MySQL::Imp::do_execute($hd, $id, $null_bit_map, $packet_num);

        AnyEvent::MySQL::Imp::do_execute_param($hd, $id, \@bind_values, $sth->[AnyEvent::MySQL::st::PARAMi]);
        AnyEvent::MySQL::Imp::recv_response($hd, prepare => 1, sub {
            if( $_[0]==AnyEvent::MySQL::Imp::RES_RESULT ) {
                $fth->[DATAi] = $_[2];
                $cb->($fth);
                _consume_task($fth);
            }
            elsif( $_[0]==AnyEvent::MySQL::Imp::RES_ERROR ) {
                local $errno = $_[1];
                local $errstr = $_[3];
                $cb->();
                _zombie_flush($fth);
            }
            else {
                local $errno = 2000;
                local $errstr = "Unknown response: $_[0]";
                $cb->();
                _zombie_flush($fth);
            }
            $next_act->();
        });
    }, $cb);

    return $fth;
}

=head2 $fth->bind_columns(@list_of_refs_to_vars_to_bind, [$cb->($rc)])

=cut
sub bind_columns {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my $fth = shift;
    my @list_of_refs_to_vars_to_bind = @_;
    _check_and_act($fth, sub {
        my $next_act = shift;
        my $sth = $fth->[STHi];
        if( 0+@list_of_refs_to_vars_to_bind == 0+@{$sth->[AnyEvent::MySQL::st::FIELDi]} ) {
            $fth->[BINDi] = \@list_of_refs_to_vars_to_bind;
            $cb->(1);
        }
        else {
            local $errno = 2000;
            local $errstr = "Column num not matched (should be @{[0+@{$sth->[AnyEvent::MySQL::st::FIELDi]}]})";
            $cb->();
        }
        $next_act->();
    }, $cb);
}

=head2 $fth->bind_col($col_num, \$col_variable, [$cb->($rc)])

=cut
sub bind_col {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($fth, $col_num, $col_ref) = @_;
    _check_and_act($fth, sub {
        my $next_act = shift;
        my $sth = $fth->[STHi];
        if( 0<=$col_num && $col_num<@{$sth->[AnyEvent::MySQL::st::FIELDi]} ) {
            $fth->[BINDi] ||= [];
            $fth->[BINDi][$col_num] = $col_ref;
            $cb->(1);
        }
        else {
            local $errno = 2000;
            local $errstr = "Column num not matched (should be between 0 and @{[0+@{$sth->[AnyEvent::MySQL::st::FIELDi]}]})";
            $cb->();
        }
        $next_act->();
    }, $cb);
}

=head2 $fth->fetch($cb->($rv))

=cut
sub fetch {
    my $cb = pop;
    my $fth = shift;
    _check_and_act($fth, sub {
        my $next_act = shift;
        if( $fth->[BINDi] && $fth->[DATAi] && @{$fth->[DATAi]} ) {
            my $bind = $fth->[BINDi];
            my $row = shift @{$fth->[DATAi]};
            for(my $i=0; $i<@$row; ++$i) {
                ${$bind->[$i]} = $row->[$i] if $bind->[$i];
            }
            $cb->(1);
        }
        else {
            $cb->();
        }
        $next_act->();
    }, $cb);
}

=head2 $fth->fetchrow_array($cb->(@row_ary))

=cut
sub fetchrow_array {
    my $cb = pop;
    my $fth = shift;
    _check_and_act($fth, sub {
        my $next_act = shift;
        if( $fth->[DATAi] && @{$fth->[DATAi]} ) {
            $cb->(@{ shift @{$fth->[DATAi]} });
        }
        else {
            $cb->();
        }
        $next_act->();
    });
}

=head2 $fth->fetchrow_arrayref($cb->($ary_ref))

=cut
sub fetchrow_arrayref {
    my $cb = pop;
    my $fth = shift;
    _check_and_act($fth, sub {
        my $next_act = shift;
        if( $fth->[DATAi] && @{$fth->[DATAi]} ) {
            $cb->(shift @{$fth->[DATAi]});
        }
        else {
            $cb->();
        }
        $next_act->();
    });
}

=head2 $fth->fetchrow_hashref($cb->($hash_ref))

=cut
sub fetchrow_hashref {
    my $cb = pop;
    my $fth = shift;
    _check_and_act($fth, sub {
        my $next_act = shift;
        if( $fth->[DATAi] && @{$fth->[DATAi]} ) {
            my $field = $fth->[STHi][AnyEvent::MySQL::st::FIELDi];
            my $hash = {};
            my $row = shift @{$fth->[DATAi]};
            for(my $i=0; $i<@$row; ++$i) {
                $hash->{$field->[$i][4]} = $row->[$i];
            }
            $cb->($hash);
        }
        else {
            $cb->();
        }
        $next_act->();
    });
}

=head1 AUTHOR

Cindy Wang (CindyLinz)

=head1 BUGS

Please report any bugs or feature requests to C<http://github.com/CindyLinz/Perl-AnyEvent-MySQL>.
I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc AnyEvent::MySQL


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=AnyEvent-MySQL>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/AnyEvent-MySQL>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/AnyEvent-MySQL>

=item * Search CPAN

L<http://search.cpan.org/dist/AnyEvent-MySQL/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2011 Cindy Wang (CindyLinz).

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1; # End of AnyEvent::MySQL
