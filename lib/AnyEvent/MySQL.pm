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
    BUSY => 0,
    IDLE => 1,
    ZOMBIE => 2,
    ZOMBIE_DB => 3,
    ZOMBIE_ST => 4,
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
    DSNi => 0,
    USERNAMEi => 1,
    AUTHi => 2,
    ATTRi => 3,
    HDi => 4,
    STATEi => 5,
    TASKi => 6,
    STi => 7,
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
        if( my $task = shift @{$dbh->[TASKi]} ) {
            $dbh->[STATEi] = AnyEvent::MySQL::BUSY;
            $task->[0]($loop_sub);
        }
        else {
            $dbh->[STATEi] = AnyEvent::MySQL::IDLE;
            undef $loop_sub;
        }
    }; $loop_sub->();
}

sub _check_and_act {
    my($dbh, $act, $cb) = @_;
    if( $dbh->[STATEi]==AnyEvent::MySQL::ZOMBIE ) {
        local $errno = 2006;
        local $errstr = 'MySQL server has gone away';
        $cb->();
    }
    elsif( $dbh->[STATEi]==AnyEvent::MySQL::BUSY ) {
        push @{$dbh->[TASKi]}, [$act, $cb];
    }
    elsif( $dbh->[STATEi]==AnyEvent::MySQL::IDLE ) {
        push @{$dbh->[TASKi]}, [$act, $cb];
        _consume_task($dbh);
    }
    else {
        local $errno = 2000;
        local $errstr = "Unknown state: $dbh->[STATEi]";
        _warn_when_verbose($dbh, 2);
        $cb->();
    }
}

sub _zombie_flush {
    my($dbh) = @_;
    $dbh->[STATEi] = AnyEvent::MySQL::ZOMBIE;
#    local $errno = 2006;
#    local $errstr = 'MySQL server has gone away';

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
    $dbh->[STATEi] = AnyEvent::MySQL::BUSY;
    $dbh->[TASKi] = [];
    $dbh->[STi] = [];

    if( $dsn =~ /^DBI:mysql:(.*)$/ ) {
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
            $dbh->[STATEi] = AnyEvent::MySQL::ZOMBIE;
            _warn_when_verbose($dbh);
            $cb->();
        }
        else {
            tcp_connect($param->{host}, $param->{port}, sub {
                my $fh = shift;
                if( !$fh ) {
                    local $errno = 2003;
                    local $errstr = "Connect to $param->{host}:$param->{port} fail: $!";
                    $dbh->[STATEi] = AnyEvent::MySQL::ZOMBIE;
                    _warn_when_verbose($dbh);
                    $cb->();
                    return;
                }

                weaken( my $wdbh = $dbh );
                $dbh->[HDi] = AnyEvent::Handle->new(
                    fh => $fh,
                    on_error => sub {
                        $wdbh->[STATEi] = AnyEvent::MySQL::ZOMBIE;
                    },
                );

                AnyEvent::MySQL::Imp::do_auth($dbh->[HDi], $username, $auth, $param->{database}, sub {
                    my($success, $err_num_and_msg) = @_;
                    if( $success ) {
                        $dbh->[STATEi] = AnyEvent::MySQL::IDLE;

                        if( $dbh->[ATTRi]{on_connect} ) {
                            $dbh->[ATTRi]{on_connect}($dbh, sub {
                                $cb->($dbh);
                                _consume_task($dbh);
                            });
                        }
                        else {
                            $cb->($dbh);
                            _consume_task($dbh);
                        }
                    }
                    else {
                        local $errno = 2012;
                        local $errstr = $err_num_and_msg;
                        _warn_when_verbose($dbh);
                        $cb->();
                        _zombie_flush($dbh);
                    }
                });
            });
        }
    }
    else {
        local $errno = 2054;
        local $errstr = "data_source should be begin with 'DBI:mysql:'";
        _warn_when_verbose($dbh);
        $cb->();
        _zombie_flush($dbh);
    }

    return $dbh;
}

=head2 $dbh->do($statement, [\%attr, [@bind_values,]] [$cb->($rv)])

=cut
sub do {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($dbh, $statement, $attr, @bind_values) = @_;

    _check_and_act($dbh, sub {
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
                $cb->(0);
            }
            $next_act->();
        });
    }, $cb);
}

=head2 $dbh->selectall_arrayref($statement, $cb->($ary_ref))

=cut
sub selectall_arrayref {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($dbh, $statement) = @_;

    _check_and_act($dbh, sub {
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
                $cb->($_[2]);
            }
            $next_act->();
        });
    }, $cb);
}

=head2 $dbh->selectall_hashref($statement, $key_field, $cb->($hash_ref))

=cut
sub selectall_hashref {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($dbh, $statement, @key_field) = @_;

    _check_and_act($dbh, sub {
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
    }, $cb);
}

=head2 $sth = $dbh->prepare($statement, [$cb->($sth)])

=cut
sub prepare {
    my $dbh = $_[0];

    if( $dbh->[STATEi]==AnyEvent::MySQL::ZOMBIE ) {
        return AnyEvent::MySQL::st->new_zombie_db(@_);
    }
    else {
        my $sth = AnyEvent::MySQL::st->new(@_);
        push @{$dbh->[STi]}, $sth;
        weaken($dbh->[STi][-1]);
        return $sth;
    }
}

package AnyEvent::MySQL::st;

use strict;
use warnings;

use Scalar::Util qw(weaken);

use constant {
    DBHi => 0,
    IDi => 1,
    STATEi => 2,
    TASKi => 3,
    FTi => 4,
    PARAMi => 5,
    FIELDi => 6,
};

*errstr = *AnyEvent::MySQL::errstr;
*errno = *AnyEvent::MySQL::errno;
our $errstr;
our $errno;

sub _consume_task {
    my $sth = shift;
    my $loop_sub; $loop_sub = sub {
        if( my $task = shift @{$sth->[TASKi]} ) {
            $sth->[STATEi] = AnyEvent::MySQL::BUSY;
            $task->[0]($loop_sub);
        }
        else {
            $sth->[STATEi] = AnyEvent::MySQL::IDLE;
            undef $loop_sub;
        }
    }; $loop_sub->();
}

sub _check_and_act {
    my($sth, $act, $cb) = @_;
    if( $sth->[STATEi]==AnyEvent::MySQL::IDLE ) {
        push @{$sth->[TASKi]}, [$act, $cb];
        _consume_task($sth);
    }
    elsif( $sth->[STATEi]==AnyEvent::MySQL::BUSY ) {
        push @{$sth->[TASKi]}, [$act, $cb];
    }
    elsif( $sth->[STATEi]==AnyEvent::MySQL::ZOMBIE ) {
        local $errno = 2030;
        local $errstr = 'Statement not prepared';
        $cb->();
    }
    elsif( $sth->[STATEi]==AnyEvent::MySQL::ZOMBIE_DB ) {
        local $errno = 2006;
        local $errstr = 'MySQL server has gone away';
        $cb->();
    }
    else {
        local $errno = 2000;
        local $errstr = "Unknown state: $sth->[STATEi]";
        _warn_when_verbose($sth, 2);
        $cb->();
    }
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
    $sth->[STATEi] = AnyEvent::MySQL::ZOMBIE;
    local $errno = 2030;
    local $errstr = 'Statement not prepared';

    _zombie_flush_common($sth);
}

sub _zombie_parent_flush {
    my($sth) = @_;
    $sth->[STATEi] = AnyEvent::MySQL::ZOMBIE_DB;

    _zombie_flush_common($sth);
}

=head2 $sth_zombie = AnyEvent::MySQL::st->new_zombie_db($dbh, $statement, [$cb->($sth)])

=cut
sub new_zombie_db {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($class, $dbh, $statement) = @_;
    my $sth = bless [], $class;
    $sth->[STATEi] = AnyEvent::MySQL::ZOMBIE_DB;
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
    $sth->[STATEi] = AnyEvent::MySQL::BUSY;
    $sth->[FTi] = [];

    AnyEvent::MySQL::db::_check_and_act($dbh, sub {
        my $next_act = shift;
        my $hd = $dbh->[AnyEvent::MySQL::db::HDi];
        AnyEvent::MySQL::Imp::send_packet($hd, 0, AnyEvent::MySQL::Imp::COM_STMT_PREPARE, $statement);
        AnyEvent::MySQL::Imp::recv_response($hd, prepare => 1, sub {
            $next_act->();
            if( $_[0]==AnyEvent::MySQL::Imp::RES_PREPARE ) {
                $sth->[IDi] = $_[1];
                $sth->[PARAMi] = $_[2];
                $sth->[FIELDi] = $_[3];

                $cb->($sth);
                _consume_task($sth);
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
        });
    }, $cb);

    return $sth;
}

=head2 $fth = $sth->execute(@bind_values, [$cb->($fth, $rv)])

=cut
sub execute {
    my $sth = $_[0];

    if( $sth->[STATEi]==AnyEvent::MySQL::ZOMBIE_DB ) {
        return AnyEvent::MySQL::ft->new_zombie_db(@_);
    }
    elsif( $sth->[STATEi]==AnyEvent::MySQL::ZOMBIE ) {
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
    STHi => 0,
    STATEi => 1,
    TASKi => 2,
    DATAi => 3,
    BINDi => 4,
};

sub _consume_task {
    my $fth = shift;
    my $loop_sub; $loop_sub = sub {
        if( my $task = shift @{$fth->[TASKi]} ) {
            $fth->[STATEi] = AnyEvent::MySQL::BUSY;
            $task->[0]($loop_sub);
        }
        else {
            $fth->[STATEi] = AnyEvent::MySQL::IDLE;
            undef $loop_sub;
        }
    }; $loop_sub->();
}

sub _check_and_act {
    my($fth, $act, $cb) = @_;
    if( $fth->[STATEi]==AnyEvent::MySQL::IDLE ) {
        push @{$fth->[TASKi]}, [$act, $cb];
        _consume_task($fth);
    }
    elsif( $fth->[STATEi]==AnyEvent::MySQL::BUSY ) {
        push @{$fth->[TASKi]}, [$act, $cb];
    }
    elsif( $fth->[STATEi]==AnyEvent::MySQL::ZOMBIE ) {
        local $errno = 2030;
        local $errstr = 'Statement not prepared';
        $cb->();
    }
    elsif( $fth->[STATEi]==AnyEvent::MySQL::ZOMBIE_ST ) {
        local $errno = 2030;
        local $errstr = 'Statement not prepared';
        $cb->();
    }
    elsif( $fth->[STATEi]==AnyEvent::MySQL::ZOMBIE_DB ) {
        local $errno = 2006;
        local $errstr = 'MySQL server has gone away';
        $cb->();
    }
    else {
        local $errno = 2000;
        local $errstr = "Unknown state: $fth->[STATEi]";
        _warn_when_verbose($fth, 2);
        $cb->();
    }
}

sub _zombie_flush {
    my($sth) = @_;
    $sth->[STATEi] = AnyEvent::MySQL::ZOMBIE;
    local $errno = 2030;
    local $errstr = 'Statement not prepared';

    _zombie_flush_common($sth);
}

=head2 $fth = AnyEvent::MySQL::ft->new_zombie_db($sth, @bind_values, [$cb->($fth, $rv)])

=cut
sub new_zombie_db {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($class, $sth) = @_;

    my $fth = bless [], $class;
    $fth->[STATEi] = AnyEvent::MySQL::ZOMBIE_DB;

    local $errno = 2006;
    local $errstr = 'MySQL server has gone away';
    $cb->();

    return $fth;
}

=head2 $fth = AnyEvent::MySQL::ft->new_zombie_st($sth, @bind_values, [$cb->($fth, $rv)])

=cut
sub new_zombie_st {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($class, $sth) = @_;

    my $fth = bless [], $class;
    $fth->[STATEi] = AnyEvent::MySQL::ZOMBIE_ST;

    local $errno = 2030;
    local $errstr = 'Statement not prepared';
    $cb->();

    return $fth;
}

=head2 $fth = AnyEvent::MySQL::ft->new($sth, @bind_values, [$cb->($fth, $rv)])

=cut
sub new {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($class, $sth, @bind_values) = @_;

    my $fth = bless [], $class;
    $fth->[STHi] = $sth;
    $fth->[STATEi] = AnyEvent::MySQL::BUSY;
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
