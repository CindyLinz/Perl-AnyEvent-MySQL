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

sub _empty_cb {}

use constant {
    TASK => 1,
};

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

use constant {
    BUSY => 0,
    IDLE => 1,
    ZOMBIE => 2,
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

sub _check_and_act {
    my($dbh, $act, $cb) = @_;
    if( $dbh->[STATEi]==ZOMBIE ) {
        local $errno = 2006;
        local $errstr = 'MySQL server has gone away';
        $cb->();
    }
    elsif( $dbh->[STATEi]==BUSY ) {
        push @{$dbh->[TASKi]}, [$act, $cb];
    }
    elsif( $dbh->[STATEi]==IDLE ) {
        $act->();
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
            $dbh->[STATEi] = ZOMBIE;
            _warn_when_verbose($dbh);
            $cb->();
        }
        else {
            tcp_connect($param->{host}, $param->{port}, sub {
                my $fh = shift;
                if( !$fh ) {
                    local $errno = 2003;
                    local $errstr = "Connect to $param->{host}:$param->{port} fail: $!";
                    $dbh->[STATEi] = ZOMBIE;
                    _warn_when_verbose($dbh);
                    $cb->();
                    return;
                }

                weaken( my $wdbh = $dbh );
                $dbh->[HDi] = AnyEvent::Handle->new(
                    fh => $fh,
                    on_error => sub {
                        $wdbh->[STATEi] = ZOMBIE;
                    },
                );

                AnyEvent::MySQL::Imp::do_auth($dbh->[HDi], $username, $auth, $param->{database}, sub {
                    my($success, $err_num_and_msg) = @_;
                    if( $success ) {
                        $dbh->[STATEi] = IDLE;

                        my $tasks = $dbh->[TASKi];
                        $dbh->[TASKi] = [];
                        for my $task ( @$tasks ) {
                            $task->[0]();
                        }
                        if( $dbh->[ATTRi]{on_connect} ) {
                            $dbh->[ATTRi]{on_connect}($dbh, sub { $cb->($dbh) });
                        }
                        else {
                            $cb->($dbh);
                        }
                    }
                    else {
                        local $errno = 2012;
                        local $errstr = $err_num_and_msg;
                        $dbh->[STATEi] = ZOMBIE;
                        _warn_when_verbose($dbh);
                        $cb->();
                    }
                });
            });
        }
    }
    else {
        local $errno = 2054;
        local $errstr = "data_source should be begin with 'DBI:mysql:'";
        $dbh->[STATEi] = ZOMBIE;
        _warn_when_verbose($dbh);
        $cb->();
    }

    return $dbh;
}

=head2 $dbh->do($statement, [\%attr, [@bind_values,]] [$cb->($rv)])

=cut
sub do {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($dbh, $statement, $attr, @bind_values) = @_;

    _check_and_act($dbh, sub {
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
        });
    }, $cb);
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

use constant {
    PREPARING => 0,
    PREPARED => 1,
    ZOMBIE => 2,
    ZOMBIE_DB => 3,
};

*errstr = *AnyEvent::MySQL::errstr;
*errno = *AnyEvent::MySQL::errno;
our $errstr;
our $errno;

sub _check_and_act {
    my($sth, $act, $cb) = @_;
    if( $sth->[STATEi]==PREPARED ) {
        $act->();
    }
    elsif( $sth->[STATEi]==PREPARING ) {
        push @{$sth->[TASKi]}, [$act, $cb];
    }
    elsif( $sth->[STATEi]==ZOMBIE ) {
        local $errno = 2030;
        local $errstr = 'Statement not prepared';
        $cb->();
    }
    elsif( $sth->[STATEi]==ZOMBIE_DB ) {
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
    $sth->[STATEi] = ZOMBIE;
    local $errno = 2030;
    local $errstr = 'Statement not prepared';

    _zombie_flush_common($sth);
}

sub _zombie_parent_flush {
    my($sth) = @_;
    $sth->[STATEi] = ZOMBIE_DB;

    _zombie_flush_common($sth);
}

=head2 $sth_zombie = AnyEvent::MySQL::st->new_zombie_db($dbh, $statement, [$cb->($sth)])

=cut
sub new_zombie_db {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($class, $dbh, $statement) = @_;
    my $sth = bless [], $class;
    $sth->[STATEi] = ZOMBIE_DB;
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
    $sth->[STATEi] = PREPARING;
    $sth->[FTi] = [];

    AnyEvent::MySQL::db::_check_and_act($dbh, sub {
        my $hd = $dbh->[AnyEvent::MySQL::db::HDi];
        AnyEvent::MySQL::Imp::send_packet($hd, 0, AnyEvent::MySQL::Imp::COM_STMT_PREPARE, $statement);
        AnyEvent::MySQL::Imp::recv_response($hd, prepare => 1, sub {
            if( $_[0]==AnyEvent::MySQL::Imp::RES_PREPARE ) {
                $sth->[STATEi] = PREPARED;
                $sth->[IDi] = $_[1];
                $sth->[PARAMi] = $_[3];
                $sth->[FIELDi] = $_[2];

                my $tasks = $sth->[TASKi];
                $sth->[TASKi] = [];
                for my $task (@$tasks) {
                    $task->[0]();
                }
                $cb->($sth);
            }
            else {
                $sth->[STATEi] = ZOMBIE;
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
            }
        });
    }, $cb);

    return $sth;
}

=head2 $fth = $sth->execute(@bind_values, [$cb->($fth, $rv)])

=cut
sub execute {
    my $sth = $_[0];

    if( $sth->[STATEi]==ZOMBIE_DB ) {
        return AnyEvent::MySQL::ft->new_zombie_db(@_);
    }
    elsif( $sth->[STATEi]==ZOMBIE ) {
        return AnyEvent::MySQL::ft->new_zombie_st(@_);
    }
    else {
        my $fth = AnyEvent::MySQL::ft->new(@_);
        push @{$sth->[FTi]}, $fth;
        weaken($sth->[FTi][-1]);
        return $fth;
    }
}

=head2 $sth->bind_columns(@list_of_refs_to_vars_to_bind, $cb->($rc))

=cut
sub bind_columns {
}

package AnyEvent::MySQL::ft;

use strict;
use warnings;

use constant {
    STHi => 0,
    STATEi => 1,
    TASKi => 2,
    DATAi => 3,
};

use constant {
    EXECUTING => 0,
    EXECUTED => 1,
    ZOMBIE => 2,
    ZOMBIE_DB => 3,
    ZOMBIE_ST => 4,
};

=head2 $fth = AnyEvent::MySQL::ft->new_zombie_db($sth, @bind_values, [$cb->($fth, $rv)])

=cut
sub new_zombie_db {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&AnyEvent::MySQL::_empty_cb;
    my($class, $sth) = @_;

    my $fth = bless [], $class;
    $fth->[STATEi] = ZOMBIE_DB;

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
    $fth->[STATEi] = ZOMBIE_ST;

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
    $fth->[STATEi] = EXECUTING;

    AnyEvent::MySQL::st::_check_and_act($sth, sub {
        warn;
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
            $fth->[DATAi] = $_[2];
            $cb->($fth);
        });
    }, $cb);

    return $fth;
}

=head2 $sth->fetch($cb->($sth, $rv))

=cut
sub fetch {
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
