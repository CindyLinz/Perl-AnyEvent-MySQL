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

=head2 $dbh = AnyEvent::MySQL->connect($data_source, $username, [$auth, [\%attr,]] $cb->($dbh, 1))

=cut
sub connect {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&_empty_cb;
    my($class, $dsn, $username, $auth) = @_;

    my $dbh = bless {
        hd => undef,
        zombie => undef,
    }, $class;

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
            local $@ = "unix socket not implement yet";
            $cb->($dbh);
        }
        else {
            tcp_connect $param->{host}, $param->{port}, sub {
                my $fh = shift;
                if( !$fh ) {
                    local $@ = "Connect to $param->{host}:$param->{port} fail: $!";
                    $cb->($dbh);
                    return;
                }

                weaken( my $wdbh = $dbh );
                $dbh->{hd} = AnyEvent::Handle->new(
                    fh => $fh,
                    on_error => sub {
                        $wdbh->{zombie} = 1;
                    },
                );

                AnyEvent::MySQL::Imp::do_auth($dbh->{hd}, $username, $auth, $param->{database}, sub {
                    my($success, $err_num_and_msg) = @_;
                    if( $success ) {
                        $cb->($dbh, 1);
                    }
                    else {
                        local $@ = $err_num_and_msg;
                        $cb->($dbh);
                    }
                });
            };
        }
    }
    else {
        local $@ = "data_source should be begin with 'DBI:mysql:'";
        $cb->($dbh);
    }
    return $dbh;
}

# _dbh_bracket($dbh, $res_cb, $act_cb)
sub _dbh_bracket {
    if( $_[0]{zombie} ) {
        delete $_[0]{hd};
        delete $_[0]{zombie};
    }
    if( !$_[0]{hd} ) {
        local $@ = dualvar 2006, 'MySQL server has gone away';
        $_[2]($_[0]);
        return;
    }

    $_[2]->();
}

=head2 $dbh->do($statement, [\%attr, [@bind_values,]] $cb->($dbh, $rv))

=cut
sub do {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : \&_empty_cb;
    my($dbh, $statement, $attr, @bind_values) = @_;

    _dbh_bracket($dbh, $cb, sub {
        AnyEvent::MySQL::Imp::send_packet($dbh->{hd}, 0, AnyEvent::MySQL::Imp::COM_QUERY, $statement);
        AnyEvent::MySQL::Imp::recv_response($dbh->{hd}, sub {
            if( $_[0]==AnyEvent::MySQL::Imp::RES_ERROR ) {
                local $@ = dualvar $_[1], $_[3];
                $cb->($dbh);
            }
            else {
                $cb->($dbh, $_[1]);
            }
        });
    });
}

=head2 $sth = $dbh->prepare($statement, [\%attr,] $cb->($sth, 1))

=cut
sub prepare {
}

package AnyEvent::MySQL::Statement;

use strict;
use warnings;

=head2 $sth->execute(@bind_values, $cb->($sth, $rv))

=cut
sub execute {
}

=head2 $sth->bind_columns(@list_of_refs_to_vars_to_bind, $cb->($sth, 1))

=cut
sub bind_columns {
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
