#!/usr/bin/perl

use AE;
use AnyEvent::Socket;
use AnyEvent::Handle;

use lib 'lib';
use AnyEvent::MySQL;

my $end = AE::cv;

my $dbh = AnyEvent::MySQL->connect("DBI:mysql:database=test;host=127.0.0.1;port=3306", "ptest", "pass", sub {
    my($dbh, $res) = @_;
    if( $res ) {
        warn "Connect success!";
        $dbh->do("select * from t1", sub {
            if( $#_ ) {
                warn "Do success: $_[1]";
            }
            else {
                warn "Do fail: $@ (@{[0+$@]})";
            }
            $end->send;
        });
    }
    else {
        warn "Connect fail: $@ (@{[0+$@]})";
        $end->send;
    }
});

#tcp_connect 0, 3306, sub {
#    my $fh = shift;
#    my $hd = AnyEvent::Handle->new( fh => $fh );
#    AnyEvent::MySQL::Imp::do_auth($hd, 'tiwi', '', sub {
#        undef $hd;
#        warn $_[0];
#        $end->send;
#    });
#};

$end->recv;
