#!/usr/bin/perl

use strict;
use warnings;

use AE;
use Devel::StackTrace;
use EV;
use AnyEvent::Handle;
use Devel::Caller qw(caller_cv);
use Data::Dumper;

use lib 'lib';
use AnyEvent::MySQL;

$EV::DIED = sub {
    print "EV::DIED: $@\n";
    print Devel::StackTrace->new->as_string;
};

if( @ARGV!=5 ) {
    print "usage: $0 host port db user password\n";
    exit 1;
}

my($host, $port, $db, $user, $password) = @ARGV;
my $dbh = AnyEvent::MySQL->connect("DBI:mysql:database=$db;host=$host;port=$port", $user, $password, sub {
    my($dbh) = @_;
    if( $dbh ) {
        warn "Connect success!";
    }
    else {
        warn "Connect fail: @{[$dbh->errstr]} (@{[$dbh->errno]})";
        exit;
    }
});

my $stdin = AnyEvent::Handle->new( fh => \*STDIN );
my $stdout = AnyEvent::Handle->new( fh => \*STDOUT );

$stdin->push_read( line => sub {
    $_[0]->push_read( line => caller_cv(0) );
    my $line = $_[1];
    my($func, $sql) = $line =~ /(\S+)\s*(.*)/;
    $dbh->$func( $sql ? $sql : (), sub {
        $stdout->push_write("$func($sql) -> ".Dumper($_[0]));
        if( !defined($_[0]) ) {
            $stdout->push_write("err   =".$dbh->err.$/);
            $stdout->push_write("errstr=".$dbh->errstr.$/);
        }
    });
} );

AE::cv->recv;
