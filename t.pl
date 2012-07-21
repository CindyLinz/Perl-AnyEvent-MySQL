#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use DBD::mysql;
use Data::Dumper;

my $dbh = DBI->connect("DBI:mysql:database=test;host=127.0.0.1", "test", "");

#my $st = $dbh->prepare("update t1 set a=a");
#$st->execute();
#my $res = $st->fetchall_arrayref;
##my $res = $dbh->selectall_arrayref("update t1 set a=a");
#print Dumper($res);

my $rv = $dbh->do("select * from t1");
print Dumper($rv);

#for(my $i=0; $i<1000; ++$i) {
#    my @array = (5) x 10000;
#    while( @array ) {
#        shift @array;
#    }
#}
#my @t = times;
#print "@t\n";

#/^([\x00-\x7F\xC0-\xFF][\x80-\xBF]*){6}$/

