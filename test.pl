#!/usr/bin/perl

use strict;
use warnings;

use AE;
use Data::Dumper;
use Devel::StackTrace;
use EV;

$EV::DIED = sub {
    print "EV::DIED: $@\n";
    print Devel::StackTrace->new->as_string;
};

use lib 'lib';
use AnyEvent::MySQL;

my $end = AE::cv;

my $dbh = AnyEvent::MySQL->connect("DBI:mysql:database=test;host=127.0.0.1;port=3306", "ptest", "pass", sub {
    my($dbh) = @_;
    if( $dbh ) {
        warn "Connect success!";
        $dbh->pre_do("set names latin1");
        $dbh->pre_do("set names utf8");
    }
    else {
        warn "Connect fail: $AnyEvent::MySQL::errstr ($AnyEvent::MySQL::errno)";
        $end->send;
    }
});

$dbh->do("select * from t1 where a<=?", {}, 15, sub {
    my $rv = shift;
    if( defined($rv) ) {
        warn "Do success: $rv";
    }
    else {
        warn "Do fail: $AnyEvent::MySQL::errstr ($AnyEvent::MySQL::errno)";
    }
    $end->send;
});

#$end->recv;
my $end2 = AE::cv;

#$dbh->prepare("update t1 set a=1 where b=1", sub {
#$dbh->prepare("select * from t1", sub {
my $sth = $dbh->prepare("select b, a aaa from t1 where a>?", sub {
#$dbh->prepare("select * from type_all", sub {
    warn "prepared!";
    $end2->send;
});

#$end2->recv;

my $end3 = AE::cv;

$sth->execute(1, sub {
    warn "executed! $_[0]";
    $end3->send($_[0]);
});

my $fth = $end3->recv;

my $end4 = AE::cv;

$fth->bind_col(2, \my $a, sub {
    warn $_[0];
});
my $fetch; $fetch = sub {
    $fth->fetch(sub {
        if( $_[0] ) {
            warn "Get! $a";
            $fetch->();
        }
        else {
            warn "Get End!";
            undef $fetch;
            $end4->send;
        }
    });
}; $fetch->();

#$fth->bind_columns(\my($a, $b), sub {
#    warn $_[0];
#    warn $AnyEvent::MySQL::errstr;
#});
#my $fetch; $fetch = sub {
#    $fth->fetch(sub {
#        if( $_[0] ) {
#            warn "Get! ($a, $b)";
#            $fetch->();
#        }
#        else {
#            undef $fetch;
#            $end4->send;
#        }
#    });
#}; $fetch->();

#my $fetch; $fetch = sub {
#    $fth->fetchrow_array(sub {
#        if( @_ ) {
#            warn "Get! (@_)";
#            $fetch->();
#        }
#        else {
#            undef $fetch;
#            $end4->send;
#        }
#    });
#}; $fetch->();

#my $fetch; $fetch = sub {
#    $fth->fetchrow_arrayref(sub {
#        if( $_[0] ) {
#            warn "Get! (@{$_[0]})";
#            $fetch->();
#        }
#        else {
#            undef $fetch;
#            $end4->send;
#        }
#    });
#}; $fetch->();

#my $fetch; $fetch = sub {
#    $fth->fetchrow_hashref(sub {
#        if( $_[0] ) {
#            warn "Get! (@{[%{$_[0]}]})";
#            $fetch->();
#        }
#        else {
#            undef $fetch;
#            $end4->send;
#        }
#    });
#}; $fetch->();

$end4->recv;

#tcp_connect 0, 3306, sub {
#    my $fh = shift;
#    my $hd = AnyEvent::Handle->new( fh => $fh );
#    AnyEvent::MySQL::Imp::do_auth($hd, 'tiwi', '', sub {
#        undef $hd;
#        warn $_[0];
#        $end->send;
#    });
#};

my $end5 = AE::cv;

$dbh->selectall_arrayref("select a*2, b from t1 where a<=?", {}, 15, sub {
    warn "selectall_arrayref";
    warn Dumper($_[0]);
});

$dbh->selectall_hashref("select a*2, b from t1", 'b', sub {
    warn "selectall_hashref";
    warn Dumper($_[0]);
});

$dbh->selectall_hashref("select a*2, b from t1", ['b', 'a*2'], sub {
    warn "selectall_hashref";
    warn Dumper($_[0]);
});

$dbh->selectall_hashref("select a*2, b from t1", sub {
    warn "selectall_hashref";
    warn Dumper($_[0]);
});

$dbh->selectcol_arrayref("select a*2, b from t1", { Columns => [1,2,1] }, sub {
    warn "selectcol_arrayref";
    warn Dumper($_[0]);
});

$dbh->selectall_arrayref("select * from t3", sub {
    warn "selectall_arrayref t3";
    warn Dumper($_[0]);
});

$dbh->selectrow_array("select * from t1 where a>? order by a", {}, 2, sub {
    warn "selectrow_array";
    warn Dumper(\@_);
});

$dbh->selectrow_arrayref("select * from t1 where a>? order by a", {}, 2, sub {
    warn "selectrow_arrayref";
    warn Dumper($_[0]);
});

$dbh->selectrow_hashref("select * from t1 where a>? order by a", {}, 2, sub {
    warn "selectrow_hashref";
    warn Dumper($_[0]);
});

my $st = $dbh->prepare("select * from t1 where a>? order by a");

$st->execute(2, sub {
    warn "fetchall_arrayref";
    warn Dumper($_[0]->fetchall_arrayref());
});

$st->execute(2, sub {
    warn "fetchall_hashref(a)";
    warn Dumper($_[0]->fetchall_hashref('a'));
});

$st->execute(2, sub {
    warn "fetchall_hashref";
    warn Dumper($_[0]->fetchall_hashref());

    $end5->send;
});

#my $txh = $dbh->begin_work(sub {
#    warn "txn begin.. @_";
#});
#
#$dbh->do("insert into t1 values (50,50)", { Tx => $txh }, sub {
#    warn "insert in txn @_ insertid=".$dbh->last_insert_id;
#});
#
#$txh->rollback(sub {
#    warn "rollback txn @_";
#});
#
#$dbh->selectall_arrayref("select * from t1", sub {
#    warn "check rollback txn: ".Dumper($_[0]);
#});
#
#my $txh2 = $dbh->begin_work(sub {
#    warn "txn2 begin.. @_";
#});
#
#$dbh->do("insert into t1 values (50,50)", { Tx => $txh2 }, sub {
#    warn "insert in txn2 @_ insertid=".$dbh->last_insert_id;
#});
#
#$txh2->commit(sub {
#    warn "commit txn2 @_";
#});
#
#$dbh->selectall_arrayref("select * from t1", sub {
#    warn "check commit txn: ".Dumper($_[0]);
#});
#
#$dbh->do("delete from t1 where a=50", sub {
#    warn "remove the effect @_";
#});
#
#my $update_st;
#
#my $txh3; $txh3 = $dbh->begin_work(sub {
#    warn "txn3 begin.. @_";
#});
#
#    $update_st = $dbh->prepare("insert into t1 values (?,?)", sub {
#        warn "prepare insert @_";
#    });
#    $update_st->execute(60, 60, { Tx => $txh3 }, sub {
#        warn "insert 60 @_";
#    });
#
#    $dbh->selectall_arrayref("select * from t1", { Tx => $txh3 }, sub {
#        warn "select in txn3: ".Dumper($_[0]);
#    });
#
#    $txh3->rollback(sub {
#        warn "txh3 rollback @_";
#    });
#
#    $dbh->selectall_arrayref("select * from t1", sub {
#        warn "select out txn3: ".Dumper($_[0]);
#    });

#$st_all = $dbh->prepare("select `date`, `time`, `datetime`, `timestamp` from all_type", sub {
#    warn "prepare st_all @_";
#});
#
#$st_all->execute

$end5->recv;

$end->recv;
