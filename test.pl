#!/usr/bin/perl

use AE;
use AnyEvent::Socket;
use AnyEvent::Handle;
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
    }
    else {
        warn "Connect fail: $AnyEvent::MySQL::errstr ($AnyEvent::MySQL::errno)";
        $end->send;
    }
});

$dbh->do("select * from t1", sub {
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

my $fth = $sth->execute(1, sub {
    warn "executed!";
    $end3->send;
});

#$end3->recv;

my $end4 = AE::cv;

$fth->bind_col(2, \my $a, sub {
    warn $_[0];
    warn $AnyEvent::MySQL::errstr;
});
my $fetch; $fetch = sub {
    $fth->fetch(sub {
        if( $_[0] ) {
            warn "Get! $a";
            $fetch->();
        }
        else {
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

$dbh->selectall_arrayref("select a*2, b from t1", sub {
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

my $txh = $dbh->begin_work(sub {
    warn "txn begin.. @_";
});

$dbh->do("insert into t1 values (50,50)", { Tx => $txh }, sub {
    warn "insert in txn @_";
});

$txh->rollback(sub {
    warn "rollback txn @_";
});

$dbh->selectall_arrayref("select * from t1", sub {
    warn "check rollback txn: ".Dumper($_[0]);
});

my $txh2 = $dbh->begin_work(sub {
    warn "txn2 begin.. @_";
});

$dbh->do("insert into t1 values (50,50)", { Tx => $txh2 }, sub {
    warn "insert in txn2 @_";
});

$txh2->commit(sub {
    warn "commit txn2 @_";
});

$dbh->selectall_arrayref("select * from t1", sub {
    warn "check commit txn: ".Dumper($_[0]);
});

$dbh->do("delete from t1 where a=50", sub {
    warn "remove the effect @_";
    $end5->send;
});

$end5->recv;

$end->recv;
