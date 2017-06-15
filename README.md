# NAME

AnyEvent::MySQL - Pure Perl AnyEvent socket implementation of MySQL client

# VERSION

Version 1.2.1

# SYNOPSIS

This package is used in my company since 2012 to today (2017). I think it should be stable.
(though some data type fetching through prepared command are not implemented)

Please read the test.pl file as a usage example. >w<

    #!/usr/bin/perl

    use strict;
    use warnings;

    BEGIN {
        eval {
            require AE;
            require Data::Dumper;
            require Devel::StackTrace;
            require EV;
        };
        if( $@ ) {
            warn "require module fail: $@";
            exit;
        }
    }

    $EV::DIED = sub {
        print "EV::DIED: $@\n";
        print Devel::StackTrace->new->as_string;
    };

    use lib 'lib';
    use AnyEvent::MySQL;

    my $end = AE::cv;

    my $dbh = AnyEvent::MySQL->connect("DBI:mysql:database=test;host=127.0.0.1;port=3306", "ptest", "pass", { PrintError => 1 }, sub {
        my($dbh) = @_;
        if( $dbh ) {
            warn "Connect success!";
            $dbh->pre_do("set names latin1");
            $dbh->pre_do("set names utf8");
        }
        else {
            warn "Connect fail: $AnyEvent::MySQL::errstr ($AnyEvent::MySQL::err)";
            $end->send;
        }
    });

    $dbh->do("select * from t1 where a<=?", {}, 15, sub {
        my $rv = shift;
        if( defined($rv) ) {
            warn "Do success: $rv";
        }
        else {
            warn "Do fail: $AnyEvent::MySQL::errstr ($AnyEvent::MySQL::err)";
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
    });

    $st->execute(2, sub {
        warn "fetchcol_arrayref";
        warn Dumper($_[0]->fetchcol_arrayref());
    });

    $dbh->begin_work( sub {
        warn "txn begin.. @_ | $AnyEvent::MySQL::errstr ($AnyEvent::MySQL::err)";
    } );

    $dbh->do("update t1 set a=? b=?", {}, 3, 4, sub {
        warn "error update @_ | $AnyEvent::MySQL::errstr ($AnyEvent::MySQL::err)";
    } );

    $dbh->do("update t1 set b=b+1", {}, sub {
        warn "after error update @_ | $AnyEvent::MySQL::errstr ($AnyEvent::MySQL::err)";
    } );

    $dbh->commit( sub {
        warn "aborted commit @_ | $AnyEvent::MySQL::errstr ($AnyEvent::MySQL::err)";
    } );

    $dbh->do("update t1 set b=b+1", {}, sub {
        warn "after aborted commit @_ | $AnyEvent::MySQL::errstr ($AnyEvent::MySQL::err)";
        $end5->send;
    } );

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

    my $readonly_dbh = AnyEvent::MySQL->connect("DBI:mysql:database=test;host=127.0.0.1;port=3306", "ptest", "pass", { ReadOnly => 1 }, sub {
      # ... we can only use "select" and "show" and "set names" command on this handle
    });

    $end->recv;

## $dbh = AnyEvent::MySQL->connect($data\_source, $username, \[$auth, \[\\%attr,\]\] $cb->($dbh, 1))

## $dbh = AnyEvent::MySQL::db->new($dsn, $username, \[$auth, \[\\%attr,\]\] \[$cb->($dbh, $next\_guard)\])

    $cb will be called when each time the db connection is connected, reconnected,
    or tried but failed.

    If failed, the $dbh in the $cb's args will be undef.

    You can do some connection initialization here, such as
     set names utf8;

    But you should NOT rely on this for work flow control,
    cause the reconnection can occur anytime.

## $error\_num = $dbh->err

## $error\_str = $dbh->errstr

## $rv = $dbh->last\_insert\_id

    Non-blocking get the value immediately

## $dbh->do($statement, \[\\%attr, \[@bind\_values,\]\] \[$cb->($rv)\])

## $dbh->pre\_do($statement, \[\\%attr, \[@bind\_values,\]\] \[$cb->($rv)\])

    This method is like $dbh->do except that $dbh->pre_do will unshift
    job into the queue instead of push.

    This method is for the initializing actions in the AnyEvent::MySQL->connect's callback

## $dbh->selectall\_arrayref($statement, \[\\%attr, \[@bind\_values,\]\] $cb->($ary\_ref))

## $dbh->selectall\_hashref($statement, \[$key\_field|\\@key\_field\], \[\\%attr, \[@bind\_values,\]\] $cb->($hash\_ref))

## $dbh->selectcol\_arrayref($statement, \[\\%attr, \[@bind\_values,\]\] $cb->($ary\_ref))

## $dbh->selectrow\_array($statement, \[\\%attr, \[@bind\_values,\]\], $cb->(@row\_ary))

## $dbh->selectrow\_arrayref($statement, \[\\%attr, \[@bind\_values,\]\], $cb->($ary\_ref))

## $dbh->selectrow\_hashref($statement, \[\\%attr, \[@bind\_values,\]\], $cb->($hash\_ref))

## $sth = $dbh->prepare($statement, \[$cb->($sth)\])

    $cb will be called each time when this statement is prepared
    (or re-prepared when the db connection is reconnected)

    if the preparation is not success,
    the $sth in the $cb's arg will be undef.

    So you should NOT rely on this for work flow controlling.

## $dbh->begin\_work(\[$cb->($rv)\])

## $dbh->commit(\[$cb->($rv)\])

## $dbh->rollback(\[$cb->($rv)\])

## $dbh->ping(sub {my $alive = shift;});

## $sth = AnyEvent::MySQL::st->new($dbh, $statement, \[$cb->($sth)\])

## $sth->execute(@bind\_values, \[\\%attr,\] \[$cb->($fth/$rv)\])

## $fth = AnyEvent::MySQL::ft->new(\\@data\_set)

## $rc = $fth->bind\_columns(@list\_of\_refs\_to\_vars\_to\_bind, \[$cb->($rc)\])

## $rc = $fth->bind\_col($col\_num, \\$col\_variable, \[$cb->($rc)\])

## $rv = $fth->fetch(\[$cb->($rv)\])

## @row\_ary = $fth->fetchrow\_array(\[$cb->(@row\_ary)\])

## $ary\_ref = $fth->fetchrow\_arrayref(\[$cb->($ary\_ref)\])

## $hash\_ref = $fth->fetchrow\_hashref(\[$cb->($hash\_ref)\])

## $ary\_ref = $fth->fetchall\_arrayref(\[$cb->($ary\_ref)\])

## $hash\_ref = $fth->fetchall\_hashref(\[($key\_field|\\@key\_field),\] \[$cb->($hash\_ref)\])

## $ary\_ref = $fth->fetchcol\_arrayref(\[\\%attr\], \[$cb->($ary\_ref)\])

# AUTHOR

Cindy Wang (CindyLinz)

# CONTRIBUTOR

Dmitriy Shamatrin [justnoxx@github](https://github.com/justnoxx)

clking [clking@github](https://github.com/clking)

# BUGS

Please report any bugs or feature requests to `http://github.com/CindyLinz/Perl-AnyEvent-MySQL`.
I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

# SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc AnyEvent::MySQL

You can also look for information at:

- github

    [https://github.com/CindyLinz/Perl-AnyEvent-MySQL](https://github.com/CindyLinz/Perl-AnyEvent-MySQL)

- Search CPAN

    [http://search.cpan.org/dist/AnyEvent-MySQL/](http://search.cpan.org/dist/AnyEvent-MySQL/)

# LICENSE AND COPYRIGHT

Copyright 2011-2015 Cindy Wang (CindyLinz).

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.
