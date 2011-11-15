package AnyEvent::MySQL::Imp;

use strict;
use warnings;

use AE;
use AnyEvent::Socket;
use AnyEvent::Handle;
use Digest::SHA1 qw(sha1);
use List::Util qw(reduce);
use Scalar::Util qw(dualvar);
use Combinator;

use constant {
    DEV => 1,
};

use constant {
    CLIENT_LONG_PASSWORD      =>      1, # new more secure passwords +
    CLIENT_FOUND_ROWS         =>      2, # Found instead of affected rows *
    CLIENT_LONG_FLAG          =>      4, # Get all column flags * +
    CLIENT_CONNECT_WITH_DB    =>      8, # One can specify db on connect +
    CLIENT_NO_SCHEMA          =>     16, # Don't allow database.table.column
    CLIENT_COMPRESS           =>     32, # Can use compression protocol *
    CLIENT_ODBC               =>     64, # Odbc client
    CLIENT_LOCAL_FILES        =>    128, # Can use LOAD DATA LOCAL *
    CLIENT_IGNORE_SPACE       =>    256, # Ignore spaces before '(' *
    CLIENT_PROTOCOL_41        =>    512, # New 4.1 protocol +
    CLIENT_INTERACTIVE        =>   1024, # This is an interactive client * +
    CLIENT_SSL                =>   2048, # Switch to SSL after handshake *
    CLIENT_IGNORE_SIGPIPE     =>   4096, # IGNORE sigpipes
    CLIENT_TRANSACTIONS       =>   8192, # Client knows about transactions +
    CLIENT_RESERVED           =>  16384, # Old flag for 4.1 protocol 
    CLIENT_SECURE_CONNECTION  =>  32768, # New 4.1 authentication * +
    CLIENT_MULTI_STATEMENTS   =>  65536, # Enable/disable multi-stmt support * +
    CLIENT_MULTI_RESULTS      => 131072, # Enable/disable multi-results * +
};

use constant {
    COM_SLEEP               => "\x00", #   (none, this is an internal thread state)
    COM_QUIT                => "\x01", #   mysql_close
    COM_INIT_DB             => "\x02", #   mysql_select_db 
    COM_QUERY               => "\x03", #   mysql_real_query
    COM_FIELD_LIST          => "\x04", #   mysql_list_fields
    COM_CREATE_DB           => "\x05", #   mysql_create_db (deprecated)
    COM_DROP_DB             => "\x06", #   mysql_drop_db (deprecated)
    COM_REFRESH             => "\x07", #   mysql_refresh
    COM_SHUTDOWN            => "\x08", #   mysql_shutdown
    COM_STATISTICS          => "\x09", #   mysql_stat
    COM_PROCESS_INFO        => "\x0a", #   mysql_list_processes
    COM_CONNECT             => "\x0b", #   (none, this is an internal thread state)
    COM_PROCESS_KILL        => "\x0c", #   mysql_kill
    COM_DEBUG               => "\x0d", #   mysql_dump_debug_info
    COM_PING                => "\x0e", #   mysql_ping
    COM_TIME                => "\x0f", #   (none, this is an internal thread state)
    COM_DELAYED_INSERT      => "\x10", #   (none, this is an internal thread state)
    COM_CHANGE_USER         => "\x11", #   mysql_change_user
    COM_BINLOG_DUMP         => "\x12", #   sent by the slave IO thread to request a binlog
    COM_TABLE_DUMP          => "\x13", #   LOAD TABLE ... FROM MASTER (deprecated)
    COM_CONNECT_OUT         => "\x14", #   (none, this is an internal thread state)
    COM_REGISTER_SLAVE      => "\x15", #   sent by the slave to register with the master (optional)
    COM_STMT_PREPARE        => "\x16", #   mysql_stmt_prepare
    COM_STMT_EXECUTE        => "\x17", #   mysql_stmt_execute
    COM_STMT_SEND_LONG_DATA => "\x18", #   mysql_stmt_send_long_data
    COM_STMT_CLOSE          => "\x19", #   mysql_stmt_close
    COM_STMT_RESET          => "\x1a", #   mysql_stmt_reset
    COM_SET_OPTION          => "\x1b", #   mysql_set_server_option
    COM_STMT_FETCH          => "\x1c", #   mysql_stmt_fetch
};

use constant {
    RES_OK => 0,
    RES_ERROR => 255,
    RES_RESULT => 1,
};

# $str = take_zstring($data(modified)) - null terminated string
sub take_zstr {
    $_[0] =~ s/(.*?)\x00//s;
    return $1;
}

# $num = take_lcb($data(modifed)) - length coded binary
sub take_lcb {
    my $fb = substr($_[0], 0, 1, '');
    if( $fb le "\xFA" ) { # 0-250
        return ord($fb);
    }
    if( $fb eq "\xFB" ) { # 251
        return undef;
    }
    if( $fb eq "\xFC" ) { # 252
        return unpack('v', substr($_[0], 0, 2, ''));
    }
    if( $fb eq "\xFD" ) { # 253
        return unpack('V', substr($_[0], 0, 3, '')."\x00");
    }
    if( $fb eq "\xFE" ) { # 254
        return unpack('Q<', substr($_[0], 0, 8, ''));
    }
    return undef; # error
}

# $str = take_lcs($data(modified)) - length coded string
sub take_lcs {
    my $len = &take_lcb;
    if( defined $len ) {
        return substr($_[0], 0, $len, '');
    }
    else {
        return undef;
    }
}

# $num = take_num($data(modified), $len)
sub take_num {
    return unpack('V', substr($_[0], 0, $_[1], '')."\x00\x00\x00");
}

# $str = take_str($data(modified), $len)
sub take_str {
    return substr($_[0], 0, $_[1], '');
}

# () = take_filler($data(modified), $len)
sub take_filler {
    substr($_[0], 0, $_[1], '');
    return ();
}

# put_num($data(modified), $num, $len)
sub put_num {
    $_[0] .= substr(pack('V', $_[1]), 0, $_[2]);
}

# put_str($data(modified), $str, $len)
sub put_str {
    $_[0] .= substr($_[1].("\x00" x $_[2]), 0, $_[2]);
}

# put_zstr($data(modified), $str)
sub put_zstr {
    no warnings 'uninitialized';
    $_[0] .= $_[1];
    $_[0] .= "\x00";
}

# put_lcb($data(modified), $num)
sub put_lcb {
    if( $_[1] <= 250 ) {
        $_[0] .= chr($_[1]);
    }
    elsif( !defined($_[1]) ) {
        $_[0] .= "\xFB"; # 251
    }
    elsif( $_[1] <= 65535 ) {
        $_[0] .= "\xFC"; # 252
        $_[0] .= pack('v', $_[1]);
    }
    elsif( $_[1] <= 16777215 ) {
        $_[0] .= "\xFD"; # 253
        $_[0] .= substr(pack('V', $_[1]), 0, 3);
    }
    else {
        $_[0] .= "\xFE"; # 254
        $_[0] .= pack('Q<', $_[1]);
    }
}

# put_lcs($data(modified), $str)
sub put_lcs {
    put_lcb($_[0], length($_[1]));
    $_[0] .= $_[1];
}

# ($affected_rows, $insert_id, $server_status, $warning_count, $message) | $is = parse_ok($data(modified))
sub parse_ok {
    if( substr($_[0], 0, 1) eq "\x00" ) {
        if( wantarray ) {
            substr($_[0], 0, 1, '');
            return (
                take_lcb($_[0]),
                take_lcb($_[0]),
                take_num($_[0], 2),
                take_num($_[0], 2),
                $_[0],
            );
        }
        else {
            return 1;
        }
    }
    else {
        return;
    }
}

# ($errno, $sqlstate, $message) = parse_error($data(modified))
sub parse_error {
    if( substr($_[0], 0, 1) eq "\xFF" ) {
        if( wantarray ) {
            substr($_[0], 0, 1, '');
            return (
                take_num($_[0], 2),
                ( substr($_[0], 0, 1) eq '#' ?
                  ( substr($_[0], 1, 5), substr($_[0], 6) ) :
                  ( '', $_[0] )
                )
            );
        }
        else {
            return 1;
        }
    }
    else {
        return;
    }
}

## ($field_count, $extra) = parse_result_set_header($data(modified))
#sub parse_result_set_header {
#    if( $substr($_[0], 0, 1) 
#}

# recv_packet($hd, $cb->($packet))
sub recv_packet {
    my $cb = pop;
    my($hd) = @_;
    $hd->push_read( chunk => 4, sub {
        my $len = unpack("V", $_[1]);
        my $num = $len >> 24;
        $len &= 0xFFFFFF;
        print "pack_len=$len, pack_num=$num\n" if DEV;
        $hd->unshift_read( chunk => $len, sub {
            $cb->($_[1]);
        } );
    } );
}

# skip_until_eof($hd, $cb->())
sub skip_until_eof {
    my($hd, $cb) = @_;
    recv_packet($hd, sub {
        if( substr($_[0], 0, 1) eq "\xFE" ) {
            $cb->();
        }
        else {
            skip_until_eof($hd, $cb);
        }
    });
}

# send_packet($hd, $packet_num, $packet_frag1, $pack_frag2, ...)
sub send_packet {
    my $len = reduce { $a + length($b) } 0, @_[2..$#_];
    $_[0]->push_write(substr(pack('V', $len), 0, 3));
    $_[0]->push_write(chr($_[1]));
    $_[0]->push_write($_) for @_[2..$#_];
#    $_[0]->push_write(substr(pack('V', $len), 0, 3).chr($_[1]).join('',@_[2..$#_]));
}

# recv_response($hd, [$binary_format,] $cb->(TYPE, data...))
#  RES_OK, $affected_rows, $insert_id, $server_status, $warning_count, $message
#  RES_ERROR, $errno, $sqlstate, $message
#  RES_RESULT, \@fields, \@rows
#   $fields[$i] = [$catalog, $db, $table, $org_table, $name, $org_name, $charsetnr, $length, $type, $flags, $decimals, $default]
#   $rows[$i] = [$field, $field, $field, ...]
sub recv_response {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : sub {};
    my($hd, $binary_format) = @_;

    if( DEV ) {
        my $cb0 = $cb;
        $cb = sub {
            use Data::Dumper;
            warn "recv_response: ".Dumper(\@_);
            &$cb0;
        };
    }

    warn;
    recv_packet($hd, sub {
        warn;
        my $head = substr($_[0], 0, 1);
        warn "packet_head=".ord($head);
        if( $head eq "\x00" ) { # OK
            substr($_[0], 0, 1, '');
            $cb->(
                RES_OK,
                take_lcb($_[0]),
                take_lcb($_[0]),
                take_num($_[0], 2),
                take_num($_[0], 2),
                $_[0],
            );
        }
        elsif( $head eq "\xFF" ) { # Error
            substr($_[0], 0, 1, '');
            $cb->(
                RES_ERROR,
                take_num($_[0], 2),
                ( substr($_[0], 0, 1) eq '#' ?
                  ( substr($_[0], 1, 5), substr($_[0], 6) ) : # ver 4.1
                  ( undef, $_[0] )                            # ver 4.0
                )
            );
        }
        else { # Others (EOF shouldn't be here)
            my $field_count = take_lcb($_[0]);
            my $extra = $_[0] eq '' ? undef : take_lcb($_[0]);

            warn "field_count=$field_count";

            my @field;
            for(my $i=0; $i<$field_count; ++$i) {
                warn "get field.";
                recv_packet($hd, sub {
                    warn "got field!";
                    push @field, [
                        take_lcs($_[0]), take_lcs($_[0]), take_lcs($_[0]),
                        take_lcs($_[0]), take_lcs($_[0]), take_lcs($_[0]),
                        take_filler($_[0], 1),
                        take_num($_[0], 2),
                        take_num($_[0], 4),
                        take_num($_[0], 1),
                        take_num($_[0], 2),
                        take_num($_[0], 1),
                        take_filler($_[0], 2),
                        take_lcb($_[0]),
                    ];
                });
            }
            recv_packet($hd, sub{ warn "got EOF" }); # EOF

            my @row;
            my $fetch_row; $fetch_row = sub { # text format
                warn "get row.";
                recv_packet($hd, sub {
                    warn "got row!";
                    if( substr($_[0], 0, 1) eq "\xFE" ) { # EOF
                        undef $fetch_row;
                        $cb->(
                            RES_RESULT,
                            \@field,
                            \@row,
                        );
                    }
                    else {
                        my @cell;
                        for(my $i=0; $i<$field_count; ++$i) {
                            push @cell, take_lcs($_[0]);
                        }
                        push @row, \@cell;
                        $fetch_row->();
                    }
                });
            };
            $fetch_row->();
        }
    });
}

# do_auth($hd, $username, [$password, [$database,]] $cb->($success, $err_num_and_msg))
sub do_auth {
    my $cb = ref($_[-1]) eq 'CODE' ? pop : sub {};
    my($hd, $username, $password, $database) = @_;

    recv_packet($hd, sub {
        my $proto_ver = take_num($_[0], 1); warn "proto_ver:$proto_ver" if DEV;
        my $server_ver = take_zstr($_[0]); warn "server_ver:$server_ver" if DEV;
        my $thread_id = take_num($_[0], 4); warn "thread_id:$thread_id" if DEV;
        my $scramble_buff = take_str($_[0], 8).substr($_[0], -13, 12); warn "scramble_buff:$scramble_buff" if DEV;
        my $filler = take_num($_[0], 1); warn "filler:$filler" if DEV;
        my $server_cap = take_num($_[0], 2);
        my $server_lang = take_num($_[0], 1); warn "server_lang:$server_lang" if DEV;
        my $server_status = take_num($_[0], 2); warn "server_status:$server_status" if DEV;
        $server_cap += take_num($_[0], 2) << 16;
        if( DEV ) {
            warn "server_cap:";
            warn "  CLIENT_LONG_PASSWORD" if( $server_cap & CLIENT_LONG_PASSWORD );
            warn "  CLIENT_FOUND_ROWS" if( $server_cap & CLIENT_FOUND_ROWS );
            warn "  CLIENT_LONG_FLAG" if( $server_cap & CLIENT_LONG_FLAG );
            warn "  CLIENT_CONNECT_WITH_DB" if( $server_cap & CLIENT_CONNECT_WITH_DB );
            warn "  CLIENT_NO_SCHEMA" if( $server_cap & CLIENT_NO_SCHEMA );
            warn "  CLIENT_COMPRESS" if( $server_cap & CLIENT_COMPRESS );
            warn "  CLIENT_ODBC" if( $server_cap & CLIENT_ODBC );
            warn "  CLIENT_LOCAL_FILES" if( $server_cap & CLIENT_LOCAL_FILES );
            warn "  CLIENT_IGNORE_SPACE" if( $server_cap & CLIENT_IGNORE_SPACE );
            warn "  CLIENT_PROTOCOL_41" if( $server_cap & CLIENT_PROTOCOL_41 );
            warn "  CLIENT_INTERACTIVE" if( $server_cap & CLIENT_INTERACTIVE );
            warn "  CLIENT_SSL" if( $server_cap & CLIENT_SSL );
            warn "  CLIENT_IGNORE_SIGPIPE" if( $server_cap & CLIENT_IGNORE_SIGPIPE );
            warn "  CLIENT_TRANSACTIONS" if( $server_cap & CLIENT_TRANSACTIONS );
            warn "  CLIENT_RESERVED" if( $server_cap & CLIENT_RESERVED );
            warn "  CLIENT_SECURE_CONNECTION" if( $server_cap & CLIENT_SECURE_CONNECTION );
            warn "  CLIENT_MULTI_STATEMENTS" if( $server_cap & CLIENT_MULTI_STATEMENTS );
            warn "  CLIENT_MULTI_RESULTS" if( $server_cap & CLIENT_MULTI_RESULTS );
        }
        my $scramble_len = take_num($_[0], 1); warn "scramble_len:$scramble_len" if DEV;

        my $packet = '';
        put_num($packet, $server_cap & (
            CLIENT_LONG_PASSWORD     | # new more secure passwords
            CLIENT_FOUND_ROWS        | # Found instead of affected rows
            CLIENT_LONG_FLAG         | # Get all column flags
            CLIENT_CONNECT_WITH_DB   | # One can specify db on connect
            # CLIENT_NO_SCHEMA         | # Don't allow database.table.column
            # CLIENT_COMPRESS          | # Can use compression protocol
            # CLIENT_ODBC              | # Odbc client
            # CLIENT_LOCAL_FILES       | # Can use LOAD DATA LOCAL
            # CLIENT_IGNORE_SPACE      | # Ignore spaces before '('
            CLIENT_PROTOCOL_41       | # New 4.1 protocol
            # CLIENT_INTERACTIVE       | # This is an interactive client
            # CLIENT_SSL               | # Switch to SSL after handshake
            # CLIENT_IGNORE_SIGPIPE    | # IGNORE sigpipes
            CLIENT_TRANSACTIONS      | # Client knows about transactions
            # CLIENT_RESERVED          | # Old flag for 4.1 protocol 
            CLIENT_SECURE_CONNECTION | # New 4.1 authentication
            CLIENT_MULTI_STATEMENTS  | # Enable/disable multi-stmt support
            CLIENT_MULTI_RESULTS     | # Enable/disable multi-results
            0
        ), 4); # client_flags
        put_num($packet, 0x1000000, 4); # max_packet_size
        $packet .= $server_lang; # charset_number
        $packet .= "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"; # filler
        put_zstr($packet, $username); # username
        if( $password eq '' ) {
            put_lcs($packet, '');
        }
        else {
            my $stage1_hash = sha1($password);
            put_lcs($packet, sha1($scramble_buff.sha1($stage1_hash)) ^ $stage1_hash); # scramble buff
        }
        put_zstr($packet, $database); # database name

        send_packet($hd, 1, $packet);
        recv_packet($hd, sub {
            if( parse_ok($_[0]) ) {
                $cb->(1);
            }
            else {
                my($errno, $sqlstate, $message) = parse_error($_[0]);
                warn "$errno [$sqlstate] $message" if DEV;
                $cb->(0, dualvar($errno, $message));
            }
        });
    });
}

# do_query($hd, $sql)
sub do_query {
    send_packet($_[0], 0, COM_QUERY, $_[1]);
}

1;
