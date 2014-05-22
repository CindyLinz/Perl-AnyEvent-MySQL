#!perl -T

use strict;
use warnings;
use Test::More;
plan skip_all => 'These tests are only for authors' unless $ENV{AUTHOR_TESTING} or $ENV{RELEASE_TESTING};

# Ensure a recent version of Test::Pod
my $min_tp = 1.22;
eval "use Test::Pod $min_tp";
plan skip_all => "Test::Pod $min_tp required for testing POD" if $@;

all_pod_files_ok();
