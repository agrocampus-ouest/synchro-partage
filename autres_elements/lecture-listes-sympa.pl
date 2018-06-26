#!/usr/bin/perl
use lib split(/:/, $ENV{SYMPALIB} || ''), '/usr/share/sympa/lib';
use strict;
use warnings;
use Conf;
use Sympa::List;
die "Config load failed\n" unless Conf::load();
print join( '' , ( map {
                my $list = $_;
                my $subject = $list->{admin}{subject};
                $subject =~ s/"/""/g;
                $list->{name} . ','
                        . ( ( grep { $_ eq $list->{admin}{visibility}{name} }
                                qw( conceal secret ) ) ? 1 : 0 )
                        . ',"' . $subject . "\"\n"
        } ( grep { $_->{admin}{status} eq 'open' }
				@{Sympa::List::get_lists('*')} ) ) );
