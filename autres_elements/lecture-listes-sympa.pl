#!/usr/bin/perl
use lib split(/:/, $ENV{SYMPALIB} || ''), '/usr/share/sympa/lib';
use strict;
use warnings;
use Conf;
use Sympa::List;
die "Config load failed\n" unless Conf::load();

our %GATHER_CUSTOM = map { $_ => 1 } qw( alias sender partage-group partage-list );

print join( "\n" , ( map {

                my $list = $_;

                my $subject = $list->{admin}{subject};
                $subject =~ s/"/""/g;

		my $hide = grep { $_ eq $list->{admin}{visibility}{name} }
                                qw( conceal secret );
		my $send = $list->{admin}{send}{name} ne 'closed';

		my @extraInfo = (
			( map { '"sender","' . $_->{email} . '"'
				} @{ $list->{admin}{owner} } ) ,
			( map { '"member","' . $_->{email} . '"'
				} $list->get_members( 'member' ) )
		);
		foreach my $c ( @{ $list->{admin}{custom_vars} } ) {
			my ( $n , $v ) = map { $c->{ $_ } } qw( name value );
			next unless exists $GATHER_CUSTOM{ $n };
			$n =~ s/"/""/g;
			$v =~ s/"/""/g;
			push @extraInfo , "\"$n\",\"$v\"";
		}
		my %uniqMap = map { $_ => 1 } @extraInfo;
		my $eiString = join( "\n" , keys %uniqMap );
		$eiString =~ s/"/""/gs;

		'"' . join( '","' ,
			$list->{name} ,
			$hide ? 1 : 0 ,
			$send ? 1 : 0 ,
			$subject , $eiString
		) . '"'

        } ( grep { $_->{admin}{status} eq 'open' }
		@{Sympa::List::get_lists('*')} ) ) );
