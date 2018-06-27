#!/usr/bin/python3

from aolpsync import *


#-------------------------------------------------------------------------------


class MailingListSynchronizer( ProcessSkeleton ):

    def __init__( self ):
        ProcessSkeleton.__init__( self ,
                require_ldap = False ,
                require_cos = False )

    def cli_description( self ):
        return '''Synchronise les listes Sympa avec le serveur Partage.''';

    def cli_register_arguments( self , parser ):
        pass

    def get_ml_data( self ):
        command = 'ssh -i /tmp/test-get-ml mlreader@sympa' # FIXME
        ( ev , output , errors ) = aolputils.run_shell_command( command )
        if ev != 0:
            Logging( 'ml' ).error(
                'Erreur lors de l\'exécution de `{}`: {}'.format( command ,
                        ev ) )
            dump_err = lambda l : Logging( 'alias' ).error( l )
        else:
            dump_err = lambda l : Logging( 'alias' ).warning( l )
        for l in errors:
            dump_err( l )
        if ev != 0:
            raise FatalError( 'Impossible de lire la liste des ML' )

        try:
            return [ line.decode( 'utf-8' ) for line in output ]
        except UnicodeDecodeError:
            Logging( 'alias' ).error( 'Contenu non-UTF-8' )
            raise FatalError( 'Impossible de lire la liste des ML' )

    def init( self ):
        import csv
        reader = csv.reader( self.get_ml_data( ) , delimiter = ',' ,
                quotechar = '"' )
        bss_dom = self.cfg.get( 'bss' , 'domain' )
        ml_dom = 'listes.agrocampus-ouest.fr' # FIXME
        lists = {}
        for row in reader:
            from lib_Partage_BSS.models.Group import Group
            ml = Group( '{}@{}'.format( row[ 0 ] , bss_dom ) )
            ml.members_set.add( '{}@{}'.format( row[ 0 ] , ml_dom ) )
            ml.zimbraHideInGal = int( row[ 1 ] ) == 1
            ml.description = row[ 2 ]
            ml.displayName = '{} (liste)'.format( row[ 0 ] )
            ml.zimbraMailStatus = True
            lists[ ml.name ] = ml
            print( ml.to_json_record( ) )
        self.ml_lists = lists

    def convert_db_lists( self ):
        if 'group' not in self.misc_data:
            return {}
        lists = {}
        for ( k , v ) in self.misc_data[ 'group' ].items( ):
            from lib_Partage_BSS.models.Group import Group
            ml = Group.from_json_record( v )
            if ml.name != k:
                Logging( 'ml' ).error( 'Entrée de base {} - mail {}'.format(
                        k , ml.name ) )
            else:
                lists[ k ] = ml
        return lists

    def remove_list( self , mail ):
        if not BSSAction( 'deleteGroup' , mail , _service_ = 'Group' ):
            return
        self.remove_data( 'group' , mail )
        self.db_lists.pop( mail )

    def process( self ):
        self.db_lists = self.convert_db_lists( )

        db_set = set( self.db_lists.keys( ) )
        ml_set = set( self.ml_lists.keys( ) )
        common = db_set & ml_set

        # Suppression de listes
        rem_lists = db_set - ml_set
        for l in rem_lists:
            self.remove_list( l )

        # Suppression d'aliases de listes
        for ln in common:
            l = self.db_lists[ ln ]
            ml_list = self.ml_lists[ l.name ]
            rem_aliases = l.aliases_set - ml_list.aliases_set
            if not rem_aliases: continue
            if not BSSAction( 'removeGroupAliases' , l.name , rem_aliases ,
                    _service_ = 'Group' ):
                continue
            l.aliases_set.difference_update( rem_aliases )
            self.save_data( 'group' , l.name , l.to_json_record( ) )

        # Création de listes
        add_lists = ml_set - db_set
        for l in add_lists:
            ml = self.ml_lists[ l ]
            if not BSSAction( 'createGroup' , ml , _service_ = 'Group' ):
                continue
            self.db_lists[ l ] = ml
            self.save_data( 'group' , l , ml.to_json_record( ) )

        # Ajout d'aliases de listes
        for ln in common:
            l = self.db_lists[ ln ]
            ml_list = self.ml_lists[ l.name ]
            add_aliases = ml_list.aliases_set - l.aliases_set
            if not add_aliases: continue
            if not BSSAction( 'addGroupAliases' , l.name , add_aliases ,
                    _service_ = 'Group' ):
                continue
            l.aliases_set.update( add_aliases )
            self.save_data( 'group' , l.name , l.to_json_record( ) )

        # Mise à jour des informations de chaque liste
        from lib_Partage_BSS.models.Group import Group
        for ln in common:
            db_list = self.db_lists[ ln ]
            ml_list = self.ml_lists[ ln ]
            if True in [ getattr( db_list , attr ) != getattr( ml_list , attr )
                            for attr in Group.ATTRIBUTES ]:
                if not BSSAction( 'modifyGroup' , ml_list ,
                        _service_ = 'Group' ):
                    continue
                for attr in Group.ATTRIBUTES:
                    setattr( db_list , attr , getattr( ml_list , attr ) )
                self.save_data( 'group' , ln , db_list.to_json_record( ) )
            if db_list.members_set != ml_list.members_set:
                if not BSSAction( 'updateGroupMembers' , db_list ,
                        ml_list.members_set ):
                    continue
                db_list.members_set.clear( )
                db_list.members_set.update( db_list.members_set )
                self.save_data( 'group' , ln , db_list.to_json_record( ) )
            if db_list.senders_set != ml_list.senders_set:
                if not BSSAction( 'updateGroupSenders' , db_list ,
                        ml_list.senders_set ):
                    continue
                db_list.senders_set.clear( )
                db_list.senders_set.update( db_list.senders_set )
                self.save_data( 'group' , ln , db_list.to_json_record( ) )


#-------------------------------------------------------------------------------


try:
    MailingListSynchronizer( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )

