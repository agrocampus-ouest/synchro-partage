#!/usr/bin/python3

from aolpsync import *


#
# Mode de fonctionnement avec les envoyeurs autorisés, pour l'instant:
#
# -> on supprime des informations reçues de sympa concernant les utilisateurs
# non trouvés dans la base des utilisateurs
#
# -> on ne touche pas ceux de la BDD
#


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
        command = self.cfg.get( 'bss-groups' , 'command' , raise_missing = True
                ).replace( '!configdir!' , Config.CONFIG_DIR )
        ( ev , output , errors ) = aolputils.run_shell_command( command )
        if ev != 0:
            Logging( 'ml' ).error(
                'Erreur lors de l\'exécution de `{}`: {}'.format( command ,
                        ev ) )
            dump_err = lambda l : Logging( 'alias' ).error( l )
        else:
            dump_err = lambda l : Logging( 'alias' ).warning( l )
        for l in errors:
            if l and l != b'\n':
                dump_err( l.decode( ) )
        if ev != 0:
            raise FatalError( 'Impossible de lire la liste des ML' )

        try:
            return [ line.decode( 'utf-8' )
                    for line in output ]
        except UnicodeDecodeError:
            Logging( 'alias' ).error( 'Contenu non-UTF-8' )
            raise FatalError( 'Impossible de lire la liste des ML' )

    def read_ml_or_group( self , row ):
        if not row:
            return None
        bss_dom = self.cfg.get( 'bss' , 'domain' )
        ml_dom = self.cfg.get( 'bss-groups' , 'ml-domain' )
        addr_fixer = aolputils.get_address_fixer( self.cfg )
        m_id = row[ 0 ].strip( )
        data = {
            'id'        : m_id ,
            'name'      : addr_fixer( '{}@{}'.format( m_id , bss_dom ) ) ,
            'target'    : '{}@{}'.format( m_id , ml_dom ) ,
            'hidden'    : int( row[ 1 ] ) == 1 ,
            'desc'      : row[ 3 ].strip( ) ,
            'is_list'   : int( row[ 2 ] ) == 1 ,
            'is_group'  : False ,
            'aliases'   : set( ) ,
            'senders'   : set( ) ,
            'members'   : set( ) ,
        }
        if row[ 4 ] == '':
            if not data[ 'is_list' ]: return None
            return data

        import csv
        reader = csv.reader( row[ 4 ].split( '\n' ) ,
                    delimiter = ',' , quotechar = '"' )
        p_list = False
        for sr in reader:
            ( name , value ) = ( sr[ 0 ].lower( ) , sr[ 1 ].strip( ) )
            if name == 'alias':
                if '@' in value:
                    value = addr_fixer( value )
                else:
                    value = '{}@{}'.format( value , bss_dom )
                data[ 'aliases' ].add( value )
            elif name == 'sender':
                if '@' in value:
                    value = addr_fixer( value )
                else:
                    value = '{}@{}'.format( value , bss_dom )
                if value not in self.address_map:
                    # FIXME log
                    continue
                data[ 'senders' ].add( self.address_map[ value ] )
            elif name == 'member':
                if '@' in value:
                    value = addr_fixer( value )
                else:
                    value = '{}@{}'.format( value , bss_dom )
                if value in self.address_map:
                    value = self.address_map[ value ]
                data[ 'members' ].add( value )
            elif name == 'partage-group':
                data[ 'is_group' ] = True
            elif name == 'partage-list':
                p_list = True
            else:
                Logging( 'ml' ).warning(
                        '{}: information Sympa inconnue: {}'.format(
                                data[ 'id' ] , name ) )
        if p_list:
            data[ 'is_list' ] = True
            data[ 'target' ] = None
        return data

    def row_to_groups( self , row ):
        from lib_Partage_BSS.models.Group import Group
        data = self.read_ml_or_group( row )
        if data is None:
            return ( )
        rv = []

        if data[ 'is_group' ]:
            ml = Group( 'grp-{}'.format( data[ 'name' ] ) )
            ml.zimbraHideInGal = data[ 'hidden' ]
            ml.description = data[ 'desc' ]
            ml.zimbraMailStatus = False
            ml.displayName = 'Groupe {}'.format( data[ 'id' ] )
            ml.members_set.update( data[ 'members' ] )
            rv.append( ml )

        if data[ 'is_list' ]:
            ml = Group( data[ 'name' ] )
            ml.zimbraHideInGal = data[ 'hidden' ]
            ml.description = data[ 'desc' ]
            ml.zimbraMailStatus = True
            ml.displayName = 'Liste {}'.format( data[ 'id' ] )
            if data[ 'target' ] is None:
                ml.members_set.update( data[ 'members' ] )
            else:
                ml.members_set.add( data[ 'target' ] )
            ml.aliases_set.update( data[ 'aliases' ] )
            ml.senders_set.update( data[ 'senders' ] )
            rv.append( ml )

        return rv

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

    def add_aliases( self , group , aliases ):
        for alias in aliases:
            if not BSSAction( 'addGroupAliases' , group.name , alias ,
                    _service_ = 'Group' ):
                continue
            group.aliases_set.add( alias )
            self.save_data( 'group' , group.name , group.to_json_record( ) )

    def add_members( self , group , members ):
        for member in members:
            if not BSSAction( 'addGroupMembers' , group.name , member ,
                    _service_ = 'Group' ):
                continue
            group.members_set.add( member )
            self.save_data( 'group' , group.name , group.to_json_record( ) )

    def add_senders( self , group , senders ):
        for sender in senders:
            if not BSSAction( 'addGroupSenders' , group.name , sender ,
                    _service_ = 'Group' ):
                continue
            group.senders_set.add( sender )
            self.save_data( 'group' , group.name , group.to_json_record( ) )

    def process( self ):
        self.address_map = {}
        for eppn in self.db_accounts:
            acc = self.db_accounts[ eppn ]
            if acc.markedForDeletion is not None:
                continue
            self.address_map[ eppn ] = eppn
            if acc.aliases is None:
                continue
            for alias in acc.aliases:
                self.address_map[ alias ] = eppn

        import csv
        reader = csv.reader( self.get_ml_data( ) , delimiter = ',' ,
                quotechar = '"' )
        lists = {}
        for row in reader:
            for ml in self.row_to_groups( row ):
                lists[ ml.name ] = ml
        self.ml_lists = lists

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
            oml_members = set( ml.members_set )
            oml_senders = set( ml.senders_set )
            oml_aliases = set( ml.aliases_set )
            ml.members_set.clear( )
            ml.senders_set.clear( )
            ml.aliases_set.clear( )
            if not BSSAction( 'createGroup' , ml , _service_ = 'Group' ):
                continue
            self.db_lists[ l ] = ml
            self.save_data( 'group' , l , ml.to_json_record( ) )
            if oml_aliases: self.add_aliases( ml , oml_aliases )
            if oml_members: self.add_members( ml , oml_members )
            if oml_senders: self.add_senders( ml , oml_senders )

        # Ajout d'aliases de listes
        for ln in common:
            l = self.db_lists[ ln ]
            ml_list = self.ml_lists[ l.name ]
            add_aliases = ml_list.aliases_set - l.aliases_set
            if add_aliases: self.add_aliases( l , add_aliases )

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
                        ml_list.members_set , _service_ = 'Group' ):
                    continue
                db_list.members_set.clear( )
                db_list.members_set.update( ml_list.members_set )
                self.save_data( 'group' , ln , db_list.to_json_record( ) )
            if db_list.senders_set != ml_list.senders_set:
                if not BSSAction( 'updateGroupSenders' , db_list ,
                        ml_list.senders_set , _service_ = 'Group' ):
                    continue
                db_list.senders_set.clear( )
                db_list.senders_set.update( ml_list.senders_set )
                self.save_data( 'group' , ln , db_list.to_json_record( ) )


#-------------------------------------------------------------------------------


try:
    MailingListSynchronizer( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )

