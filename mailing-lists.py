#!/usr/bin/python3

from aolpsync import *


#-------------------------------------------------------------------------------


class MailingListSynchronizer( ProcessSkeleton ):
    """
    Script de synchronisation des mailing lists, des groupes et des listes de
    distribution.
    """

    def __init__( self ):
        ProcessSkeleton.__init__( self ,
                require_ldap = False ,
                require_cos = False )

    def cli_description( self ):
        return '''Synchronise les listes Sympa avec le serveur Partage.''';

    def cli_register_arguments( self , parser ):
        pass

    #---------------------------------------------------------------------------

    def get_ml_data_( self ):
        """
        Exécute la commande permettant d'obtenir les données concernant les
        mailing lists depuis le serveur Sympa.

        :return: les lignes lues sur la sortie standard de la commande

        :raises FatalError: l'exécution de la commande a échoué, ou la sortie \
                de la commande n'était pas de l'UTF-8 valide.
        """
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

    def read_ml_or_group_( self , row ):
        """
        Extrait les informations d'une entrée de données lues depuis le serveur
        Sympa.

        :param input: une entrée CSV lue depuis le serveur

        :return: None si la ligne est vide, ou bien un dictionnaire contenant \
                les informations de l'entrée lue.
        """
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
            # Pas de données supplémentaires
            if not data[ 'is_list' ]: return None
            return data

        import csv
        csv.field_size_limit( 2 ** 31 )
        reader = csv.reader( row[ 4 ].split( '\n' ) ,
                    delimiter = ',' , quotechar = '"' )
        for sr in reader:
            ( name , value ) = ( sr[ 0 ].lower( ) , sr[ 1 ].strip( ) )
            # Entrées d'alias
            if name == 'alias':
                if '@' in value:
                    value = addr_fixer( value )
                else:
                    value = '{}@{}'.format( value , bss_dom )
                data[ 'aliases' ].add( value )
            # Expéditeurs autorisés
            elif name == 'sender':
                if '@' in value:
                    value = addr_fixer( value )
                else:
                    value = '{}@{}'.format( value , bss_dom )
                if value not in self.address_map:
                    Logging( 'ml' ).warning(
                            'Expéditeur autorisé {} non trouvé'.format(
                                value ) )
                    continue
                data[ 'senders' ].add( self.address_map[ value ] )
            # Membres de la liste
            elif name == 'member':
                if '@' in value:
                    value = addr_fixer( value )
                else:
                    value = '{}@{}'.format( value , bss_dom )
                if value in self.address_map:
                    value = self.address_map[ value ]
                data[ 'members' ].add( value )
            # Drapeaux
            elif name == 'partage-group':
                data[ 'is_group' ] = True
            elif name == 'partage-list':
                data[ 'is_list' ] = True
                data[ 'target' ] = None
            else:
                Logging( 'ml' ).warning(
                        '{}: information Sympa inconnue: {}'.format(
                                data[ 'id' ] , name ) )
        return data

    def add_mail_suffix_( self , original , suffix ):
        """
        Ajoute un suffixe à la partie "utilisateur" d'une adresse mail.

        :param original: l'adresse mail originale
        :param suffix: le suffixe à ajouter
        :return: l'adresse modifiée
        """
        from lib_Partage_BSS.exceptions import NameException
        spl_name = original.split( '@' )
        if len( spl_name ) != 2:
            raise NameException( 'Adresse {} invalide'.format( original ) )
        name = '{}-{}@{}'.format( spl_name[ 0 ] , suffix , spl_name[ 1 ] )
        return name

    def row_to_groups_( self , row ):
        """
        Transforme le contenu d'une ligne CSV extraite des informations
        transmises par le serveur Sympa en zéro, une ou deux instances de
        groupes BSS.

        :param row: la ligne CSV à traiter
        :return: une liste contenant entre 0 et 6 instances de \
                lib_Partage_BSS.models.Group.Group
        """
        from lib_Partage_BSS.models.Group import Group
        data = self.read_ml_or_group_( row )
        if data is None:
            return ( )
        rv = []

        if data[ 'is_group' ]:
            # Groupes Partage
            ml = Group( 'grp-{}'.format( data[ 'name' ] ) )
            ml.zimbraHideInGal = data[ 'hidden' ]
            ml.description = data[ 'desc' ]
            ml.zimbraMailStatus = False
            ml.displayName = 'Groupe {}'.format( data[ 'id' ] )
            ml.members_set.update( data[ 'members' ] )
            rv.append( ml )

        if data[ 'is_list' ]:
            # Mailing lists ou listes de distribution Partage
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
            if data[ 'target' ] is not None:
                # Pour les mailing lists, il faut également générer les aliases
                # <liste>-request, <liste>-editor, <liste>-unsubscribe et
                # <liste>-owner
                SPECIAL = ( 'request' , 'editor' , 'unsubscribe' , 'owner' )
                for suffix in SPECIAL:
                    name = self.add_mail_suffix_( data[ 'name' ] , suffix )
                    ml = Group( name )
                    ml.zimbraHideInGal = True
                    ml.description = '{} (Sympa: {})'.format(
                            data[ 'desc' ] , suffix )
                    ml.zimbraMailStatus = True
                    ml.displayName = 'Liste {} - {}' .format(
                            data[ 'id' ] , suffix )
                    ml.members_set.add( self.add_mail_suffix_(
                            data[ 'target' ] , suffix ) )
                    rv.append( ml )

        return rv

    def convert_db_lists_( self ):
        """
        Transforme les entrées supplémentaires de base de données dans la
        catégorie 'group' en une liste d'instances de groupes.

        :return: un dictionnaire associant à chaque adresse de groupe l'entrée \
                correspondande
        """
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

    def remove_list_( self , mail ):
        """
        Supprime un groupe de Partage et de la base de données de
        synchronisation. Si la suppression sur Partage échoue, l'entrée de base
        de données ne sera pas affectée.

        :param str mail: l'adresse principale du groupe à supprimer
        """
        if not BSSAction( 'deleteGroup' , mail , _service_ = 'Group' ):
            return
        self.remove_data( 'group' , mail )
        self.db_lists.pop( mail )

    def add_aliases_( self , group , aliases ):
        """
        Ajoute des alias à un groupe. Une tentative sera effectuée pour chaque
        alias; en cas de succès, l'alias sera ajouté à l'entrée de base de
        données correspondante.

        :param group: l'instance de la base de données de synchronisation
        :param aliases: l'ensemble ou la liste des alias à ajouter
        """
        for alias in aliases:
            if not BSSAction( 'addGroupAliases' , group.name , alias ,
                    _service_ = 'Group' ):
                continue
            group.aliases_set.add( alias )
            self.save_data( 'group' , group.name , group.to_json_record( ) )

    def add_members_( self , group , members ):
        """
        Ajoute des membres à un groupe. Une tentative sera effectuée pour chaque
        membre; en cas de succès, le membre sera ajouté à l'entrée de base de
        données correspondante.

        :param group: l'instance de la base de données de synchronisation
        :param members: la liste ou l'ensemble des membres à ajouter
        """
        for member in members:
            if not BSSAction( 'addGroupMembers' , group.name , member ,
                    _service_ = 'Group' ):
                continue
            group.members_set.add( member )
            self.save_data( 'group' , group.name , group.to_json_record( ) )

    def add_senders_( self , group , senders ):
        """
        Ajoute des expéditeurs autorisés à un groupe. Une tentative sera
        effectuée pour chaque expéditeur. En cas de succès, l'expéditeur sera
        ajouté à l'entrée de la base de données.

        :param group: l'instance de la base de données de synchronisation
        :param members: la liste ou l'ensemble des expéditeurs à ajouter
        """
        for sender in senders:
            if not BSSAction( 'addGroupSenders' , group.name , sender ,
                    _service_ = 'Group' ):
                continue
            group.senders_set.add( sender )
            self.save_data( 'group' , group.name , group.to_json_record( ) )

    #---------------------------------------------------------------------------

    def process( self ):
        """
        Effectue la synchronisation des mailing lists, groupes et listes de
        distribution en comparant le contenu de la base de synchronisation et
        les données envoyées par le serveur Sympa.
        """
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
        reader = csv.reader( self.get_ml_data_( ) , delimiter = ',' ,
                quotechar = '"' )
        lists = {}
        for row in reader:
            for ml in self.row_to_groups_( row ):
                lists[ ml.name ] = ml
        self.ml_lists = lists

        self.db_lists = self.convert_db_lists_( )

        db_set = set( self.db_lists.keys( ) )
        ml_set = set( self.ml_lists.keys( ) )
        common = db_set & ml_set

        # Suppression de listes
        rem_lists = db_set - ml_set
        for l in rem_lists:
            self.remove_list_( l )

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
            if oml_aliases: self.add_aliases_( ml , oml_aliases )
            if oml_members: self.add_members_( ml , oml_members )
            if oml_senders: self.add_senders_( ml , oml_senders )

        # Ajout d'aliases de listes
        for ln in common:
            l = self.db_lists[ ln ]
            ml_list = self.ml_lists[ l.name ]
            add_aliases = ml_list.aliases_set - l.aliases_set
            if add_aliases: self.add_aliases_( l , add_aliases )

        # Mise à jour des informations de chaque liste
        from lib_Partage_BSS.models.Group import Group
        for ln in common:
            db_list = self.db_lists[ ln ]
            ml_list = self.ml_lists[ ln ]
            # Mise à jour des attributs
            if True in [ getattr( db_list , attr ) != getattr( ml_list , attr )
                            for attr in Group.ATTRIBUTES ]:
                if not BSSAction( 'modifyGroup' , ml_list ,
                        _service_ = 'Group' ):
                    continue
                for attr in Group.ATTRIBUTES:
                    setattr( db_list , attr , getattr( ml_list , attr ) )
                self.save_data( 'group' , ln , db_list.to_json_record( ) )
            # Mise à jour des membres
            if db_list.members_set != ml_list.members_set:
                if BSSAction( 'updateGroupMembers' , db_list ,
                        ml_list.members_set , _service_ = 'Group' ):
                    db_list.members_set.clear( )
                    db_list.members_set.update( ml_list.members_set )
                    self.save_data( 'group' , ln , db_list.to_json_record( ) )
            # Mise à jour des expéditeurs
            if db_list.senders_set != ml_list.senders_set:
                if BSSAction( 'updateGroupSenders' , db_list ,
                        ml_list.senders_set , _service_ = 'Group' ):
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

