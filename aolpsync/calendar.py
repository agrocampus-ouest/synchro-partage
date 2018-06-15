from .logging import Logging
from .utils import Zimbra , ZimbraError


class CalendarSync:
    """
    Classe permettant d'ajouter les agendas correspondant aux emplois du temps.
    """

    def __init__( self , cfg ):
        """
        Lit et vérifie la configuration. Si elle ne contient pas de section
        "[calendars]", l'instance sera désactivée.

        :param cfg: la configuration
        """
        self.enabled = cfg.has_section( 'calendars' )
        if not self.enabled: return
        self.source_db_ = cfg.get( 'calendars' , 'source' ,
                raise_missing = True )
        self.source_query_ = cfg.get( 'calendars' , 'query' ,
                raise_missing = True )
        self.folder_name_ = cfg.get( 'calendars' , 'folder-name' ,
                raise_missing = True )
        self.folder_color_ = cfg.get( 'calendars' , 'folder-color' )
        self.blocking_ = cfg.has_flag( 'calendars' , 'blocking-events' )

        fixer = lambda addr : addr
        if not cfg.has_flag( 'bss' , 'dont-fix-domains' ):
            ldap_dom = '@{}'.format( cfg.get( 'ldap' , 'mail-domain' ) )
            bss_dom = '@{}'.format( cfg.get( 'bss' , 'domain' ) )
            if ldap_dom != bss_dom:
                fixer = lambda addr : (
                        addr if not addr.endswith( ldap_dom )
                        else ( addr[ :-len( ldap_dom ) ] + bss_dom )
                    )
        self.address_fixer_ = fixer

        self.zimbra_ = Zimbra( cfg )

    def synchronize( self , accounts , sync_set = None ):
        """
        Effectue la synchronisation des emplois du temps.

        :param accounts: la liste des comptes, sous la forme d'un dictionnaire \
                associant les instances SyncAccount aux EPPN
        """
        if not self.enabled: return

        address_map = self.get_address_map_( accounts )
        calendars = self.get_calendars_( address_map )

        # Génère l'ensemble des comptes à synchroniser
        sync = set( calendars.keys( ) )
        if sync_set is not None:
            sync.intersection_update( sync_set )

        # Pour chaque compte, on ajoute l'emploi du temps
        for eppn in sync:
            addr = accounts[ eppn ].mail
            try:
                self.add_calendar_( addr , calendars[ eppn ] )
            except ZimbraError as e:
                Logging( 'cal' ).error( ( 'Erreur Zimbra lors de la mise à '
                        + 'jour du calendrier pour {}: {}' ).format(
                                eppn , str( e ) ) )
        self.zimbra_.terminate( )


    def get_address_map_( self , accounts ):
        """
        Génère un dictionnaire associant toutes les adresses mail connues aux
        EPPN des utilisateurs correspondants. Les comptes pré-supprimés seront
        ignorés.

        :param accounts: la liste des comptes, sous la forme d'un dictionnaire \
                associant les instances SyncAccount aux EPPN

        :return: le dictionnaire des adresses associées à leurs EPPN
        """
        # On génère les correspondances comptes / adresses
        address_map = dict( )
        for eppn in accounts:
            account = accounts[ eppn ]
            if account.markedForDeletion is not None:
                continue
            address_map[ account.mail ] = eppn
            if not account.aliases:
                continue
            if isinstance( account.aliases , str ):
                address_map[ account.aliases ] = eppn
            else:
                for alias in account.aliases:
                    address_map[ alias ] = eppn
        return address_map

    def get_calendars_( self , address_map ):
        """
        Lit la liste des calendriers depuis la base de données source.

        :param address_map: un dictionnaire associant à chaque adresse connue \
                l'EPPN d'un utilisateur

        :return: un dictionnaire associant à chaque EPPN une URL de calendrier
        """
        from .sqldb import query as sql_query
        calendars_list = sql_query( self.source_db_ , self.source_query_ )
        calendars = dict( )
        for row in calendars_list:
            ( addr , url ) = row
            addr = self.address_fixer_( addr )
            if addr not in address_map:
                Logging( 'cal' ).debug(
                        'Adresse {} non trouvée'.format( addr ) )
                continue
            eppn = address_map[ addr ]
            if eppn in calendars:
                Logging( 'cal' ).warning(
                        'EPPN {} trouvé plusieurs fois'.format( eppn ) )
                continue
            Logging( 'cal' ).debug(
                    'Adresse {}, EPPN {}, URL {}'.format( addr , eppn , url ) )
            calendars[ eppn ] = url
        return calendars

    def add_calendar_( self , addr , url ):
        """
        Vérifie si l'emploi du temps doit être ajouté au compte d'un
        utilisateur; s'il n'existe pas, il sera créé. S'il est présent mais
        uniquement dans la poubelle, il en sera extrait.

        :param addr: l'adresse mail du compte
        :param url: l'URL du calendrier
        """
        self.zimbra_.set_user( addr )
        root_folder = self.zimbra_.get_folder( )
        found = self.find_calendar_( root_folder , url )

        if not found:
            # Aucune correspondance -> on crée
            Logging( 'cal' ).info( 'Création du calendrier pour {}'.format(
                    addr ) )
            n = self.gen_calendar_name_( root_folder )
            f = 'i#' + ( '' if self.blocking_ else 'b' )
            self.zimbra_.create_folder( name = n ,
                    parent_id = root_folder[ 'id' ] ,
                    folder_type = 'appointment',
                    color = self.folder_color_ ,
                    flags = f ,
                    url = url ,
                    others = { 'sync' : 1 } )
            return

        # Le calendrier existe et n'est pas à la poubelle -> on ne touche à rien
        if False in ( v[ 0 ] for v in found.values( ) ):
            return

        # Le calendrier est à la poubelle, on le restaure
        Logging( 'cal' ).info( 'Calendrier pour {}: dans la poubelle'.format(
                addr ) )
        ( restore , *junk ) = found.keys( )
        f_data = found[ restore ][ 1 ]
        n = self.gen_calendar_name_( root_folder )
        if n != f_data[ 'name' ]:
            self.zimbra_.rename_folder( f_data[ 'id' ] , n )
        self.zimbra_.move_folder( f_data[ 'id' ] , root_folder[ 'id' ] )

    def gen_calendar_name_( self , root ):
        """
        Génère le nom du calendrier à créer, en vérifiant si le nom existe déjà
        et en y ajoutant un nombre en suffixe si c'est le cas.

        :param root: le dossier racine de l'utilisateur
        :return: le nom à utiliser pour le dossier
        """

        def has_subfolder_( folder , sub_name ):
            """
            Vérifie si un dossier contient un sous-dossier avec un certain nom.

            :param folder: les données du dossier parent
            :param sub_name: le nom à vérifier

            :return: True si un sous-dossier portant le nom spécifié existe
            """
            return 'folder' in folder and sub_name in (
                    f[ 'name' ] for f in folder[ 'folder' ] )

        if not has_subfolder_( root , self.folder_name_ ):
            return self.folder_name_
        x = 2
        while True:
            n = '{} {}'.format( self.folder_name_ , x )
            if not has_subfolder_( root , n ):
                return n
            x += 1

    def find_calendar_( self , folder , url , in_trash = False , v = None ):
        """
        Trouve le ou les exemplaires d'un emploi du temps dans les dossiers d'un
        utilisateur.

        :param folder: le dossier dans lequel chercher
        :param url: l'URL à chercher
        :param in_trash: les dossiers en cours de parcours font-ils partie \
                de la poubelle? (utilisation interne lors de la récursion)
        :param v: le dictionnaire des dossiers trouvés (utilisation interne \
                lors de la récursion)

        :return: un dictionnaire associant aux chemins absolus des comptes \
                trouvés un tuple contenant un booléen qui indique si le \
                dossier trouvé est dans la poubelle, et les données du dossier
        """
        if v is None:
            v = dict( )
        if 'url' in folder and folder[ 'url' ] == url:
            v[ folder[ 'absFolderPath' ] ] = ( in_trash , folder )
            return v
        if 'folder' in folder:
            in_trash = in_trash or folder[ 'absFolderPath' ] == '/Trash'
            for f in folder[ 'folder' ]:
                self.find_calendar_( f , url , in_trash , v )
        return v
