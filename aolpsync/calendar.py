from .logging import Logging
from .utils import ( Zimbra , ZimbraError , ZimbraConnectionError ,
                     ZimbraRequestError , FatalError )
from .rules import Rule

#
# Nouvelle procédure pour la mise à jour des calendriers:
#   * Pour chaque source de type SQL, on lit l'ensemble des entrées
#   * Pour chaque source Zimbra:
#       * Pour chaque compte à synchroniser, on vérifie les droits, avec un
#         résultat de type "aucun/lecture/écriture"
#       * On stocke le triplet "compte source / compte cible / droit"
#   * Pour chaque compte source Zimbra, on se connecte et on modifie les ACL
#     en fonction des triplets générés à l'étape précédente
#   * Pour chaque compte cible, on calcule l'ensemble des calendriers qui
#     devraient être importés.
#   * Pour chaque compte cible:
#       * On se connecte et on lit l'ensemble des dossiers;
#       * On recherche les dossiers déjà existants correspondant aux imports
#       * On crée les entrées manquantes, soit sous forme de dossier WebCal,
#         soit sous forme de montage, en fonction du type de source.
#       * Si des entrées correspondantes existent mais sont dans la poubelle,
#         on les sort de la poubelle.
#       * Si des sources externes sont utilisées, on récupère la liste des
#         sources de données externes, on vérifie la fréquence de
#         rafraîchissment par rapport à la configuration, et on la met à jour.
#


class CalendarImport:
    """
    Classe abstraite qui représente une importation de calendrier.
    """

    def __init__( self , cfg , identifier ):
        """
        Initialise les éléments communs pour les importation de calendriers:
        nom et couleur du dossier, drapeau pour les événements bloquants.

        :param cfg: la configuration
        :param identifier: l'identifiant de l'entrée de synchronisation
        """
        self.identifier = identifier
        self.src_section_ = 'calendars-{}'.format( identifier )
        self.folder_name_ = cfg.get( self.src_section_ , 'folder-name' ,
                raise_missing = True )
        self.folder_color_ = cfg.get( self.src_section_ , 'folder-color' )
        self.blocking_ = cfg.has_flag( self.src_section_ , 'blocking-events' )

        from .utils import get_address_fixer
        self.address_fixer_ = get_address_fixer( cfg )

    def read_data( self , zimbra , address_map ):
        """
        Charge les données correspondant à cette source.

        :param zimbra: une instance de communication avec Zimbra
        :param address_map: un dictionnaire associant à chaque adresse connue \
                l'EPPN d'un utilisateur
        """
        raise NotImplementedError

    def check_privileges( self , accounts , eppn ):
        """
        Vérifie les privilèges dont dispose un utilisateur sur un import de
        calendrier.

        :param accounts: la base de données des comptes
        :param eppn: l'EPPN de l'utilisateur dont les privilèges doivent être \
                calculés

        :return: une chaîne correspondant aux privilèges: "none" si \
                l'utilisateur n'a pas le droit d'accéder au calendrier, \
                "ro" si l'utilisateur a accès en lecture, "rw" si \
                l'utilisateur dispose des droits d'écriture. Si la méthode \
                renvoit None au lieu d'une chaîne, le compte devrait être \
                ignoré pour cette source.
        """
        raise NotImplementedError

    def set_effective_privileges( self , zimbra , privileges ):
        """
        Effectue des opérations d'ajout ou de suppression de privilèges.

        :param zimbra: une instance de communication avec Zimbra
        :param privileges: un dictionnaire associant des EPPN avec les \
                valeurs renvoyées par check_privileges
        """
        raise NotImplementedError

    def find_user_folder( self , eppn , folder ):
        """
        Trouve le ou les exemplaires d'un calendrier dans les dossiers d'un
        utilisateur.

        :param eppn: l'EPPN de l'utilisateur
        :param folder: les données Zimbra du dossier dans lequel chercher

        :return: un dictionnaire associant aux chemins absolus des dossiers \
                trouvés un tuple contenant un booléen qui indique si le \
                dossier trouvé est dans la poubelle, et les données du dossier
        """
        raise NotImplementedError

    def folder_name( self , root , *args ):
        """
        Génère un nom approprié pour un dossier de calendrier à partir du nom
        configuré et du contenu du compte utilisateur; utilisé afin d'éviter
        des problèmes liés à des noms dupliqués.

        :param root: le dossier racine de l'utilisateur
        :param args: dossiers supplémentaires dans lesquels on doit vérifier
        :return: le nom à utiliser pour le dossier
        """

        def has_subfolder_( folder , sub_name ):
            """
            Vérifie si un dossier contient un sous-dossier avec un certain nom.

            :param folder: les données du dossier parent
            :param sub_name: le nom à vérifier

            :return: True si un sous-dossier portant le nom spécifié existe
            """
            return ( ( 'folder' in folder and sub_name in (
                            f[ 'name' ] for f in folder[ 'folder' ] ) )
                or ( 'link' in folder and sub_name in (
                            f[ 'name' ] for f in folder[ 'link' ] ) ) )

        def check_all_folders_( sub_name ):
            """
            Vérifie la présence d'un dossier portant le nom spécifié dans
            l'intégralité des dossiers passés en paramètre à la méthode
            folder_name.

            :param sub_name: le nom à vérifier
            :return: True si l'un des dossiers au moins contient un \
                    sous-dossier portant le nom spécifié
            """
            return True in [
                    has_subfolder_( f , sub_name )
                        for f in ( root , *args ) ]

        if not check_all_folders_( self.folder_name_ ):
            return self.folder_name_
        x = 2
        while True:
            n = '{} {}'.format( self.folder_name_ , x )
            if not check_all_folders_( n ):
                return n
            x += 1

    def create_user_folder( self , zimbra , eppn , parent_id , name ):
        """
        Crée le dossier dans le compte de l'utilisateur.

        :param zimbra: une instance de communication avec Zimbra
        :param eppn: l'EPPN de l'utilisateur
        :param parent_id: l'identifiant du dossier parent
        :param name: le nom du dossier calculé par folder_name

        :return: les informations renvoyées par Zimbra au sujet du dossier créé
        """
        raise NotImplementedError

    def needs_postprocess( self ):
        """
        Permet de vérifier si des opérations supplémentaires sont requises
        pour les sources de ce type.

        :return: True si des opérations supplémentaires sont requises, False \
                dans le cas contraire
        """
        return False

    def start_postproc_for( self , zimbra , eppn , postproc_data ):
        """
        Commence les opérations de postprocessing pour le type de source en
        cours.

        :param zimbra: une instance de communication avec Zimbra
        :param eppn: l'EPPN de l'utilisateur
        :param postproc_data: un dictionnaire dans lequel les informations \
                spécifiques seront stockées
        """
        pass

    def postprocess( self , zimbra , eppn , postproc_data , folder_id ):
        """
        Effectue les opérations de postprocessing pour cette source de
        calendriers.

        :param zimbra: une instance de communication avec Zimbra
        :param eppn: l'EPPN de l'utilisateur
        :param postproc_data: le dictionnaire contenant les éventuelles \
                données supplémentaires chargées par start_postproc_for
        :param folder_id: l'identifiant du dossier ou du point de montage \
                utilisé pour cette ressource
        """
        raise NotImplementedError

    def end_postproc_for( self , zimbra , eppn , postproc_data ):
        """
        Termine les opérations de postprocessing pour le type de source en
        cours.

        :param zimbra: une instance de communication avec Zimbra
        :param eppn: l'EPPN de l'utilisateur
        :param postproc_data: le dictionnaire contenant les données \
                éventuellement générées par start_postproc_for
        """
        pass

#-------------------------------------------------------------------------------

class SQLCalendarImport( CalendarImport ):
    """
    Représente une source de calendriers de type SQL.
    """

    def __init__( self , cfg , identifier ):
        CalendarImport.__init__( self , cfg , identifier )
        self.source_db_ = cfg.get( self.src_section_ , 'source' ,
                raise_missing = True )
        self.source_query_ = cfg.get( self.src_section_ , 'query' ,
                raise_missing = True )
        pi_temp = cfg.get( self.src_section_ , 'polling-interval' , 86400 )
        try:
            self.polling_interval = int( pi_temp ) * 1000
        except ValueError:
            raise FatalError(
                    ( 'Source de calendriers {}: intervalle de '
                    + 'rafraichissement incorrect' ).format( identifier ) )
        if self.polling_interval < 0:
            self.polling_interval = 0

    def read_data( self , zimbra , address_map ):
        # On doit charger toutes les entrées depuis la base de données et les
        # faire correspondre aux EPPNs.
        from .sqldb import query as sql_query
        raw_data = sql_query( self.source_db_ , self.source_query_ )
        self.data_ = dict( )
        for row in raw_data:
            ( addr , url ) = row
            addr = self.address_fixer_( addr )
            if addr not in address_map:
                Logging( 'cal.sql' ).debug(
                        'Adresse {} non trouvée'.format( addr ) )
                continue
            eppn = address_map[ addr ]
            if eppn in self.data_:
                Logging( 'cal.sql' ).warning(
                        'EPPN {} trouvé plusieurs fois'.format( eppn ) )
                continue
            Logging( 'cal.sql' ).debug(
                    'Adresse {}, EPPN {}, URL {}'.format( addr , eppn , url ) )
            self.data_[ eppn ] = url

    def check_privileges( self , accounts , eppn ):
        # 'ro' ou None en fonction de la présence de l'EPPN dans la liste
        if eppn in self.data_:
            return 'ro'
        return None

    def set_effective_privileges( self , zimbra , privileges ):
        # Rien à faire pour ce type de source
        pass

    def find_user_folder( self , eppn , folder ):
        # Vérification des vrais dossiers pour lesquels une URL est définie
        # et correspond à celle attendue pour l'utilisateur en question
        if eppn not in self.data_:
            return {}
        return self.find_recursive_( folder , self.data_[ eppn ] , False , {} )

    def find_recursive_( self , folder , url , in_trash , rv ):
        """
        Trouve le ou les exemplaires d'un calendrier Webcal dans les dossiers
        d'un utilisateur.

        :param folder: le dossier dans lequel chercher
        :param url: l'URL à chercher
        :param in_trash: les dossiers en cours de parcours font-ils partie \
                de la poubelle?
        :param rv: le dictionnaire des dossiers trouvés

        :return: un dictionnaire associant aux chemins absolus des comptes \
                trouvés un tuple contenant un booléen qui indique si le \
                dossier trouvé est dans la poubelle, et les données du dossier
        """
        if 'url' in folder and folder[ 'url' ] == url:
            rv[ folder[ 'absFolderPath' ] ] = ( in_trash , folder )
            return rv
        if 'folder' in folder:
            in_trash = in_trash or folder[ 'absFolderPath' ] == '/Trash'
            for f in folder[ 'folder' ]:
                self.find_recursive_( f , url , in_trash , rv )
        return rv

    def create_user_folder( self , zimbra , eppn , parent_id , name ):
        # Création d'un dossier synchronisé sur l'URL indiquée
        assert eppn in self.data_
        return zimbra.create_folder(
                name = name ,
                parent_id = parent_id ,
                folder_type = 'appointment',
                color = self.folder_color_ ,
                flags = 'i#' + ( '' if self.blocking_ else 'b' ) ,
                url = self.data_[ eppn ] ,
                others = { 'sync' : 1 } )

    def needs_postprocess( self ):
        """
        Les calendriers externes ont toujours besoin de traîtements
        supplémentaires.
        """
        return True

    def start_postproc_for( self , zimbra , eppn , postproc_data ):
        """
        Récupère la liste des sources de données pour cet utilisateur si elle
        n'a pas déjà été chargée.
        """
        assert eppn in self.data_
        if 'data_sources' not in postproc_data:
            postproc_data[ 'data_sources' ] = zimbra.get_data_sources( )
            Logging( 'cal.sql' ).debug( 'Sources de données pour {}: {}'.format(
                    eppn , repr( postproc_data[ 'data_sources' ] ) ) )

    def postprocess( self , zimbra , eppn , postproc_data , folder_id ):
        """
        :param zimbra: une instance de communication avec Zimbra
        :param eppn: l'EPPN de l'utilisateur
        :param postproc_data: le dictionnaire contenant les éventuelles \
                données supplémentaires chargées par start_postproc_for
        :param folder_id: l'identifiant du dossier ou du point de montage \
                utilisé pour cette ressource
        """

        # A-t-on des sources de données ?
        ds = postproc_data[ 'data_sources' ]
        if 'cal' not in ds:
            Logging( 'cal.sql' ).warning(
                'Pas de sources de type calendrier pour {}'.format( eppn ) )
            return

        # On recherche la source de données pour ce dossier
        source = None
        cals = ds[ 'cal' ] if type( ds[ 'cal' ] ) is list else [ ds[ 'cal' ] ]
        for cal in cals:
            if str( folder_id ) == str( cal[ 'l' ] ):
                source = cal
                break
        if source is None:
            Logging( 'cal.sql' ).warning(
                'Dossier #{} pour {}: source de données non trouvée'.format(
                        folder_id , eppn ) )
            return
        Logging( 'cal.sql' ).debug(
                ( 'Dossier #{} pour {}: source de données {} '
                        + '/ intervalle de mise à jour {} ms' ).format(
                    folder_id , eppn , cal[ 'id' ] ,
                    cal[ 'pollingInterval' ] ) )

        # Vérification / mise à jour de l'intervalle de rafraîchissement
        if cal[ 'pollingInterval' ] == self.polling_interval:
            return
        Logging( 'cal.sql' ).info(
                ( "Mise à jour de l'intervalle de rafraîchissement du dossier "
                        + "#{} de {} ({} ms vers {} ms)" ).format(
                    folder_id , eppn , cal[ 'pollingInterval' ] ,
                    self.polling_interval ) )
        try:
            zimbra.modify_data_source( 'cal' , cal[ 'id' ] ,
                    pollingInterval = self.polling_interval )
        except ZimbraError as e:
            Logging( 'cal.sql' ).error(
                ( "Mise à jour de l'intervalle de rafraîchissement du dossier "
                        + "#{} de {}: erreur Zimbra {}" ).format(
                    folder_id , eppn , str( e ) ) )


#-------------------------------------------------------------------------------

class ZimbraCalendarImport( CalendarImport ):
    """
    Représente un calendrier partagé Zimbra.
    """

    def __init__( self , cfg , identifier ):
        CalendarImport.__init__( self , cfg , identifier )
        self.source_account_ = cfg.get( self.src_section_ , 'source' ,
                raise_missing = True )
        self.source_folder_ = cfg.get( self.src_section_ , 'uuid' ,
                raise_missing = True )
        self.read_rule_ = Rule( '{}/read-rule'.format( self.src_section_ ) ,
                cfg.get( self.src_section_ , 'read-rule' ,
                        default = '(true)' ) )
        self.write_rule_ = Rule( '{}/write-rule'.format( self.src_section_ ) ,
                cfg.get( self.src_section_ , 'write-rule' ,
                        default = '(false)' ) )

    def read_data( self , zimbra , address_map ):
        # On lit les informations du dossier depuis l'API Zimbra, et on extrait
        # les privilèges associés.
        zimbra.set_user( self.source_account_ )
        try:
            self.folder_data_ = zimbra.get_folder(
                    uuid = self.source_folder_ , recursive = False )
        except ZimbraRequestError as e:
            raise FatalError( 'Section {}: erreur Zimbra {}'.format(
                    self.src_section_ , e ) )
        if ( self.folder_data_ is None
                or self.folder_data_[ 'view' ] != 'appointment' ):
            raise FatalError( 'Section {}: calendrier {} non trouvé'.format(
                    self.src_section_ , self.source_folder_ ) )
        self.folder_acl_ = zimbra.extract_acl( self.folder_data_ )

    def check_privileges( self , accounts , eppn ):
        # On utilise les règles configurées pour calculer les droits d'un
        # utilisateur
        if eppn == self.source_account_:
            return None
        assert eppn in accounts
        if self.write_rule_.check( accounts[ eppn ] ):
            return 'rw'
        if self.read_rule_.check( accounts[ eppn ] ):
            return 'ro'
        return 'none'

    def set_effective_privileges( self , zimbra , privileges ):
        # On vérifie pour chaque EPPN la présence ou l'absence d'ACL
        # correspondants et l'on ajoute ou supprime des entrées en conséquence.
        zimbra.set_user( self.source_account_ )
        zimbra_privs = {
            'ro': 'r' ,
            'rw': 'rwidx'
        }
        for eppn in privileges:
            wanted = privileges[ eppn ]
            if wanted == 'none':
                self.remove_priv_( zimbra , eppn )
            else:
                self.set_priv_( zimbra , eppn , zimbra_privs[ wanted ] )

    def remove_priv_( self , zimbra , eppn ):
        """
        Supprime les droits qu'un utilisateur pourrait avoir sur le dossier.

        :param zimbra: l'instance de communication avec Zimbra
        :param eppn: l'EPPN de l'utilisateur
        """
        if eppn in self.folder_acl_:
            zimbra.remove_grant( self.folder_data_[ 'id' ] ,
                    self.folder_acl_[ eppn ][ 'zid' ] )

    def set_priv_( self , zimbra , eppn , wanted ):
        """
        Place les droits voulus pour un utilisateur, à moins qu'il ne les ait
        déjà.

        :param zimbra: l'instance de communication avec Zimbra
        :param eppn: l'EPPN de l'utilisateur
        :param wanted: les permissions Zimbra voulues
        """
        if ( eppn not in self.folder_acl_
                or self.folder_acl_[ eppn ][ 'perm' ] != wanted ):
            zimbra.set_grant( self.folder_data_[ 'id' ] , eppn , wanted )

    def find_user_folder( self , eppn , folder ):
        # Vérification des liens dans chaque dossier; on recherche un lien pour
        # lequel le champ ruuid correspond à l'UUID du dossier source.
        return self.find_recursive_( folder , False , {} )

    def find_recursive_( self , folder , in_trash , rv ):
        """
        Trouve le ou les exemplaires d'un calendrier Webcal dans les dossiers
        d'un utilisateur.

        :param folder: le dossier dans lequel chercher
        :param in_trash: les dossiers en cours de parcours font-ils partie \
                de la poubelle?
        :param rv: le dictionnaire des dossiers trouvés

        :return: un dictionnaire associant aux chemins absolus des comptes \
                trouvés un tuple contenant un booléen qui indique si le \
                dossier trouvé est dans la poubelle, et les données du dossier
        """
        in_trash = in_trash or folder[ 'absFolderPath' ] == '/Trash'
        if 'link' in folder:
            for l in folder[ 'link' ]:
                if ( l[ 'ruuid' ] == self.source_folder_
                        and l[ 'owner' ] == self.source_account_ ):
                    rv[ l[ 'absFolderPath' ] ] = ( in_trash , l )
        if 'folder' in folder:
            for f in folder[ 'folder' ]:
                self.find_recursive_( f , in_trash , rv )
        return rv

    def create_user_folder( self , zimbra , eppn , parent_id , name ):
        # Création d'un point de montage
        return zimbra.mount(
                source_eppn = self.source_account_ ,
                source_id   = self.folder_data_[ 'id' ] ,
                parent_id   = parent_id ,
                name        = name ,
                folder_type = 'appointment' ,
                color       = self.folder_color_ ,
                flags       = 'i#' + ( '' if self.blocking_ else 'b' )
            )


#-------------------------------------------------------------------------------

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

        # Nombre de tentatives
        max_attempts = cfg.get( 'calendars' , 'zimbra-max-attempts' ,
                default = '3' )
        try:
            max_attempts = int( max_attempts )
            if max_attempts < 1:
                raise ValueError
        except ValueError:
            raise FatalError(
                    'Valeur incorrecte pour calendars.zimbra-max-attempts' )
        self.max_attempts_ = max_attempts

        # Chargement des sources de données
        src_names = cfg.get( 'calendars' , 'sources' , raise_missing = True )
        sources = {}
        for src_name in src_names.split( ',' ):
            if src_name in sources:
                raise FatalError( 'La source {} est listée plusieurs fois' )
            sec = 'calendars-{}'.format( src_name )
            if not cfg.has_section( sec ):
                raise FatalError( 'La source {} n\'est pas configurée'.format(
                        src_name ) )
            src_type = cfg.get( sec , 'type' , raise_missing = True )
            if src_type == 'sql':
                src = SQLCalendarImport( cfg , src_name )
            elif src_type == 'mount':
                src = ZimbraCalendarImport( cfg , src_name )
            else:
                raise FatalError( 'Type de source {} inconnu'.format(
                        src_type ) )
            sources[ src_name ] = src
        if not sources:
            raise FatalError( 'Synchronisation calendrier: pas de sources' )
        self.sources_ = sources

        # Initialisation connexion Zimbra
        self.zimbra_ = Zimbra( cfg )

    def synchronize( self , accounts , sync_set = None ):
        """
        Effectue la synchronisation des emplois du temps.

        :param accounts: la liste des comptes, sous la forme d'un dictionnaire \
                associant les instances SyncAccount aux EPPN
        :param sync_set: si ce paramètre est utilisé, il doit contenir un \
                ensemble d'EPPNs; seuls les comptes correspondants seront \
                mis à jour, à condition qu'ils existent.
        """
        if not self.enabled: return

        # Si on n'a pas de sync_set, on le génère à partir des comptes
        if sync_set is None:
            sync_set = set( accounts.keys( ) )
        sync_set = set( k for k in sync_set
                if accounts[ k ].markedForDeletion is None )
        if not sync_set:
            return

        # Chargement des données initiales
        address_map = self.get_address_map_( accounts )
        for src in self.sources_:
            Logging( 'cal' ).info( 'Chargement données depuis source {}'.format(
                    src ) )
            self.sources_[ src ].read_data( self.zimbra_ , address_map )

        # Pour chaque combinaison source / utilisateur à synchroniser, on
        # détermine les privilèges applicables.
        user_privs = { }
        source_privs = { }
        for src in self.sources_:
            source = self.sources_[ src ]
            for user in sync_set:
                priv = source.check_privileges( accounts , user )
                if priv is None:
                    continue
                if user not in user_privs:
                    user_privs[ user ] = {}
                if src not in source_privs:
                    source_privs[ src ] = {}
                user_privs[ user ][ src ] = priv
                source_privs[ src ][ user ] = priv
        if not user_privs:
            return
        Logging( 'cal' ).debug(
                'Comptes à synchroniser: {}'.format( len( user_privs ) ) )

        # On tente d'appliquer les privilèges pour chaque source
        for src in source_privs:
            Logging( 'cal' ).debug(
                    'Mise en place des privilèges pour la source {}'.format(
                            src ) )
            self.sources_[ src ].set_effective_privileges( self.zimbra_ ,
                    source_privs[ src ] )
        try:
            self.zimbra_.terminate( )
        except ZimbraError as e:
            pass

        # Pour chaque compte, on doit charger la liste des dossiers puis,
        # pour chaque source applicable:
        #   * vérifier la présence du dossier,
        #   * le supprimer si l'utilisateur n'a plus les droits,
        #   * le sortir de la poubelle si l'utilisateur a des droits et que le
        #     dossier a été supprimé,
        #   * le créer s'il n'existe pas.
        for eppn in user_privs:
            usrc = user_privs[ eppn ]

            # Lecture des dossiers
            ( root , err ) = self.zimbra_retry_loop_(
                    lambda : self.get_user_folders_( eppn )
                )
            if root is None:
                if err is not None:
                    Logging( 'cal' ).error( ( 'Erreur Zimbra lors de la '
                            + 'lecture des dossiers de {} : {}' ).format(
                                    eppn , str( err ) ) )
                else:
                    Logging( 'cal' ).error( ( 'Erreur inconnue lors de la '
                            + 'lecture des dossiers de {}' ).format( eppn ) )
                continue

            # Gestion des imports
            import_results = {}
            needs_postproc = []
            for src_name in usrc:
                src = self.sources_[ src_name ]
                ( src_id , err ) = self.zimbra_retry_loop_(
                        lambda : self.handle_source_( src , eppn ,
                                    usrc[ src_name ] , root )
                    )
                if err is None:
                    import_results[ src_name ] = src_id
                    if src.needs_postprocess( ):
                        needs_postproc.append( src_name )
                else:
                    Logging( 'cal' ).error( ( 'Erreur Zimbra lors de la mise à '
                            + 'jour du calendrier {} pour {}: {}' ).format(
                                    src_name , eppn , str( err ) ) )

            # Postprocessing
            if needs_postproc:
                ( junk , err ) = self.zimbra_retry_loop_(
                        lambda : self.handle_postproc_( eppn , needs_postproc ,
                                                        import_results )
                    )
                if err is not None:
                    Logging( 'cal' ).error( ( 'Erreur Zimbra après la mise à '
                            + 'jour du calendrier {} pour {}: {}' ).format(
                                    src_name , eppn , str( err ) ) )

        try:
            self.zimbra_.terminate( )
        except ZimbraError as e:
            pass

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

    def get_user_folders_( self , eppn ):
        """
        Récupère les informations complètes concernant les dossiers d'un
        utilisateur.

        :param eppn: l'EPPN de l'utilisateur
        :return: les informations des dossiers

        :raises ZimbraError: une erreur de communication s'est produite
        """
        self.zimbra_.set_user( eppn )
        return self.zimbra_.get_folder( )

    def handle_source_( self , source , eppn , privilege , root ):
        """
        Effectue les opérations requises pour la synchronisation d'un compte
        utilisateur par rapport à une source de données.

        :param source: la source de données
        :param eppn: l'EPPN de l'utilisateur
        :param privilege: le niveau de privilèges de l'utilisateur sur cette \
                source (none, ro, rw)
        :param root: le dossier racine du compte utilisateur

        :return: l'identifiant (champ Zimbra "id") du dossier correspondant \
                à la source, ou None si aucun dossier n'a été créé

        :raises ZimbraError: une erreur de communication s'est produite
        """
        self.zimbra_.set_user( eppn )
        imp_folders = source.find_user_folder( eppn , root )
        if privilege == 'none':
            self.remove_import_( source , eppn , [
                    f[ 1 ][ 'id' ] for f in imp_folders.values( ) ] )
            return None
        else:
            return self.add_import_( source , eppn , privilege , root ,
                                     imp_folders )

    def remove_import_( self , source , eppn , imp_folders ):
        """
        Supprime des dossiers importés depuis une source.

        :param source: la source de données
        :param eppn: l'EPPN de l'utilisateur
        :param imp_folders: les identifiants des dossiers à supprimer
        """
        for folder in imp_folders:
            Logging( 'cal' ).info(
                    'Suppression du dossier #{} de {} (source {})'.format(
                            folder , eppn , source.identifier ) )
            self.zimbra_.delete_folder( folder )

    def add_import_( self , source , eppn , privilege , root , imp_folders ):
        """
        Ajoute ou recrée si nécessaire un calendrier importé à partir d'une
        source de données.

        :param source: la source de données
        :param eppn: l'EPPN de l'utilisateur
        :param privilege: le niveau de privilèges de l'utilisateur sur cette \
                source (none, ro, rw)
        :param root: le dossier racine du compte utilisateur
        :param imp_folders: les dossiers existants qui pourraient correspondre \
                à la source de données
        """

        # Aucune correspondance -> on crée
        if not imp_folders:
            Logging( 'cal' ).info( 'Création du calendrier {} pour {}'.format(
                    source.identifier , eppn ) )
            n = source.folder_name( root )
            folder_info = source.create_user_folder( self.zimbra_ , eppn ,
                                                     root[ 'id' ] , n )
            return folder_info[ 'id' ]

        # Le calendrier existe et n'est pas à la poubelle -> on ne touche à rien
        if False in ( v[ 0 ] for v in imp_folders.values( ) ):
            results = tuple( v[ 1 ]
                    for v in imp_folders.values( )
                    if not v[ 0 ] )
            return results[ 0 ][ 'id' ]

        # Le calendrier est à la poubelle, on le restaure
        Logging( 'cal' ).info( 'Calendrier {} pour {}: dans la poubelle'.format(
                source.identifier , eppn ) )
        ( restore , *junk ) = imp_folders.keys( )
        f_data = imp_folders[ restore ][ 1 ]
        n = source.folder_name( root )
        if n != f_data[ 'name' ]:
            trash = self.find_trash_( root )
            n2 = source.folder_name( root , trash )
            if n2 != n:
                self.zimbra_.rename_folder( f_data[ 'id' ] , n2 )
                self.zimbra_.move_folder( f_data[ 'id' ] , root[ 'id' ] )
                self.zimbra_.rename_folder( f_data[ 'id' ] , n )
                return
            self.zimbra_.rename_folder( f_data[ 'id' ] , n )
        self.zimbra_.move_folder( f_data[ 'id' ] , root[ 'id' ] )
        return f_data[ 'id' ]

    def find_trash_( self , root ):
        """
        Trouve le dossier /Trash dans les dossiers de la racine.

        :param root: la racine des dossiers
        :return: les données du dossier /Trash
        :raises RuntimeError: si le dossier /Trash n'existe pas
        """
        for f in root[ 'folder' ]:
            if f[ 'absFolderPath' ] == '/Trash':
                return f
        raise RuntimeError( 'Dossier /Trash non trouvé' )

    def handle_postproc_( self , eppn , sources , folder_ids ):
        """
        Effectue le postprocessing après importation de calendriers.

        :param eppn: l'EPPN du compte
        :param sources: la liste des noms des sources pour lesquelles une \
                opération de postprocessing est nécessaire
        :param folder_ids: un dictionnaire indiquant, pour chaque source, \
                l'identifiant Zimbra du dossier correspondant
        """
        postproc_data = {}
        self.zimbra_.set_user( eppn )
        for src_name in sources:
            self.sources_[ src_name ].start_postproc_for( self.zimbra_ ,
                    eppn , postproc_data )
        for src_name in sources:
            self.sources_[ src_name ].postprocess( self.zimbra_ ,
                    eppn , postproc_data , folder_ids[ src_name ] )
        for src_name in sources:
            self.sources_[ src_name ].end_postproc_for( self.zimbra_ ,
                    eppn , postproc_data )

    def zimbra_retry_loop_( self , action ):
        """
        Exécute une opération utilisant l'API Zimbra, en ré-essayant plusieurs
        fois si des erreurs d'expiration de l'authentification se produisent.

        :param action: une fonction sans paramètres à exécuter
        :return: un tuple qui contient la valeur de retour de la fonction et \
                l'erreur Zimbra qui s'est produite (ou None s'il n'y a pas \
                eu d'erreur)
        """
        assert callable( action )
        attempts = 0
        exit_loop = False
        log_err = None
        rv = None
        while attempts < self.max_attempts_ and not exit_loop:
            try:
                rv = action( )
            except ZimbraConnectionError as e:
                log_err = e
                exit_loop = True
            except ZimbraRequestError as e:
                is_exp = str( e.fault_code ) == 'service.AUTH_EXPIRED'
                if not is_exp or attempts == self.max_attempts_ - 1:
                    exit_loop = True
                    log_err = e
            else:
                exit_loop = True
            attempts += 1
        if log_err is not None:
            try:
                self.zimbra_.terminate( )
            except ZimbraError as e:
                pass
        return ( rv , log_err )
