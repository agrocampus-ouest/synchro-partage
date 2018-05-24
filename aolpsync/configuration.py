from .logging import Logging
from .data import SyncAccount
from .rules import Rule , RuleError
from .utils import FatalError


#-------------------------------------------------------------------------------


class CfgOverride:
    """
    Classe utilisée pour représenter les options de ligne de commande permettant
    d'écraser des éléments de la configuration.
    """

    def __init__( self , section , name , value = None , undef = False ):
        """
        Initialise cette entrée. Toutes les entrées d'écrasement possèdent une
        section et un nom. Les entrées de modification ont en plus une valeur,
        et les entrées de suppression un drapeau de suppression.

        :param str section: le nom de la section
        :param str name: le nom de l'option
        :param str value: la valeur de l'option; paramètre ne devant être \
                présent que s'il s'agit d'un remplacement de valeur
        :param bool undef: doit-on supprimer l'option?
        """
        self.section = section
        self.name = name
        self.value = value
        self.undef = undef

    @property
    def key( self ):
        """
        Génère une clé de dictionnaire pour cette entrée

        :return: la clé sous la forme d'un tuple (section,nom)
        """
        return ( self.section , self.name )

    @property
    def action( self ):
        """
        Détermine le type d'action représentée en vérifiant la valeur et le
        drapeau de suppression.

        :return: le type d'action: 's' pour remplacement, 'u' pour suppression \
                ou 'd' pour définition
        """
        if self.value is not None:
            return 's'
        return 'u' if self.undef else 'd'

    def apply( self , cfg ):
        """
        Applique l'action à une configuration.

        :param configparser.ConfigParser cfg: l'instance de configuration à \
                mettre à jour
        """
        a = self.action
        if a == 'u':
            if cfg.has_section( self.section ):
                cfg.remove_option( self.section , self.name )
            return
        if not cfg.has_section( self.section ):
            cfg.add_section( self.section )
        if a == 'd' and cfg.has_option( self.section , self.name ):
            return
        cfg.set( self.section , self.name ,
                self.value if self.action == 's' else '' )


#-------------------------------------------------------------------------------


class Config:
    """
    Cette classe permet de charger, de vérifier et d'utiliser la configuration
    du script.
    """

    # Répertoire contenant les fichiers de configuration
    CONFIG_DIR = '.'

    # Nom de fichier par défaut; sera remplacé lors de l'initialisation du
    # programme afin d'utiliser le chemin du script.
    FILE_NAME = 'partage-sync.ini'

    def __init__( self , overrides = () ):
        """
        Charge le fichier de configuration et vérifie que toutes les entrées
        requises sont présentes.

        :raises FatalError: le fichier n'existe pas, ou la configuration est \
                incomplète
        """
        from configparser import ConfigParser
        config = ConfigParser( allow_no_value = True )
        try:
            config.read_file( open( Config.FILE_NAME ) )
        except FileNotFoundError:
            raise FatalError(
                    'Fichier de configuration "{}" non trouvé'.format(
                        Config.FILE_NAME )
                )
        for co in overrides:
            co.apply( config )
        param_checks = (
            ( 'ldap' , (
                'host' , 'user' , 'pass' ,
                'people-dn' , 'groups-dn' ,
                'mail-domain' , 'eppn-domain'
            ) ) ,
            ( 'db' , (
                'path' ,
            ) ) ,
            ( 'bss' , (
                'domain' , 'token' , 'default-cos' ,
                'deletion-threshold'
            ) ) ,
            ( 'ldap-people-classes' , ( ) ) ,
        )
        for section in param_checks:
            ( sn , items ) = section
            if sn not in config:
                raise FatalError( 'Section {} manquante'.format( sn ) )
            for item in items:
                if item in config[ sn ]:
                    continue
                raise FatalError( 'Section {}: élément {} manquant'.format(
                    sn , item ) )
        self.cfg_ = config

    def parse_cos_rules( self ):
        rules = dict( )
        if 'cos-rules' not in self.cfg_:
            return rules
        SyncAccount( self ) # Initialise la configuration des comptes
        section = self.cfg_[ 'cos-rules' ]
        try:
            for r in section:
                rules[ r ] = Rule( r , section[ r ] )
        except RuleError as e:
            Logging( 'cfg' ).critical( str( e ) )
            raise FatalError( 'Erreur dans les règles d\'attribution de CoS' )
        return rules

    def check_coses( self , coses ):
        to_check = [ self.get( 'bss' , 'default-cos' ) ] + self.get_list(
                'cos-rules' , [] )
        for n in to_check:
            if n not in coses:
                raise FatalError(
                    'Classe de service {} non trouvée sur le serveur'
                        .format( n ) )

    #---------------------------------------------------------------------------

    def get( self , section , value , default = None ):
        """
        Lit une valeur depuis la configuration.

        :param str section: le nom de la section de configuration
        :param str value: le nom du paramètre de configuration
        :param default: la valeur par défaut à renvoyer si la valeur ne peut \
                être trouvée

        :return: la valeur de l'entrée, ou la valeur par défaut
        """
        if section not in self.cfg_:
            return default
        return self.cfg_[ section ].get( value , default )

    def has_flag( self , section , name ):
        """
        Vérifie si un drapeau est actif. Dans le fichier de configuration, un
        drapeau est défini par une entrée sans valeur, qui sera présente ou non.

        :param str section: le nom de la section
        :param str name: le nom du drapeau

        :return: True si le drapeau est présent, False s'il ne l'est pas.
        """
        if section not in self.cfg_:
            return False
        return name in self.cfg_[ section ]

    def get_list( self , section , default = None ):
        """
        Lit les clés d'une section de configuration, sous la forme d'une liste.

        :param str section: le nom de la section à transformer en liste
        :param default: la valeur par défaut; si ce paramètre est None et que \
                la section n'existe pas, une erreur sera provoquée

        :return: la liste des clés de la section de configuration spécifiée, \
                ou la valeur par défaut si elle est définie.

        :raises FatalError: la section n'existe pas et aucune valeur par \
                défaut n'a été spécifiée.
        """
        if section in self.cfg_:
            return [ k for k in self.cfg_[ section ] ]
        if default is None:
            raise FatalError( 'Section {} vide'.format( section ) )
        return default

    def get_section( self , section , allow_empty = False ):
        """
        Lit une section de configuration sous la forme d'un dictionnaire.

        :param str section: le nom de la section de configuration devant être \
                lue
        :param bool allow_empty: ce paramètre détermine ce qu'il se passe si \
                la section n'existe pas. S'il est à True, un dictionnaire vide \
                sera renvoyé; sinon, une exception sera levée.

        :return: le dictionnaire des entrées de la section, ou un dictionnaire \
                vide si la section n'existe pas mais que le paramètre \
                allow_empty a la valeur True.

        :raises FatalError: la section n'existe pas et allow_empty a la valeur \
                False
        """
        if section in self.cfg_:
            s = self.cfg_[ section ]
            return { k : s[ k ] for k in s }
        if allow_empty:
            return dict()
        raise FatalError( 'Section {} non trouvée'.format( section ) )

    #---------------------------------------------------------------------------

    def ldap_server( self ):
        """
        Crée l'instance qui représente le serveur LDAP à partir de la
        configuration.

        :return: une instance de ldap3.Server configurée
        """
        import ldap3
        lc = self.cfg_[ 'ldap' ]
        return ldap3.Server( lc[ 'host' ] ,
                port = int( lc.get( 'port' , 636 ) ) ,
                use_ssl = bool( int( lc.get( 'ssl' , '1' ) ) ) ,
                get_info = 'ALL' )

    def ldap_connection( self ):
        """
        Établit la connexion au serveur LDAP, en utilisant la configuration.

        :return: la connexion
        """
        import ldap3
        server = self.ldap_server( )
        Logging( 'ldap' ).info( 'Connexion au serveur LDAP: ' + str( server ) )
        lc = self.cfg_[ 'ldap' ]
        return ldap3.Connection( self.ldap_server( ) ,
                lc[ 'user' ] , lc[ 'pass' ] , auto_bind = True )

    def lmdb_env( self ):
        """
        Initialise l'environnement pour LightningDB à partir de la
        configuration.

        :return: l'environnement LightningDB
        """
        import lmdb
        db = self.get( 'db' , 'path' )
        Logging( 'db' ).info( 'Initialisation base de données: ' + db )
        return lmdb.Environment( subdir = True , path = db , mode = 0o700 ,
                map_size = int( self.get( 'db' , 'map-size' ,
                    str( 200 * 1024 * 1024 ) ) ) )

    def bss_connection( self ):
        """
        Configure la connexion à l'API BSS.

        :raises FatalError: la connexion ou l'authentification ont échoué
        """
        from lib_Partage_BSS.services.BSSConnexionService import BSSConnexion
        from lib_Partage_BSS.exceptions import BSSConnexionException
        dom = self.get( 'bss' , 'domain' )
        Logging( 'bss' ).info( 'Connexion à l\'API BSS, domaine: ' + dom )
        cn = BSSConnexion()
        cn.setDomainKey({ dom : self.get( 'bss' , 'token' ) })
        try:
            cn.token( dom )
        except BSSConnexionException as e:
            Logging( 'bss' ).error( "Connexion BSS - erreur: " + str( e ) )
            raise FatalError( "Échec de la connexion au service BSS" )

    def alias_commands( self ):
        """
        Accède à une instance (unique) de gestion des commandes d'obtention
        d'aliases supplémentaires. Si l'instance n'avait pas encore été créée,
        elle le sera.

        :return: l'instance de gestion des commandes d'obtention d'aliases
        """
        if not hasattr( self , 'alias_commands_' ):
            from .aliases import AliasCommands
            self.alias_commands_ = AliasCommands( self )
        return self.alias_commands_
