from .configuration import Config , CfgOverride
from .data import SyncAccount
from .logging import Logging
from .data import LDAPData , AliasesMap
from .rules import Rule , RuleError
from .utils import BSSAction , FatalError


class ProcessSkeleton:
    """
    Classe servant de 'squelette' aux processeurs spécifiques. Contient un
    certain nombre de méthodes communes.
    """

    def parse_arguments( self ):
        """
        Configure le lecteur d'arguments puis l'exécute. Les valeurs lues seront
        stockées dans self.arguments.

        Par défaut:

        * la description est lue grâce à la méthode cli_description();

        * un éventuel épilogue est lu grâce à la méthode cli_epilog();

        * un argument '-C' permettant de changer le répertoire de configuration
        est ajouté;

        * un argument '-S' permettant d'écraser la valeur d'une option de
        configuration est ajouté;

        * un argument '-D' permettant de définir un drapeau de configuration est
        ajouté;

        * un argument '-U' permettant de supprimer un drapeau ou une option de
        configuration est ajouté.

        D'autres arguments peuvent être ajoutés en surchargeant la méthode
        cli_register_arguments().
        """
        import argparse , os.path , sys
        parser = argparse.ArgumentParser(
                description = self.cli_description( ) ,
                epilog = self.cli_epilog( ) )

        cfg_group = parser.add_argument_group( 'Configuration' )
        own_path = os.path.dirname( os.path.realpath( sys.argv[ 0 ] ) )
        cfg_group.add_argument( '-C' , '--config-dir' ,
                action = 'store' , nargs = 1 , type = str ,
                help = 'Répertoire des fichiers de configuration' ,
                metavar = ( 'directory' , ) ,
                default = own_path )
        cfg_group.add_argument( '-S' , '--cfg-set' ,
                action = 'append' , nargs = 3 ,
                metavar = ( 'section' , 'name' , 'value' ) ,
                help = 'Écrase la valeur d\'une option de configuration.' )
        cfg_group.add_argument( '-D' , '--cfg-define' ,
                action = 'append' , nargs = 2 ,
                metavar = ( 'section' , 'name' ) ,
                help = 'Définit un drapeau de configuration.' )
        cfg_group.add_argument( '-U' , '--cfg-undefine' ,
                action = 'append' , nargs = 2 ,
                metavar = ( 'section' , 'name' ) ,
                help = 'Supprime une option ou un drapeau de configuration.' )

        self.cli_register_arguments( parser )
        self.arguments = parser.parse_args( )

    def cli_description( self ):
        """
        Cette méthode doit être surchargée afin de renvoyer la description
        correspondant à l'outil implémenté par cette classe.

        :return: le texte de la description
        """
        raise NotImplementedError

    def cli_epilog( self ):
        """
        Cette méthode peut être surchargée afin de renvoyer un épilogue à
        afficher lorsque l'on demande l'aide en ligne de commande.

        :return: le texte de l'épilogue
        """
        return None

    def cli_register_arguments( self , parser ):
        """
        Cette méthode peut être surchargée afin d'ajouter des arguments au
        lecteur de ligne de commande.

        :param argparse.ArgumentParser parser: le lecteur de ligne de commande
        """
        pass

    def get_cfg_overrides( self ):
        """
        Génère une liste d'objets représentant des surcharges de configuration à
        partir des arguments -S/-U/-D du programme.

        :raises FatalError: si plusieurs arguments font référence à la même \
                option

        :return: la liste des surcharges à appliquer
        """
        col = []
        if self.arguments.cfg_set:
            for co_set in self.arguments.cfg_set:
                col.append( CfgOverride( *co_set ) )
        if self.arguments.cfg_define:
            for co_def in self.arguments.cfg_define:
                col.append( CfgOverride( *co_def ) )
        if self.arguments.cfg_undefine:
            for co_undef in self.arguments.cfg_undefine:
                col.append( CfgOverride( *co_undef , undef = True ) )
        cod = {}
        for co in col:
            if co.key in cod:
                raise FatalError(
                        ( 'Plusieurs options affectent la variable de '
                        + 'configuration "{}" de la section "{}".' ).format(
                            co.name , co.section ) )
            cod[ co.key ] = co
        return cod.values( )

    def load_cos( self ):
        """
        Charge les classes de services depuis le serveur BSS et construit un
        dictionnaire associant les noms de classes à leurs identifiants.
        """
        import lib_Partage_BSS.services.COSService as bsssc
        import lib_Partage_BSS.exceptions as bsse
        try:
            coses = bsssc.getAllCOS( self.cfg.get( 'bss' , 'domain' ) )
        except ( bsse.NameException , bsse.DomainException ,
                bsse.ServiceException ) as error:
            Logging( 'bss' ).error( "Erreur lecture CoS: {}".format(
                    repr( error ) ) )
            raise FatalError( 'Impossible de lire la liste des CoS' )
        self.coses = { c.cn : c.id for c in coses }
        Logging( 'bss' ).debug( 'Classes de service: {}'.format(
                ', '.join([ '{} (ID {})'.format( str( c.cn ) , str( c.id ) )
                        for c in coses ]) ) )

    def load_from_ldap( self ):
        """
        Charge les données depuis le serveur LDAP. Les comptes et groupes
        seront chargés, puis la liste des aliases sera établie. Les comptes ne
        devant pas être synchronisés car ils correspondent à un alias seront
        ôtés de la liste. Enfin, si le domaine BSS et le domaine mail sont
        différents (parce que l'on est sur le serveur de test, par exemple),
        corrige toutes les adresses.
        """
        ldap_data = LDAPData( self.cfg )
        aliases = AliasesMap( self.cfg , ldap_data.accounts )
        ldap_data.remove_alias_accounts( )
        ldap_data.set_aliases( aliases )
        ldap_data.fix_mail_domain( self.cfg )
        ldap_data.clear_empty_sets( )

        # Sélection des comptes
        try:
            match_rule = Rule( 'account selection' ,
                    self.cfg.get( 'ldap' , 'match-rule' , '(true)' ) )
        except RuleError as e:
            Logging( 'cfg' ).critical( str( e ) )
            raise FatalError( 'Erreur dans la règle de sélection des comptes' )
        self.ldap_accounts = {}
        for eppn in ldap_data.accounts:
            a = ldap_data.accounts[ eppn ]
            if match_rule.check( a ):
                self.ldap_accounts[ eppn ] = a
            else:
                Logging( 'ldap' ).debug( 'Compte {} éliminé via règle'.format(
                        eppn ) )

    def load_db_accounts( self , txn ):
        """
        Lit l'intégralité des comptes depuis la base de données.

        :param txn: la transaction LightningDB
        :return: la liste des comptes lus depuis la base
        """
        def d_( x ): return x.decode( 'utf-8' )
        def na_( x ): return SyncAccount( self.cfg ).from_json( d_( x ) )
        with txn.cursor( ) as cursor:
            acc = { d_( a[ 0 ] ) : na_( a[ 1 ] ) for a in cursor }
        for x in acc.values( ): x.clear_empty_sets( )
        Logging( 'db' ).info( '{} comptes chargés depuis la BDD'.format(
                len( acc ) ) )
        self.db_accounts = acc

    def save_account( self , account ):
        """
        Sauvegarde les informations d'un compte dans la base de données. Si le
        drapeau de simulation est présent dans la configuration, l'opération ne
        sera pas réellement effectuée.

        :param SyncAccount account: le compte à sauvegarder
        """
        sim = self.cfg.has_flag( 'bss' , 'simulate' )
        mode = 'simulée ' if sim else ''
        Logging( 'db' ).debug( 'Sauvegarde {}du compte {} (mail {})'.format(
                mode , account.eppn, account.mail ) )
        if sim: return

        db_key = account.eppn.encode( 'utf-8' )
        account.clear_empty_sets( )
        with self.db.begin( write = True ) as txn:
            txn.put( db_key , account.to_json( ).encode( 'utf-8' ) )

    def remove_account( self , account ):
        """
        Supprime l'enregistrement d'un compte de la base de données.

        :param SyncAccount account: le compte à supprimer
        """
        sim = self.cfg.has_flag( 'bss' , 'simulate' )
        mode = 'simulée ' if sim else ''
        Logging( 'db' ).debug( 'Suppression {}du compte {} (mail {})'.format(
                mode , account.eppn, account.mail ) )
        if not sim:
            with self.db.begin( write = True ) as txn:
                txn.pop( account.eppn.encode( 'utf-8' ) )

    def preinit( self ):
        """
        Cette méthode peut être surchargée pour implémenter toute action
        nécessaire en préinitialisation. Elle sera appelée après le chargement
        de la configuration, mais avant l'initialisation des connexions.
        """
        pass

    def init( self ):
        """
        Cette méthode peut être surchargée pour implémenter des actions à
        effectuer pour compléter l'initialisation. Elle est appelée après
        l'établissement des connexions, mais avant que la base de données ne
        soit lue.
        """
        pass

    def process( self ):
        """
        Cette méthode doit être surchargée afin d'implémenter l'action à
        effectuer.
        """
        raise NotImplementedError

    def __init__( self ,
            require_bss = True ,
            require_cos = True ,
            require_ldap = True ):
        """
        Initialise le processeur de données. Pour cela, la configuration est
        chargée, puis les diverses données sont lues, en fonction des
        paramètres. Enfin, la méthode process() est appelée.

        :param bool require_bss: la connexion à Partage doit-elle être établie?
        :param bool require_cos: la liste des classes de service doit-elle \
                être chargée? (il faut que require_bss soit vrai aussi)
        :param bool require_ldap: les informations du LDAP doivent-elle être \
                chargées?
        """
        self.parse_arguments( )

        # Initialisation des chemins de configuration
        from os.path import join as opjoin
        cd = self.arguments.config_dir
        Logging.FILE_NAME = opjoin( cd , 'partage-sync-logging.ini' )
        Config.FILE_NAME = opjoin( cd , 'partage-sync.ini' )

        self.cfg = Config( self.get_cfg_overrides( ) )
        self.preinit( )

        if require_bss:
            self.cfg.bss_connection( )
            if require_cos:
                self.load_cos( )
                self.cfg.check_coses( self.coses )
        else:
            assert not require_cos

        if require_ldap:
            self.load_from_ldap( )

        if self.cfg.has_flag( 'bss' , 'simulate' ):
            Logging( ).warn( 'Mode simulation activé' )
            BSSAction.SIMULATE = True

        self.init( )
        with self.cfg.lmdb_env( ) as db:
            self.db = db
            with db.begin( write = False ) as txn:
                self.load_db_accounts( txn )
            self.process( )

