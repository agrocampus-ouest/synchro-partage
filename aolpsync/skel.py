from .configuration import Config
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
        self.cfg = Config( )
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

