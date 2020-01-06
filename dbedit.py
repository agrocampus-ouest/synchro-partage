#!/usr/bin/python3

from aolpsync import *


#-------------------------------------------------------------------------------


class DbEditor( ProcessSkeleton ):
    """
    Implémentation de l'outil de dépannage permettant de modifier directement
    la base de données de synchronisation.
    """

    def cli_description( self ):
        return '''Outil de dépannage permettant de modifier la base de données
                  de synchronisation. Il est possible d'éditer les champs
                  manuellement, ou de les copier depuis l'annuaire LDAP ou le
                  serveur Partage.'''

    def cli_register_arguments( self , parser ):
        parser.add_argument( 'eppn' , action = 'store' ,
                help = '''L'EPPN (ou l'UID) de l'utilisateur à éditer.''' )
        parser.add_argument( 'field' , action = 'store' ,
                help = '''Le nom du champ à modifier.''' )
        parser.add_argument( 'value' , action = 'store' , nargs = '*' ,
                help = '''La ou les valeurs désirées pour le champ.''' )

        parser.add_argument( '--copy-from' ,
                action = 'store' , metavar = 'source' ,
                choices = ( 'bss' , 'ldap' ) ,
                help = '''Copie les informations depuis une source (bss
                          ou ldap).''' )
        parser.add_argument( '--force' , '-f' ,
                action = 'store_true' ,
                help = '''Ne demande pas confirmation.''' )

    #---------------------------------------------------------------------------

    def __init__( self ):
        ProcessSkeleton.__init__( self ,
                require_ldap = False ,
                require_bss = False ,
                require_cos = False )

    def preinit( self ):
        """
        Vérifie les arguments et demande les connexions nécessaires (par exemple
        connexion LDAP si l'on veut copier l'enregistrement LDAP).
        """
        SyncAccount( self.cfg ) # On s'assure que la liste des champs est prête

        eppn = self.arguments.eppn
        eppn_domain = self.cfg.get( 'ldap' , 'eppn-domain' )
        self.target = eppn if '@' in eppn else '{}@{}'.format( eppn ,
                                                               eppn_domain )

        f = self.arguments.field
        if f not in SyncAccount.STORAGE:
            raise FatalError( 'Champ {} inconnu'.format( f ) )
        if f == 'eppn':
            raise FatalError( 'Impossible de modifier l\'EPPN' )
        self.field = f

        src = self.arguments.copy_from
        self.copy_from = src
        if src is None:
            return

        if self.arguments.value:
            raise FatalError( 'Valeurs et copie ne sont pas compatibles' )
        if src == 'bss' and not ( f in SyncAccount.BSS.values( )
                or f in ( 'mail' , 'aliases' , 'markedForDeletion' , 'cos' ) ):
            raise FatalError(
                    'Champ {} non supporté pour copie depuis Partage'.format(
                        f ) )

        if src == 'bss':
            self.requires[ 'bss' ] = True
            self.requires[ 'cos' ] = True
            return

        # On génère la requête pour le LDAP
        self.requires[ 'ldap' ] = True
        if eppn.endswith( '@' + eppn_domain ):
            uid = eppn[ :-( len( eppn_domain ) + 1 ) ]
        elif '@' not in eppn:
            uid = eppn
        else:
            uid = None
        if uid is None:
            self.ldap_query = (
                    '(eduPersonPrincipalName={})'.format( self.target )
                )
        else:
            self.ldap_query = (
                    '(|(uid={})(eduPersonPrincipalName={}))'.format(
                        uid , self.target )
                )

    #---------------------------------------------------------------------------

    def get_from_bss( self ):
        """
        Lit les données d'un compte depuis l'API Partage.

        :return: le compte lu depuis Partage
        :raise FatalError: la connexion à Partage a échoué, ou le compte ne \
                figure pas sur le serveur
        """
        bss_domain = self.cfg.get( 'bss' , 'domain' )
        eppn = self.target
        retr = BSSAction( BSSQuery( 'getAllAccounts' ) ,
                bss_domain , offset = 0 , limit = 100 ,
                ldapQuery = '(carLicense={})'.format( eppn ) )
        if not retr:
            raise FatalError( 'Impossible de rechercher un compte Partage' )

        obtained = retr.get( )
        if not obtained:
            raise FatalError( 'Compte {} absent de Partage'.format( eppn ) )

        assert len( obtained ) == 1
        Logging( 'edit' ).debug( 'Compte {} présent sur le BSS'.format( eppn ) )

        mail = obtained[ 0 ].name
        qr = BSSAction( BSSQuery( 'getAccount' ) , mail )
        if not qr:
            raise FatalError(
                    'Échec de la lecture du compte {}'.format( eppn ) )

        account = SyncAccount( self.cfg )
        try:
            account.from_bss_account( qr.get( ) , self.reverse_coses )
        except AccountStateError as e:
            raise FatalError( 'Échec de la lecture du compte {}: {}'.format(
                    eppn , str( e ) ) )
        assert account.eppn == eppn
        return account


    def get_from_ldap( self ):
        """
        Lit les données d'un compte depuis l'annuaire LDAP

        :return: le compte importé depuis l'annuaire
        :raise FatalError: le compte n'existe pas dans l'annuaire
        """
        if self.target in self.ldap_accounts:
            return self.ldap_accounts[ self.target ]
        raise FatalError( 'Compte {} non trouvé dans le LDAP'.format(
                self.target ) )

    def get_values_ext( self ):
        """
        Récupère les données depuis une source externe (API Partage ou annuaire
        LDAP).

        :return: la ou les valeurs correspondant au champ
        """
        if self.copy_from == 'ldap':
            account = self.get_from_ldap( )
        else:
            assert self.copy_from == 'bss'
            account = self.get_from_bss( )
        return getattr( account , self.field )

    def get_values( self ):
        """
        Récupère les nouvelles valeurs du champ à modifier, soit depuis la ligne
        de commande, soit depuis une source de données externe.

        :return: la ou les valeurs
        """
        if self.copy_from is not None:
            values = self.get_values_ext( )
        else:
            values = self.arguments.value
            if values:
                values = set( values ) if len( values ) > 1 else values[ 0 ]
            else:
                values = None
        Logging( 'edit' ).debug( 'Valeurs: {}'.format( repr( values ) ) )
        return values

    def get_print_value( self , v ):
        """
        Génère la chaîne à afficher pour prévisualiser les modifications à
        effectuer.

        :param v: la ou les valeurs devant être affichée(s)
        :return: la chaîne d'affichage
        """
        if v is None:
            return '(pas de valeur)'
        elif isinstance( v , list ) or isinstance( v , set ):
            return '\n  {}'.format( '\n  '.join( v ) )
        return '\n  {}'.format( v )

    def confirm_change( self ):
        """
        Affiche la liste des modifications et demande confirmation à
        l'utilisateur. Si l'argument '-f' a été passé, les valeurs seront
        affichées mais aucune confirmation ne sera demandée.

        :return: True si la mise à jour doit être effectuée, False si elle \
                doit être annulée.
        """
        from aolpsync.utils import multivalued_check_equals as mce
        initial = getattr( self.db_accounts[ self.target ] , self.field )
        if mce( self.values , initial ):
            print( 'Aucune modification à effectuer' )
            return False

        print( )
        print( 'Modification du compte {}, champ {}'.format( self.target ,
                    self.field ) )
        print( )
        print( 'Valeur initiale: {}'.format( self.get_print_value( initial ) ) )
        print( )
        print( 'Nouvelle valeur: {}'.format( self.get_print_value(
                self.values ) ) )
        print( )

        if self.arguments.force:
            return True

        ok = input( 'Effectuer cette modification [o/N] ? ' )
        ok = ok.strip( ).lower( ) == 'o'
        if not ok:
            print( "Modification annulée" )
        return ok

    def write_log( self ):
        """
        Écrit l'entrée de journal correspondant aux modifications demandées.
        """
        Logging( 'edit' ).info( 'Modification du compte {}, champ {}'.format(
                self.target , self.field ) )
        Logging( 'edit' ).info( 'Valeur initiale: {}'.format(
                repr( getattr( self.db_accounts[ self.target ] , self.field ) )
            ) )
        Logging( 'edit' ).info( 'Nouvelle valeur: {}'.format(
                repr( self.values ) ) )

    def process( self ):
        """
        Récupère les nouvelles valeurs, demande éventuellement confirmation,
        puis effectue les modifications sur l'enregistrement en base.
        """
        if self.target not in self.db_accounts:
            raise FatalError( 'Compte {} absent de la base'.format(
                    self.target ) )
        self.values = self.get_values( )
        if not self.confirm_change( ):
            return
        self.write_log( )
        account = self.db_accounts[ self.target ]
        setattr( account , self.field , self.values )
        self.save_account( account )
        print( "Modification effectuée" )



#-------------------------------------------------------------------------------


try:
    DbEditor( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    print( "ERREUR: {}".format( str( e ) ) )
    sys.exit( 1 )


