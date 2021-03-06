#!/usr/bin/python3

from aolpsync import *


class DiffItem:
    """
    Cette classe représente une entrée de données pour un EPPN et un champ. Elle
    stocke, compare et peut afficher les valeurs.
    """

    COLORS = ( '\033[32m' , '\033[33m' , '\033[31m' , '' )
    DOMAIN = None

    class Unknown:
        """
        Classe utilisée comme marqueur pour les champs dont la valeur ne peut
        être connue (par exemple, les entrées venant du BSS ne peuvent avoir
        d'UID).
        """
        pass

    class NoAccount:
        """
        Classe utilisée comme marqueur pour les champs d'un enregistrement
        manquant.
        """
        pass

    def __init__( self , eppn , field ):
        """
        Initialise une entrée pour un EPPN et un champ. Les valeurs seront
        initialisées à "inconnu".

        :param str eppn: l'EPPN de l'utilisateur pour lequel on examine les \
                données
        :param str field: le nom du champ
        """
        self.eppn = eppn
        self.field = field
        self.bss_equals = None
        self.values_ = {
            'ldap': DiffItem.Unknown ,
            'db': DiffItem.Unknown ,
            'bss': DiffItem.Unknown ,
        }

    def set_value( self , source , value ):
        """
        Initialise une valeur pour l'une des sources de données. Si la valeur
        est une chaîne se terminant par le domaine BSS, ou un ensemble de
        chaînes se terminant par le domaine BSS, le nom de domaine sera remplacé
        par '[]'.

        :param source: la source de données; doit être 'ldap', 'db' ou 'bss'
        :param value: la valeur du champ, ou DiffItem.NoAccount si le compte \
                correspondant n'existe pas
        """
        assert self.values_[ source ] is not None
        assert self.values_[ source ] == DiffItem.Unknown
        rep_mail = lambda s : ( '{}@[]'.format( s[ :-len( DiffItem.DOMAIN ) ] )
                                    if s.endswith( DiffItem.DOMAIN )
                                    else s )

        if isinstance( value , set ) or isinstance( value , list ):
            value = sorted( rep_mail( v ) for v in value )
        elif isinstance( value , str ):
            value = rep_mail( value )
        self.values_[ source ] = value

    def display_widths( self ):
        """
        Calcule et retourne les largeurs requises pour l'affichage des
        différentes colonnes.

        :return: un tuple contenant les largeurs pour la colonne LDAP, base de \
                données et BSS
        """
        n_lines = self.get_lines_( )
        return tuple( self.get_width_( source , n_lines )
                for source in ( 'ldap' , 'db' , 'bss' ) )

    def get_width_( self , source , n_lines ):
        """
        Calcule la largeur requise pour l'affichage des informations concernant
        une source de données.

        :param source: la source de données ('ldap', 'db' ou 'bss')
        :param n_lines: le nombre de lignes qui seront affichées

        :return: la largeur requise pour cette source
        """
        return max( len( self.get_text( source , i ) )
                        for i in range( 0 , n_lines ) )

    def get_text( self , source , line ):
        """
        Retourne le texte à afficher pour une ligne et source de données.

        :param source: la source de données ('ldap', 'db' ou 'bss')
        :param line: le numéro de la ligne d'affichage pour laquelle on veut \
                récupérer le texte.

        :return: le texte à afficher
        """
        v = self.values_[ source ]
        if ( v is None or isinstance( v , str )
                    or v == DiffItem.NoAccount
                    or v == DiffItem.Unknown ):
            if line != 0:
                return ''
            if v is None:
                return 'Non renseigné'
            if isinstance( v , str ):
                return '«{}»'.format( v )
            if v == DiffItem.NoAccount:
                return 'Compte absent'
            return 'N/A'

        if line >= len( v ):
            return ''
        v = tuple( v )
        return '{:02d}. «{}»'.format( line + 1 , v[ line ] )

    def get_lines_( self ):
        """
        Calcule le nombre de lignes requises pour l'affichage de cette entrée.

        :return: le nombre de ligne requises
        """
        if hasattr( self , 'lines_' ):
            return self.lines_
        m = 1
        for v in self.values_.values( ):
            if not ( v is None or isinstance( v , str )
                    or v == DiffItem.NoAccount or v == DiffItem.Unknown ):
                m = max( m , len( v ) )
        self.lines_ = m
        return m

    def check_differences( self ):
        """
        Vérifie si des différences existent et assigne chaque donnée à un groupe
        en fonction de sa valeur. Les données 'inconnues' (i.e. celles qui
        n'existent pas pour cette source de données) seront ignorées lors de la
        comparaison.

        :return: un tuple contenant un booléen qui indique si des \
                modifications ont été trouvées, et un dictionnaire associant \
                à chaque source de données un groupe (sous la forme d'un \
                entier entre 0 et 3 - cette dernière valeur indiquant un \
                champ inconnu).
        """
        v = self.values_
        groups = {
            'ldap' : 3 if v[ 'ldap' ] == DiffItem.Unknown else 0 ,
        }

        from aolpsync.utils import multivalued_check_equals as mce
        if v[ 'db' ] == DiffItem.Unknown:
            groups[ 'db' ] = 3
        elif mce( v[ 'ldap' ] , v[ 'db' ] ):
            groups[ 'db' ] = 0
        else:
            groups[ 'db' ] = 1

        if v[ 'bss' ] == DiffItem.Unknown:
            groups[ 'bss' ] = 3
        elif mce( v[ 'bss' ] , v[ 'ldap' ] ):
            groups[ 'bss' ] = groups[ 'ldap' ]
        elif mce( v[ 'bss' ] , v[ 'db' ] ):
            groups[ 'bss' ] = groups[ 'db' ]
        else:
            groups[ 'bss' ] = 2

        return ( bool( set( groups.values( ) ) - set(( 0 , 3 )) ) , groups )

    def print_data( self , widths , color ):
        """
        Génère les lignes d'affichage correspondant à cette entrée.

        :param widths: un tuple à 4 éléments indiquant les largeurs des \
                colonnes correspondant au nom du champ, aux données LDAP, \
                données base, et données BSS.
        :param color: un booléen indiquant si l'on veut que le résultat \
                soit affiché en couleurs.
        """
        n_lines = self.get_lines_( )
        order = ( 'ldap' , 'db' , 'bss' )
        ( diffs , groups ) = self.check_differences( )
        if diffs and color:
            colors = ( '\033[1m' , ) + tuple(
                    DiffItem.COLORS[ groups[ s ] ] for s in order )
            nc = '\033[0m'
        else:
            colors = tuple( '' for i in range( 0 , 4 ) )
            nc = ''

        for line in range( 0 , n_lines ):
            f = self.field if line == 0 else ''
            if self.bss_equals is not None:
                f += ' (=)' if self.bss_equals else ' (≠)'
            data = [ f ] + [ self.get_text( s , line )
                                    for s in ( 'ldap' , 'db' , 'bss' ) ]
            sl = []
            for i in range( 0 , 4 ):
                sl.append( colors[ i ] + (
                        '{:<' + str( widths[ i ] ) + '}' ).format( data[ i ] )
                    + nc )
            print( '| {} |'.format( ' | '.join( sl ) ) )


class DiffViewer( ProcessSkeleton ):
    """
    Implémentation de l'outil de diagnostic permettant l'affichage des
    différences entre les diverses sources de données.
    """

    def cli_description( self ):
        return '''Outil de diagnostic qui affiche les différences trouvées entre
                  les entrées du LDAP, de la base de synchronisation et de l'API
                  BSS pour un ou plusieurs comptes.'''

    def cli_register_arguments( self , parser ):
        parser.add_argument( '--no-colors' ,
                action = 'store_true' , default = False ,
                help = '''Désactive l'affichage couleur.''' )
        parser.add_argument( '--diff-only' , '-d' ,
                action = 'store_true' , default = False ,
                help = '''N'affiche que les lignes présentant une
                          différence.''' )
        parser.add_argument( 'eppns' ,
                action = 'store' , nargs = '+' , type = str ,
                help = '''EPPNs des comptes pour lesquels on veut afficher les
                          différences.''' )

    #---------------------------------------------------------------------------

    def preinit( self ):
        DiffItem.DOMAIN = '@{}'.format( self.cfg.get( 'bss' , 'domain' ) )

        eppn_domain = self.cfg.get( 'ldap' , 'eppn-domain' )
        self.check_accounts = []
        for eppn in self.arguments.eppns:
            if '@' not in eppn:
                eppn = '{}@{}'.format( eppn, eppn_domain )
            if eppn not in self.check_accounts:
                self.check_accounts.append( eppn )
        Logging( 'diff' ).debug( 'EPPNs concernés: {}'.format( ', '.join(
                    self.check_accounts ) ) )

        suffix = '@' + eppn_domain
        sl = -len( suffix )
        uids = [ eppn[ :sl ]
            for eppn in self.check_accounts
                if eppn.endswith( suffix ) ]
        self.ldap_query = '(|{}{})'.format(
                ''.join( '(eduPersonPrincipalName={})'.format( eppn )
                            for eppn in self.check_accounts ) ,
                ''.join( '(uid={})'.format( uid ) for uid in uids ) )

    #---------------------------------------------------------------------------

    def read_bss_account( self , eppn , bss_domain ):
        """
        Lit les informations d'un compte sur le serveur Partage et stocke ces
        informations dans le dictionnaire des comptes Partage après les avoir
        converties.

        :param eppn: l'EPPN de l'utilisateur à rechercher
        :param bss_domain: le nom de domaine Partage concerné
        """
        retr = BSSAction( BSSQuery( 'getAllAccounts' ) ,
                bss_domain , offset = 0 , limit = 100 ,
                ldapQuery = '(carLicense={})'.format( eppn ) )
        if not retr:
            raise FatalError( 'Impossible de rechercher un compte Partage' )

        obtained = retr.get( )
        if not obtained:
            Logging( 'diff' ).debug(
                    'Compte {} absent du BSS'.format( eppn ) )
            return

        assert len( obtained ) == 1
        Logging( 'diff' ).debug( 'Compte {} présent sur le BSS'.format( eppn ) )

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
        self.bss_accounts[ eppn ] = account

    def init( self ):
        """
        Initialise l'outil en lisant les comptes demandés depuis le serveur
        Partage et en générant la liste des champs à afficher.
        """
        self.bss_accounts = {}
        Logging( 'diff' ).info( 'Recherche et extraction des comptes BSS' )
        bss_domain = self.cfg.get( 'bss' , 'domain' )
        for eppn in self.check_accounts:
            self.read_bss_account( eppn , bss_domain )
        Logging( 'diff' ).debug( '{} entrée(s) trouvée(s) sur le BSS'.format(
                len( self.bss_accounts ) ) )

        fields = set( SyncAccount.STORAGE )
        fields.remove( 'eppn' )
        self.fields = sorted( fields )

    #---------------------------------------------------------------------------

    def di_set_source( self , di , source ):
        """
        Initialise un DiffItem pour une source de donnée en allant lire le champ
        correspondant dans le dictionnaire approprié.

        :param DiffItem di: l'objet à mettre à jour
        :param source: la source de données ('ldap', 'db' ou 'bss')
        """
        a = getattr( self , '{}_accounts'.format( source ) )
        if di.eppn in a:
            v = getattr( a[ di.eppn ] , di.field )
        else:
            v = DiffItem.NoAccount

        if isinstance( v , bytes ):
            v = v.decode( 'ascii' )
        elif isinstance( v , int ):
            v = str( v )

        di.set_value( source , v )

    def init_diff_item( self , eppn , field ):
        """
        Génère une entrée de liste de différences pour une combinaison EPPN /
        champ en lisant les valeurs correspondantes.

        :param str eppn: l'EPPN de l'utilisateur
        :param str field: le nom du champ

        :return: l'enregistrement de différences
        """
        di = DiffItem( eppn , field )
        self.di_set_source( di , 'ldap' )
        self.di_set_source( di , 'db' )
        if field in SyncAccount.BSS.values( ) or field in (
                    'mail' , 'aliases' , 'markedForDeletion' , 'cos' ):
            self.di_set_source( di , 'bss' )
        if ( field in SyncAccount.DETAILS and eppn in self.bss_accounts
                and eppn in self.db_accounts ):
            from aolpsync.utils import multivalued_check_equals as mce
            va = getattr( self.db_accounts[ eppn ] , field , None )
            vb = getattr( self.bss_accounts[ eppn ] , field , None )
            di.bss_equals = mce( va , vb )
        return di

    def compute_diff( self , eppn ):
        """
        Génère les enregistrements de différences pour un compte.

        :param str eppn: l'EPPN de l'utilisateur
        :return: la liste des DiffItem pour chaque champ
        """
        if eppn in self.bss_accounts and eppn in self.db_accounts:
            ad = self.db_accounts[ eppn ]
            ab = self.bss_accounts[ eppn ]
            bss_equals = ad.bss_equals( ab )
            if bss_equals:
                bsse_extra = ''
            else:
                from aolpsync.utils import multivalued_check_equals as mce
                mail_ok = mce( ad.mail , ab.mail )
                mfd_ok = ad.markedForDeletion == ab.markedForDeletion
                details_ok = not ad.details_differ( ab )
                aliases_ok = mce( ad.aliases , ab.aliases )
                bsse_extra = ( ' - mail: {} / mFD: {} / '
                               'details: {} / alias: {}' ).format(
                        *( ( 'OK' if e else 'KO' ) for e in (
                                mail_ok , mfd_ok , details_ok , aliases_ok ) ) )
        else:
            bss_equals = None
            bsse_extra = ''
        return ( bss_equals , bsse_extra ,
                [ self.init_diff_item( eppn , fld )
                        for fld in self.fields ] )

    def process( self ):
        diffs = []

        def find_max_widths_( ):
            """
            Identifie les largeurs maximales, sur l'intégralité des
            enregistrements, pour les différentes colonnes (titre + colonnes de
            données).

            :return: les largeurs maximales pour les 4 colonnes sous la forme \
                    d'un tuple
            """
            wf = 0
            ws = [ 0 , 0 , 0 ]
            for d in diffs:
                wf = max( len( d.field ) , wf )
                dws = d.display_widths( )
                for i in range( 0 , 3 ):
                    ws[ i ] = max( ws[ i ] , dws[ i ] )
            return ( wf , *ws )

        # Génération des entrées de différences
        eq_check = {}
        ec_extra = {}
        for eppn in self.check_accounts:
            bss_equals , extra , acc_diffs = self.compute_diff( eppn )
            diffs.extend( acc_diffs )
            eq_check[ eppn ] = bss_equals
            ec_extra[ eppn ] = extra
        widths = find_max_widths_( )
        total_width = 13 + sum( widths )

        # Génération des lignes de séparation des tables
        sep0 = ( '*{:=>' + str( total_width - 2 ) + '}*' ).format( '' )
        sep1 = '*{}*'.format( '*'.join(
                    ( '{:=>' + str( w + 2 ) + '}' )
                        for w in widths
                    ).format( '' , '' , '' , '' ) )
        sep2 = '+{}+'.format( '+'.join(
                    ( '{:->' + str( w + 2 ) + '}' )
                        for w in widths
                    ).format( '' , '' , '' , '' ) )

        # Affichage
        prev_eppn = None
        for di in diffs:
            if self.arguments.diff_only:
                diffs = di.check_differences( )[ 0 ]
                if not diffs:
                    continue
            if prev_eppn != di.eppn:
                if prev_eppn is not None:
                    print( sep1 )
                if eq_check[ di.eppn ] is None:
                    ec = 'N/A'
                elif eq_check[ di.eppn ]:
                    ec = 'OK'
                else:
                    ec = 'KO'
                ttl_str = '{} (bss_equals: {}{})'.format( di.eppn , ec ,
                        ec_extra[ di.eppn ] )
                print( )
                print( sep0 )
                print( ( ( '| EPPN {: <' + str( total_width - 9 )
                            + '} |' ).format( ttl_str ) ) )
                print( sep1 )
                prev_eppn = di.eppn
            else:
                print( sep2 )
            di.print_data( widths , not self.arguments.no_colors )

        if prev_eppn is None:
            print( "Aucune entrée ne présente de différences." )
        else:
            print( sep1 )
            print( )



#-------------------------------------------------------------------------------


try:
    DiffViewer( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )
