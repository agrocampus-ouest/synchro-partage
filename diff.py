#!/usr/bin/python3

from aolpsync import *


class DiffItem:

    COLORS = ( '\033[32m' , '\033[33m' , '\033[31m' , '' )
    DOMAIN = None

    class Unknown: pass
    class NoAccount: pass

    def __init__( self , cfg , eppn , field ):
        if DiffItem.DOMAIN is None:
            DiffItem.DOMAIN = '@{}'.format( cfg.get( 'bss' , 'domain' ) )
        self.cfg = cfg
        self.eppn = eppn
        self.field = field
        self.values_ = {
            'ldap': DiffItem.Unknown ,
            'db': DiffItem.Unknown ,
            'bss': DiffItem.Unknown ,
        }
        self.text_ = {}

    def set_value( self , source , value ):
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
        n_lines = self.get_lines_( )
        return tuple( self.get_width_( source , n_lines )
                for source in ( 'ldap' , 'db' , 'bss' ) )

    def get_width_( self , source , n_lines ):
        return max( len( self.get_text( source , i ) )
                        for i in range( 0 , n_lines ) )

    def get_text( self , source , line ):
        v = self.values_[ source ]
        if ( v is None or isinstance( v , str )
                    or v == DiffItem.NoAccount
                    or v == DiffItem.Unknown ):
            if line != 0:
                return ''
            if v is None:
                return 'Non renseigné'
            if isinstance( v , str ):
                return v
            if v == DiffItem.NoAccount:
                return 'Compte absent'
            return 'N/A'

        if line >= len( v ):
            return ''
        v = tuple( v )
        return v[ line ]

    def get_lines_( self ):
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
        v = self.values_
        groups = {
            'ldap' : 0 ,
            'db' : 0 if v[ 'ldap' ] == v[ 'db' ] else 1 ,
        }
        if v[ 'bss' ] == DiffItem.Unknown:
            groups[ 'bss' ] = 3
        elif v[ 'bss' ] == v[ 'ldap' ]:
            groups[ 'bss' ] = groups[ 'ldap' ]
        elif v[ 'bss' ] == v[ 'db' ]:
            groups[ 'bss' ] = groups[ 'db' ]
        else:
            groups[ 'bss' ] = 2
        return ( bool( set( groups.values( ) ) - set(( 0 , 3 )) ) , groups )

    def print_data( self , widths , color ):
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
            data = [ f ] + [ self.get_text( s , line )
                                    for s in ( 'ldap' , 'db' , 'bss' ) ]
            sl = []
            for i in range( 0 , 4 ):
                sl.append( colors[ i ] + (
                        '{:<' + str( widths[ i ] ) + '}' ).format( data[ i ] )
                    + nc )
            print( '| {} |'.format( ' | '.join( sl ) ) )


class DiffViewer( ProcessSkeleton ):

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
        eppn_domain = self.cfg.get( 'ldap' , 'eppn-domain' )
        self.check_accounts = set((
            eppn if '@' in eppn else ( '{}@{}'.format( eppn, eppn_domain ) )
                for eppn in self.arguments.eppns ))
        Logging( 'diff' ).debug( 'EPPNs concernés: {}'.format( ', '.join(
                    self.check_accounts ) ) )

    #---------------------------------------------------------------------------

    def read_bss_account( self , eppn , bss_domain ):
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

    def diff_item_init( self , di , eppn , field , source ):
        a = getattr( self , '{}_accounts'.format( source ) )
        if eppn in a:
            v = getattr( a[ eppn ] , field )
        else:
            v = DiffItem.NoAccount

        if isinstance( v , bytes ):
            v = v.decode( 'ascii' )
        elif isinstance( v , int ):
            v = str( v )

        di.set_value( source , v )

    def compute_diff_item( self , eppn , field ):
        di = DiffItem( self.cfg , eppn , field )

        self.diff_item_init( di , eppn , field , 'ldap' )
        self.diff_item_init( di , eppn , field , 'db' )
        if field in SyncAccount.BSS.values( ) or field in (
                    'aliases' , 'markedForDeletion' , 'cos' ):
            self.diff_item_init( di , eppn , field , 'bss' )

        print( di.check_differences( ) )
        return di

    def compute_diff( self , eppn ):
        return [ self.compute_diff_item( eppn , fld )
                        for fld in self.fields ]

    def process( self ):
        diffs = []

        def find_max_widths_( ):
            wf = 0
            ws = [ 0 , 0 , 0 ]
            for d in diffs:
                wf = max( len( d.field ) , wf )
                dws = d.display_widths( )
                for i in range( 0 , 3 ):
                    ws[ i ] = max( ws[ i ] , dws[ i ] )
            return ( wf , *ws )


        for eppn in self.check_accounts:
            diffs.extend( self.compute_diff( eppn ) )
        widths = find_max_widths_( )
        total_width = 13 + sum( widths )

        prev_eppn = None
        sep0 = ( '*{:=>' + str( total_width - 2 ) + '}*' ).format( '' )
        sep1 = '*{}*'.format( '*'.join(
                    ( '{:=>' + str( w + 2 ) + '}' )
                        for w in widths
                    ).format( '' , '' , '' , '' ) )
        sep2 = '+{}+'.format( '+'.join(
                    ( '{:->' + str( w + 2 ) + '}' )
                        for w in widths
                    ).format( '' , '' , '' , '' ) )
        for di in diffs:
            if self.arguments.diff_only:
                diffs = di.check_differences( )[ 0 ]
                if not diffs:
                    continue
            if prev_eppn != di.eppn:
                if prev_eppn is not None:
                    print( sep1 )
                print( )
                print( sep0 )
                print( ( ( '| EPPN {: <' + str( total_width - 9 )
                            + '} |' ).format( di.eppn ) ) )
                print( sep1 )
                prev_eppn = di.eppn
            else:
                print( sep2 )
            di.print_data( widths , not self.arguments.no_colors )

        if prev_eppn is not None:
            print( sep1 )
            print( )



#-------------------------------------------------------------------------------


try:
    DiffViewer( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )
