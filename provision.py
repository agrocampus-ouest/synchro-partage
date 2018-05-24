#!/usr/bin/python3

from aolpsync import *


#-------------------------------------------------------------------------------


class Provisioner( ProcessSkeleton ):
    """
    Cette classe implémente la génération du LDIF initial. Il crée également un
    fichier JSON qui pourra être utilisé pour alimenter la base de données
    intermédiaire.
    """

    def get_ldif_mapping( self ):
        domain = self.cfg.get( 'ldap' , 'eppn-domain' ).split( '.' )
        dn_root = 'ou={},dc={}'.format( self.arguments.organizational_unit ,
                ',dc='.join( domain ) )
        dn_fmt = 'uid={},' + dn_root

        from collections import OrderedDict
        def pw_decoder_( entry ):
            try:
                return entry.passwordHash.decode( 'ascii' )
            except UnicodeDecodeError:
                raise FatalError( ( 'Le mot de passe de {} contient des '
                        + 'caractères non-ASCII' ).format( e.eppn ) )

        mapping = OrderedDict((
            ( 'dn' , lambda e : dn_fmt.format( e.uid ) ) ,
            ( 'zimbraMailAlias' , lambda e : ( None if e.aliases is None
                                                  else list( e.aliases ) ) ) ,
            ( 'userPassword' , pw_decoder_ ) ,
            ( 'description' , lambda e : e.cos ) ,
            ( 'Id_eppn' , lambda e : e.eppn ) ,
        ))

        # Autres mappings
        def mk_lambda_( attr ):
            return lambda e : getattr( e , attr )
        for bss_attr in SyncAccount.BSS:
            if bss_attr in mapping or bss_attr == 'carLicense':
                continue
            mapping[ bss_attr ] = mk_lambda_( SyncAccount.BSS[ bss_attr ] )

        return mapping

    def cli_description( self ):
        return '''Génère le fichier LDIF de provisioning ainsi qu'un fichier
                  JSON qui servira ensuite à alimenter la base de données.'''

    def cli_register_arguments( self , parser ):
        parser.add_argument( '-u' , '--organizational-unit' ,
                action = 'store' , type = str ,
                metavar = 'ou' , default = 'people' ,
                help = '''Spécifie l'OU à utiliser dans les enregistrements
                          LDAP (défaut: 'people').''' )
        parser.add_argument( '-r' , '--redirects' ,
                action = 'store' , type = str , metavar = 'csv-file' ,
                help = '''Utilise un fichier CSV listant des redirections à
                          mettre en place lors du provisioning.''' )
        parser.add_argument( 'ldif_output' ,
                action = 'store' , type = str ,
                help = '''Le fichier LDIF à générer''' )
        parser.add_argument( 'json_output' ,
                action = 'store' , type = str ,
                help = '''Le fichier JSON d'initialisation de la base de données
                          à générer''' )

    def load_redirects( self , in_file ):
        import csv
        rd_csv = csv.reader( in_file )
        ln = 0
        eppn_domain = self.cfg.get( 'ldap' , 'eppn-domain' )
        redirects = dict( )
        for row in rd_csv:
            ln = ln + 1
            if len( row ) != 2:
                Logging( 'prov' ).error(
                    'Liste des redirections, ligne {} invalide'.format( ln ) )
                continue
            ( uid_or_eppn , target ) = row
            if '@' not in uid_or_eppn:
                uid_or_eppn = '{}@{}'.format( uid_or_eppn , eppn_domain )
            redirects[ uid_or_eppn ] = target
        Logging( 'prov' ).debug( '{} redirection(s) chargée(s)'.format(
                len( redirects ) ) )
        return redirects

    def map_to_ldif( self , mapping , account ):
        from collections import OrderedDict
        output = OrderedDict( )
        for field in mapping:
            value = mapping[ field ]( account )
            if value is not None:
                output[ field ] = value
        return output

    def init_ldif( self ):
        mapping = self.get_ldif_mapping( )
        dn = set( )
        entries = []
        for eppn in self.ldap_accounts:
            account = self.ldap_accounts[ eppn ]
            ldif_data = self.map_to_ldif( mapping , account )
            if ldif_data[ 'dn' ] in dn:
                raise FatalError(
                        'Mapping LDIF incorrect, doublon du DN {}'.format(
                                ldif_data[ 'dn' ] ) )
            if eppn in self.redirects:
                ldif_data[ 'zimbraPrefMailForwardingAddress' ] = self.redirects[
                        eppn ]
            entries.append( ldif_data )
        return entries

    def init_json( self ):
        return {
            eppn : self.ldap_accounts[ eppn ].to_json_record( )
                for eppn in self.ldap_accounts
        }

    def write_ldif( self , out , data ):
        def write_attr_( name , value ):
            print( '{}: {}'.format( name , value ) , file = out )

        for e in data:
            write_attr_( 'dn' , e[ 'dn' ] )
            for attr in e:
                if attr == 'dn': continue
                av = e[ attr ]
                if isinstance( av , str ):
                    write_attr_( attr , av )
                else:
                    for v in av: write_attr_( attr , v )
            print( file = out )

    def preinit( self ):
        if self.arguments.redirects is not None:
            # Chargement des redirections
            try:
                with open( self.arguments.redirects ) as csv_file:
                    self.redirects = self.load_redirects( csv_file )
            except IOError as e:
                raise FatalError( "Impossible de charger '{}': {}".format(
                        self.arguments.redirects , str( e ) ) )
        else:
            self.redirects = { }

    def process( self ):
        # Génère les données
        ldif_data = self.init_ldif( )
        json_data = self.init_json( )

        # Écrit le LDIF
        try:
            with open( self.arguments.ldif_output , 'w' ) as ldif_file:
                self.write_ldif( ldif_file , ldif_data )
        except IOError as e:
            raise FatalError( "Impossible de créer '{}': {}".format(
                    self.arguments.ldif_output , str( e ) ) )

        # Écrit le fichier JSON
        try:
            with open( self.arguments.json_output , 'w' ) as json:
                print( aolputils.json_dump( json_data ) , file = json )
        except IOError as e:
            raise FatalError( "Impossible de créer '{}': {}".format(
                    self.arguments.json_output , str( e ) ) )

    def __init__( self ):
        ProcessSkeleton.__init__( self ,
                require_bss = False ,
                require_cos = False )

#-------------------------------------------------------------------------------


try:
    Provisioner( )
except FatalError as e:
    import sys
    Logging( 'prov' ).critical( str( e ) )
    sys.exit( 1 )

