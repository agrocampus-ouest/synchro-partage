#!/usr/bin/python3

from aolpsync import *
from aolpsync.utils import ( Zimbra , ZimbraError , ZimbraConnectionError ,
                             ZimbraRequestError )


class LsFolders( ProcessSkeleton ):
    """
    Outil d'affichage de la liste des dossiers d'un utilisateur.
    """

    def cli_description( self ):
        return '''Outil d'affichage de la liste des dossiers Zimbra d'un
                  utilisateur.'''

    def cli_register_arguments( self , parser ):
        parser.add_argument( 'eppn' ,
                action = 'store' , type = str ,
                help = '''EPPN du compte dont on veut lister les dossiers.''' )

    #---------------------------------------------------------------------------

    def __init__( self ):
        ProcessSkeleton.__init__( self ,
                require_ldap = False ,
                require_bss  = False ,
                require_cos  = False )

    def process( self ):
        zimbra = Zimbra( self.cfg )
        if '@' in self.arguments.eppn:
            eppn = self.arguments.eppn
        else:
            eppn = '{}@{}'.format( self.arguments.eppn ,
                    self.cfg.get( 'ldap' , 'eppn-domain' ) )

        def dump_folder( folder ):
            """
            Fonction d'affichage récursif des dossiers.
            """
            print( "{} {:^13} {}".format( folder[ 'uuid' ] ,
                    folder[ 'view' ] if 'view' in folder else 'N/A' ,
                    folder[ 'absFolderPath' ] ) )
            if 'folder' in folder:
                for f in folder[ 'folder' ]:
                    dump_folder( f )

        try:
            try:
                zimbra.set_user( self.arguments.eppn )
                root_folder = zimbra.get_folder( )
            finally:
                zimbra.terminate( )
        except ZimbraError as e:
            print( "Erreur Zimbra: {}".format( str( e ) ) )
        else:
            dump_folder( root_folder )


#-------------------------------------------------------------------------------


try:
    LsFolders( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )
