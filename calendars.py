#!/usr/bin/python3

from aolpsync import *


#-------------------------------------------------------------------------------


class CalendarsSynchronizer( ProcessSkeleton ):

    def cli_description( self ):
        return '''Effectue la synchronisation des emplois du temps, si l'option
                  est activée dans la configuration.'''

    def __init__( self ):
        ProcessSkeleton.__init__( self ,
                require_ldap = False ,
                require_bss = False ,
                require_cos = False )

    def process( self ):
        """
        Pour ce script, on ne veut rien faire pendant que la base de données est
        ouverte. On effectuera les modifications après l'avoir fermée, en
        utilisant la liste qui aura été chargée, mais sans être activement
        connecté à la base.
        """
        pass

    def postprocess( self ):
        calendars = CalendarSync( self.cfg )
        calendars.synchronize( self.db_accounts )

#-------------------------------------------------------------------------------


try:
    CalendarsSynchronizer( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )
