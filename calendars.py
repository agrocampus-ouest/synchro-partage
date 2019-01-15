#!/usr/bin/python3

from aolpsync import *


#-------------------------------------------------------------------------------


class CalendarsSynchronizer( ProcessSkeleton ):

    def cli_description( self ):
        return '''Effectue la synchronisation des calendriers, si l'option
                  est activée dans la configuration.'''

    def cli_register_arguments( self , parser ):
        parser.add_argument( 'eppn' ,
                action = 'store' , type = str , nargs = '*' ,
                help = '''EPPN d'un compte dont on veut forcer la
                          synchronisation.''' )

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

        # A-t-on une liste d'EPPNs à vérifier?
        if self.arguments.eppn:
            sync_set = set( )
            for eppn in self.arguments.eppn:
                if '@' not in eppn:
                    eppn = '{}@{}'.format(
                            eppn , self.cfg.get( 'ldap' , 'eppn-domain' ) )
                if eppn in self.db_accounts:
                    sync_set.add( eppn )
            if not sync_set:
                raise FatalError( 'Pas de comptes à synchroniser' )
            Logging( 'cal' ).info( 'Synchronisation manuelle: {}'.format(
                    ', '.join( sync_set ) ) )
        else:
            # Doit-on *tout* mettre à jour?
            n_accounts = int( self.cfg.get( 'calendars' , 'batch-size' , 5 ) )
            self.do_full_update = ( n_accounts == 0 )
            if self.do_full_update:
                Logging( 'cal' ).info( 'Mise à jour complète' )
                return
            Logging( 'cal' ).info(
                    'Mise à jour d\'au plus {} comptes'.format( n_accounts ) )

            # On récupère la liste des comptes déjà mis à jour
            if ( 'calendars' in self.misc_data
                    and 'processed' in self.misc_data[ 'calendars' ] ):
                processed = set( self.misc_data[ 'calendars' ][ 'processed' ] )
            else:
                processed = set( )
            Logging( 'cal' ).debug(
                    '{} compte(s) déjà mis à jour'.format( len( processed ) ) )

            # On génère la liste des mises à jour potentielles
            potential_updates = list( set( self.db_accounts.keys( ) ) - processed )
            over = ( len( potential_updates ) <= n_accounts )
            if over:
                sync_set = set( potential_updates )
            else:
                sync_set = set( potential_updates[ 0 : n_accounts ] )
            Logging( 'cal' ).debug(
                    '{} compte(s) seront synchronisés'.format( len( sync_set ) ) )

        # On effectue la mise à jour pour ces comptes
        CalendarSync( self.cfg ).synchronize( self.db_accounts , sync_set )

        if not self.arguments.eppn:
            # On enregistre la nouvelle liste de comptes à jour, sauf si l'on a
            # passé la liste entière; dans ce cas, on efface l'entrée.
            if over:
                Logging( 'cal' ).debug( 'Cycle terminé' )
                self.remove_data( 'calendars' , 'processed' )
            else:
                updated = processed | sync_set
                Logging( 'cal' ).debug(
                    'Cycle en cours: {} comptes mis à jour'.format(
                        len( updated ) ) )
                self.save_data( 'calendars' , 'processed' , updated )


    def postprocess( self ):
        """
        Effectue la synchronisation des calendriers.
        """
        if self.do_full_update:
            CalendarSync( self.cfg ).synchronize( self.db_accounts )

#-------------------------------------------------------------------------------


try:
    CalendarsSynchronizer( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )
