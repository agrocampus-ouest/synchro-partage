#!/usr/bin/python3

from aolpsync import *


#-------------------------------------------------------------------------------


class CosMigration( ProcessSkeleton ):
    """
    Outil permettant de mettre à jour la base de données en cas de mise à jour
    des noms de classes de services.
    """

    def cli_description( self ):
        return '''Outil de dépannage permettant de modifier les noms des
                  classes de services dans la base de données intermédiaire.
                  Ce script peut être utilisé si l'équipe de Partage décide de
                  renommer des classes de services.'''

    def cli_register_arguments( self , parser ):
        parser.add_argument( 'csv' , action = 'store' ,
                help = '''Un fichier CSV contenant la liste des classes de
                          service affectées. Chaque ligne du fichier contient
                          un champ avec le nom initial et un champ avec le
                          nouveau nom.''' )

        parser.add_argument( '--ignore-bss' , '-I' ,
                action = 'store_true' ,
                help = '''Ne vérifie pas l'existence des nouveaux noms dans
                          le BSS. DANGEREUX.''' )

    #---------------------------------------------------------------------------

    def __init__( self ):
        ProcessSkeleton.__init__( self , require_ldap = False )

    def preinit( self ):
        """
        Charge les données depuis le fichier CSV.
        """
        import csv
        try:
            with open( self.arguments.csv , 'r' ) as csvfile:
                csv_reader = csv.reader( csvfile , delimiter = ',' ,
                        quotechar = '"' )
                self.substs_ = {}
                nl = 1
                for row in csv_reader:
                    if len( row ) != 2:
                        raise FatalError( ( '{} ligne {}: 2 entrées attendues, '
                                    + '{} trouvées' ).format(
                                        self.arguments.csv , nl , len( row ) ) )
                    ( oname , nname ) = row
                    if oname in self.substs_:
                        raise FatalError( ( '{} ligne {}: plusieurs entrées '
                                    + 'pour la CoS {}' ).format(
                                        self.arguments.csv , nl , oname ) )
                    if oname == nname:
                        Logging( ).warning( '{} ligne {}: identité'.format(
                                self.arguments.csv , nl ) )
                    self.substs_[ oname ] = nname
                    nl += 1
        except IOError as e:
            raise FatalError( 'Impossible de lire le fichier {}: {}'.format(
                    self.arguments.csv , str( e ) ) )

    def init( self ):
        """
        Vérifie que les nouveaux noms de CoS sont définis.
        """
        if self.arguments.ignore_bss:
            log_err = lambda x : Logging( ).warning( x )
        else:
            log_err = lambda x : Logging( ).error( x )
        nmissing = 0
        for nname in self.substs_.values( ):
            if nname not in self.coses:
                log_err( 'CoS {} non trouvée sur le BSS'.format( nname ) )
                nmissing += 1
        if nmissing and not self.arguments.ignore_bss:
            raise FatalError(
                'Certains noms de CoS ne sont pas définis sur le BSS.' )

    def process( self ):
        naffected = 0
        for eppn in self.db_accounts.keys( ):
            account = self.db_accounts[ eppn ]
            if account.cos not in self.substs_:
                continue
            account.cos = self.substs_[ account.cos ]
            self.save_account( account )
            naffected += 1
        Logging( ).info( '{} compte(s) affecté(s)'.format( naffected ) )



#-------------------------------------------------------------------------------


try:
    CosMigration( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    print( "ERREUR: {}".format( str( e ) ) )
    sys.exit( 1 )


