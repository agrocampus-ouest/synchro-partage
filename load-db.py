#!/usr/bin/python3

from aolpsync import *


#-------------------------------------------------------------------------------


class DbLoader( ProcessSkeleton ):

    def cli_description( self ):
        return '''Charge des enregistrements dans la base de données de
                  synchronisation en se basant sur le fichier JSON créé lors du
                  provisioning.'''

    def cli_epilog( self ):
        return '''Les enregistrements seront chargés en respectant les critères
                  de sélection définis dans la section 'ldap' du fichier de
                  configuration.'''

    def cli_register_arguments( self , parser ):
        parser.add_argument( '-o' , '--overwrite' ,
                action = 'store_true' ,
                help = '''Remplace les enregistrements déjà présents dans la
                          base de données''' )
        parser.add_argument( 'json_input' ,
                action = 'store' , type = str ,
                help = '''Le fichier JSON d'initialisation généré par le script
                          de provisioning''' )

    def __init__( self ):
        ProcessSkeleton.__init__( self ,
                require_ldap = False , require_bss = False ,
                require_cos = False )

    #---------------------------------------------------------------------------

    def preinit( self ):
        """
        Charge les données depuis le fichier JSON spécifié.
        """
        import json
        try:
            with open( self.arguments.json_input , 'r' ) as src_file:
                str_data = src_file.read( )
            data = aolputils.json_load( str_data )
        except IOError as e:
            raise FatalError( "Impossible de lire '{}': {}".format(
                    self.arguments.json_input , str_data ) )
        except json.decoder.JSONDecodeError as e:
            raise FatalError( "Erreur JSON dans '{}' - {}".format(
                    self.arguments.json_input , str( e ) ) )
        self.file_accounts = {
            eppn : SyncAccount( self.cfg ).from_json_record( data[ eppn ] )
                for eppn in data
        }

    def process( self ):
        """
        Filtre les comptes lus depuis le fichier en se basant sur le critère
        défini dans la configuration, puis ajoute les nouveaux enregistrements à
        la base de données. Si l'option d'écrasement a été spécifiée, tous les
        enregistrements seront écrits.
        """
        # On applique les filtres
        flt = self.get_match_rule( )
        filtered = [ eppn for eppn in self.file_accounts
                if flt.check( self.file_accounts[ eppn ] ) ]
        Logging( 'load' ).debug( 'Enregistrements candidats: {}'.format(
                ', '.join( filtered ) ) )

        # On sauvegarde les nouveaux enregistrements
        force = self.arguments.overwrite
        for eppn in filtered:
            if eppn in self.db_accounts:
                if not force:
                    Logging( 'load' ).info(
                        'Enregistrement {} déjà chargé'.format( eppn ) )
                    continue
                Logging( 'load' ).info( 'Enregistrement {} à écraser'.format(
                        eppn ) )
            else:
                Logging( 'load' ).info( 'Enregistrement {} à ajouter'.format(
                        eppn ) )
            self.save_account( self.file_accounts[ eppn ] )





#-------------------------------------------------------------------------------


try:
    DbLoader( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )

