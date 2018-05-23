#!/usr/bin/python3

from aolpsync import *


#-------------------------------------------------------------------------------


class Deleter( ProcessSkeleton ):
    """
    Cette classe implémente le script de suppression des comptes ayant été
    marqués comme pré-supprimés.
    """

    def __init__( self ):
        ProcessSkeleton.__init__( self ,
                require_ldap = False ,
                require_cos = False )

    def cli_description( self ):
        return '''Supprime les comptes ayant été pré-supprimés il y a 
                  suffisamment longtemps.''';

    def cli_register_arguments( self , parser ):
        parser.add_argument( '-A' , '--auto' ,
                action = 'store_true' ,
                help = '''Effectue les suppressions automatiquement, sans
                    intéraction avec l'utilisateur.''' )

    def get_deletion_threshold( self ):
        """
        Lit le seuil de suppression depuis le fichier de configuration.

        :return: le seuil de suppression, en secondes
        """
        threshold = self.cfg.get( 'bss' , 'deletion-threshold' )
        try:
            threshold = int( threshold )
            if threshold <= 0:
                raise ValueError
        except ValueError:
            raise FatalError( 'Seuil de suppression incorrect ("{}")'.format(
                    threshold ) )
        Logging( ).info( 'Seuil de suppression: {} jour(s)'.format(
                threshold ) )
        return threshold * 86400 # 86400 == secondes / jour

    def identify_targets( self ):
        """
        Identifie les comptes pré-supprimés devant être supprimés.
        """
        import time
        threshold = self.get_deletion_threshold( )
        cur_time = int( time.time( ) )
        self.to_delete = [ account
                for account in self.db_accounts.values( )
                if ( account.markedForDeletion is not None
                    and cur_time - account.markedForDeletion >= threshold ) ]

    def display_and_confirm( self ):
        """
        Affiche la liste des comptes à supprimer et demande confirmation à
        l'utilisateur.

        :return: True si l'on doit supprimer les comptes
        """
        if not self.to_delete:
            print( "Aucun compte à supprimer" )
            return False

        print( "{:45}{}".format( "Compte à supprimer" , "Pré-suppression" ) )
        print( )
        for account in self.to_delete:
            from datetime import datetime
            psdt = datetime.fromtimestamp( account.markedForDeletion )
            print( "{:45}{}".format( account.eppn ,
                    psdt.strftime( '%d/%m/%Y %H:%M:%S' ) ) )
        print( )
        ok = input( 'Supprimer ces comptes [o/N] ? ' )
        ok = ok.strip( ).lower( ) == 'o'
        if not ok:
            print( "Suppression annulée" )
        return ok

    def delete_account( self , account ):
        """
        Supprime un compte Partage et, en cas de succès, supprime l'entrée de
        base de données correspondante.

        :param SyncAccount account: le compte à supprimer
        """
        Logging( ).info( 'Suppression de {}'.format( account.mail ) )
        if not BSSAction( 'deleteAccount' , account.mail ):
            Logging( ).error( 'Compte {}: échec de la suppression'.format(
                    account.mail ) )
            # FIXME: vérifier si le compte existe chez Partage, le supprimer à
            # la main de la base si ce n'est pas le cas
        else:
            self.remove_account( account )

    def delete_accounts( self ):
        """
        Supprime les comptes.
        """
        for account in self.to_delete:
            self.delete_account( account )

    def process( self ):
        """
        Tente de lister les comptes à supprimer. S'il y en a, demande
        éventuellement une confirmation à l'utilisateur (si l'on est en mode
        intéractif), puis effectue les suppressions.
        """
        self.identify_targets( )
        if self.arguments.auto:
            if not self.to_delete:
                Logging( ).info( 'Aucun compte à supprimer' )
                return
            Logging( ).info( '{} compte(s) à supprimer'.format(
                    len( self.to_delete ) ) )
        elif not self.display_and_confirm( ):
            return
        self.delete_accounts( )


#-------------------------------------------------------------------------------


try:
    Deleter( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )
