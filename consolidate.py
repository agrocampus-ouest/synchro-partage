#!/usr/bin/python3

from aolpsync import *


#-------------------------------------------------------------------------------


class Consolidator( ProcessSkeleton ):

    def cli_description( self ):
        return '''Tente de consolider les données présentes dans la base locale
                  à partir des informations du serveur de Renater.

                  Ce script devrait être utilisé afin de s'assurer que les
                  modifications effectuées à partir de l'interface Web ne sont
                  pas perdues, et aussi pour vérifier la cohérence des diverses
                  sources d'information.'''

    def list_bss_accounts( self ):
        """
        Télécharge la liste des comptes Partage pour lesquels un EPPN est
        défini. La liste sera téléchargée de manière paginée, avec une taille de
        page définie dans l'entrée "page-size" de la section "bss".

        :return: la liste des emails des comptes
        """
        bss_accounts = set()
        finished = False
        offset = 0
        bss_domain = self.cfg.get( 'bss' , 'domain' )
        try:
            page_size = int( self.cfg.get( 'bss' , 'page-size' , '100' ) )
            if page_size < 1:
                raise ValueError
        except ValueError:
            raise FatalError( 'Erreur de configuration: '
                    + 'bss > page-size invalide' )

        Logging( 'bss' ).info( 'Récupération de la liste des comptes Partage' )
        while not finished:
            retr = BSSAction( BSSQuery( 'getAllAccounts' ) ,
                    bss_domain , offset = offset , limit = page_size ,
                    ldapQuery = '(carLicense=*)' )
            if not retr:
                raise FatalError( 'Impossible de lire la liste des comptes '
                        + 'depuis Partage' )

            obtained = retr.get( )
            if obtained:
                bss_accounts.update([ a.name for a in obtained ])
                offset += len( obtained )
                finished = ( len( obtained ) < page_size )
            else:
                finished = True

        Logging( 'bss' ).debug(
                'Fin de la liste, {} entrées trouvées'.format(
                    len( bss_accounts ) ) )
        return bss_accounts

    def fetch_bss_data( self ):
        failed = False
        accounts = {}
        for mail in self.list_bss_accounts( ):
            qr = BSSAction( BSSQuery( 'getAccount' ) , mail )
            if not qr:
                Logging( 'bss' ).error(
                        'Échec de la lecture du compte {}'.format( mail ) )
                failed = True
                continue
            data = qr.get( )
            accounts[ data.carLicense ] = data
        return None if failed else accounts

    def eppn_differences( self ):
        sources = ( 'bss' , 'ldap' , 'db' )
        return {
            ( s0 , s1 ) :
                set( getattr( self , '{}_accounts'.format( s0 ) ) ) - set(
                        getattr( self , '{}_accounts'.format( s1 ) ) )
            for s0 in sources
            for s1 in sources
            if s0 != s1
        }

    def process( self ):
        self.bss_accounts = self.fetch_bss_data( )
        if self.bss_accounts is None:
            raise FatalError( 'Échec de la lecture de la liste des comptes' )
        self.eppn_diff = self.eppn_differences( )

        """
        Présence / valeurs:

        LDAP      | DB        | BSS         LD
                                            c

        """



#-------------------------------------------------------------------------------


try:
    Consolidator( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )
