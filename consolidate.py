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

        # ## | LDAP      | DB        | BSS       | État
        # ---+-----------+-----------+-----------+------------------------------
        # 01 | A         | A         | A         | OK
        # 02 | -         | Ap        | Ap        | OK
        # 03 | -         | -         | A         | OK
        # ---+-----------+-----------+-----------+------------------------------
        # 10 | B         | -         | -         | OK/NS
        # 11 | B         | A         | A         | OK/NS
        # 12 | -         | A         | A         | OK/NS
        # 13 | B         | Ap        | Ap        | OK/NS
        # ---+-----------+-----------+-----------+------------------------------
        # 20 | A         | A         | -         | NOK/B-
        # 21 | B         | A         | -         | NOK/B-
        # 22 | A         | -         | A         | NOK/B+
        # 23 | -         | -         | Ap        | NOK/B+
        # 24 | -         | Ap        | Bp        | NOK/B~
        # ---+-----------+-----------+-----------+------------------------------
        # 30 | A         | -         | B         | NOK/NA
        # 31 | -         | Ap        | B         | NOK/NA
        # 32 | -         | A         | Bp        | NOK/NA
        # 33 | A         | A         | B         | NOK/NA
        # 34 | A         | B         | C         | NOK/NA
        #
        # Données:
        #   A/B/C - Valeurs différentes pour un même enregistrement
        #   Xb - Compte marqué comme présupprimé
        #
        # États:
        #   OK (bien, on ne fait rien)
        #       OK/NS (bien, sera mis à jour par script de synchro)
        #   NOK (pas bien, on crie et parfois on essaie de corriger)
        #   Bx: action base de données requise:
        #       B- suppression, B+ ajout, B~ m.a.j.
        #
        # Cas par cas:
        #
        #-- 0x - OK ------------------------------------------------------------
        #
        #       01. Le compte est actif, les données sont identiques dans les
        # trois systèmes.
        #
        #       02. Le compte est présupprimé, les données sont identiques
        # entre la base intermédiaire et Partage.
        #
        #       03. Le compte a été créé sur le Dashboard et n'est pas géré par
        # le SI d'Agrocampus.
        #
        #-- 1x - OK/NS ---------------------------------------------------------
        #
        #       10. Un compte a été créé dans le LDAP mais la synchronisation
        # vers Partage n'a pas encore eu lieu.
        #
        #       11. Un compte existant a été mis à jour dans le LDAP mais la
        # synchronisation n'a pas encore eu lieu.
        #
        #       12. Un compte a été supprimé dans le LDAP mais la
        # synchronisation n'a pas encore eu lieu.
        #
        #       13. Un compte qui avait été supprimé a été recréé dans le LDAP
        # mais la synchronisation n'a pas encore eu lieu.
        #
        #-- 2x - NOK/action ----------------------------------------------------
        #
        #       20. Un compte LDAP existe, ainsi qu'une entrée en base qui y
        # correspond exactement. Cependant aucun compte n'existe sur le serveur
        # Renater. Cela peut être causé: par la suppression manuelle du compte
        # sur le serveur Renater, par un problème du côté de l'API BSS qui
        # aurait renvoyé OK sans effectuer les actions demandées, par une
        # corruption de la base, par une restauration des données de la base.
        # Une notification sera envoyée, et l'entrée en base supprimée, ce qui
        # devrait provoquer une nouvelle tentative de génération du compte.
        #
        #       21. Une compte LDAP existe, ainsi qu'une entrée en base pour ce
        # compte. L'entrée en base ne correspond pas au compte LDAP, et aucune
        # entrée n'est présente sur le BSS. Les causes sont similaires à celles
        # de l'état 20 ci-dessus, avec éventuellement une mise à jour du LDAP
        # entre temps. Les actions seront identiques.
        #
        #       22. Un compte LDAP existe et est présent sur Partage avec les
        # mêmes données; en revanche, aucune entrée n'existe en base. Cela peut
        # avoir été causé par une création manuelle des deux côtés, ou par une
        # restauration de sauvegarde de la base, ou encore par une corruption de
        # la base. Le compte sera ajouté à la base avec un hash de mot de passe
        # invalide (il sera donc re-synchronisé), et une notification sera
        # envoyée.
        #
        #       23. Un compte correspondant aux critères de présuppression est
        # présent sur le BSS, mais n'existe ni dans la base, ni dans le LDAP.
        # Cela peut avoir été causé par une corruption ou restauration de
        # sauvegarde de la base de données, ou encore par une modification
        # manuelle sur le dashboard (compte désactivé et renommé avec le bon
        # motif de nommage). Le compte sera ajouté à la base avec un hash de mot
        # de passe invalide, et une notification sera envoyée.
        #
        #       24. Un compte présupprimé, présent à la fois sur le serveur
        # Partage et en base de données, a des données discordantes. Cela peut
        # avoir été causé par une corruption ou restauration de sauvegarde de la
        # base de données, ou par une modification manuelle du compte
        # présupprimé via le dashboard. Les données en base seront mises à jour,
        # et une notification sera envoyée.
        #
        #-- 3x - NOK/inaction --------------------------------------------------
        #
        # Les cas ci-dessous correspondent à des incohérences pour lesquelles il
        # est impossible de déterminer une action corrective automatiquement.
        # Une notification est envoyée, mais aucune autre action n'est
        # effectuée.
        #
        #       30. Un compte existe dans le LDAP avec certaines données, et sur
        # le serveur Partage avec d'autres données. Cela peut avoir été causé
        # par une création manuelle du compte de chaque côté avec des
        # différences de saisie, ou bien par une corruption/restauration de la
        # base de données puis mise à jour du LDAP.
        #
        #       31. Un compte est enregistré comme présupprimé dans la base de
        # données, n'existe pas dans le LDAP, mais le compte BSS correspondant
        # n'est pas dans un état de présuppression et ses données ne
        # correspondent pas à celles de la base. Cela peut avoir été causé par
        # une corruption/restauration de la base de synchronisation, par une
        # restauration manuelle du compte via le dashboard, ou par un échec
        # silencieux de l'API Partage.
        #
        #       32. Un compte est marqué comme présupprimé sur le serveur
        # Partage, n'est pas présent dans le LDAP, et existe dans la base de
        # données sans marqueur de présuppression et avec des données
        # différentes. Cela peut avoir été causé par une corruption/restauration
        # de la base de données, ou par une présuppression manuelle du compte
        # sur le dashboard.
        #
        #       33. Données cohérentes entre l'annuaire LDAP et la base de
        # données, mais différentes sur le serveur Partage. Causes possibles:
        # modifications manuelles via le dashboard, échec silencieux de l'API
        # Partage.
        #
        #       34. Un compte existe avec des données différentes dans les trois
        # systèmes. Causes possibles: corruption/restauration de la base après
        # des mises à jour manuelles incohérentes pour pallier l'interruption du
        # fonctionnement des scripts, nuées de sauterelles, fleuves de sang.
        # Bref, c'est la mouise.
        #



#-------------------------------------------------------------------------------


try:
    Consolidator( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )
