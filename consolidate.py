#!/usr/bin/python3

from aolpsync import *

#
#     ## | LDAP      | DB        | BSS       | État
#     ---+-----------+-----------+-----------+------------------------------
#   ✓ 01 | A         | A         | A         | OK
#   ✓ 02 | -         | Ap        | Ap        | OK
#   ✓ 03 | -         | -         | A         | OK
#     ---+-----------+-----------+-----------+------------------------------
#   ✓ 10 | B         | -         | -         | OK/NS
#   ✓ 11 | B         | A         | A         | OK/NS
#   ✓ 12 | -         | A         | A         | OK/NS
#   ✓ 13 | B         | Ap        | Ap        | OK/NS
#     ---+-----------+-----------+-----------+------------------------------
#   ✓ 20 | A         | A         | -         | NOK/B-
#   ✓ 21 | B         | A         | -         | NOK/B-
#   ✓ 22 | A         | -         | A         | NOK/B+
#   ✓ 23 | -         | -         | Ap        | NOK/B+
#   ✓ 24 | -         | Ap        | Bp        | NOK/B~
#   ✓ 25 | -         | A/Ap      | -         | NOK/B-
#     ---+-----------+-----------+-----------+------------------------------
#   ✓ 30 | A         | -         | B/Ap/Bp   | NOK/NA
#   ✓ 31 | -         | Ap        | B         | NOK/NA
#   ✓ 32 | -         | A         | Bp        | NOK/NA
#   ✓ 33 | A         | A         | B         | NOK/NA
#   ✓ 34 | A         | B         | C         | NOK/NA
#   ✓ 35 | -         | A         | B         | NOK/NA
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
#       25. Un compte n'existe que dans la base de données, probablement
# à cause d'une restauration de backup. On supprime le compte et on
# notifie.
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
#       35. Un compte existe avec des données différentes dans la base
# et sur le serveur Partage, sans marqueur de présuppression; il n'existe
# pas dans l'annuaire LDAP. Causes possibles:  corruption/restauration de
# la base après des mises à jour manuelles incohérentes pour pallier
# l'interruption du fonctionnement des scripts, it's the end of the world
# as we know it.
#

#-------------------------------------------------------------------------------


class AccountState:
    """
    Décrit les différents états qui peuvent être détectés pour un EPPN.

    :ivar bool is_error: drapeau qui indique si l'état est une erreur
    :ivar int code: code de l'état
    :ivar str text: description courte de l'état
    :ivar str color: couleur à utiliser dans le rapport HTML
    :ivar str description: description de l'état à afficher dans le rapport HTML
    :ivar str action: action corrective effectuée
    """

    def __init__( self , is_err , code , text , color , description ,
            action = None ):
        self.is_error = is_err
        self.code = code
        self.text = text
        self.color = color
        self.description = description
        self.action = action


class Consolidator( ProcessSkeleton ):
    """
    Script de consolidation du système de synchronisation.

    Ce script vérifie la cohérence des données entre l'annuaire LDAP, la base de
    données de synchronisation et le serveur Partage. Il peut essayer de
    corriger certains problèmes, générer un rapport et envoyer des mails
    d'avertissement.
    """

    def cli_description( self ):
        return '''Tente de consolider les données présentes dans la base locale
                  à partir des informations du serveur de Renater.

                  Ce script devrait être utilisé afin de s'assurer de la
                  cohérence des diverses sources d'information utilisées.'''

    #---------------------------------------------------------------------------

    def preinit( self ):
        """
        Initialise le dictionaire des états possibles.
        """
        AS = AccountState
        self.states = { a.code : a for a in (
            # États OK
            AS( False ,  1 , 'LDAP+ DB+ BSS+' , '#c3ffcd' ,
                '''Le compte est actif, les données sont identiques dans les
                   trois systèmes.''' ) ,
            AS( False ,  2 , 'LDAP- DB~ BSS~' , '#c3ffcd' ,
                '''Le compte est présupprimé, les données sont identiques
                   dans la base et sur Partage.''' ) ,
            AS( False ,  3 , 'LDAP- DB- / BSS+' , '#c3ffcd' ,
                '''Le compte a été créé sur le Dashboard et n'est pas géré par
                   les scripts.''' ) ,
            # États en attente de synchro
            AS( False , 10 , 'LDAP+ / DB- BSS-' , '#cdffc3' ,
                '''Un compte a été créé dans le LDAP mais la synchronisation
                   vers Partage n'a pas encore eu lieu.''' ) ,
            AS( False , 11 , 'LDAP+ / DB+ BSS+' , '#cdffc3' ,
                '''Un compte existant a été mis à jour dans le LDAP mais la
                   synchronisation vers Partage n'a pas encore eu lieu.''' ) ,
            AS( False , 12 , 'LDAP- / DB+ BSS+' , '#cdffc3' ,
                '''Un compte a été supprimé dans le LDAP mais la synchronisation
                   vers Partage n'a pas encore eu lieu.''' ) ,
            AS( False , 13 , 'LDAP+ / DB~ BSS~' , '#cdffc3' ,
                '''Un compte qui avait été présupprimé a été recréé dans le LDAP
                   mais la synchronisation vers Partage n'a pas encore eu
                   lieu.''' ) ,
            # États erronés avec correction auto
            AS(  True , 20 , 'LDAP+ DB+ / BSS-' , '#f2ffba' ,
                '''Un compte LDAP existe, ainsi qu'une entrée en base qui y
                   correspondant exactement. Cependant aucun compte n'existe sur
                   le serveur Partage.''' ,
                'suppr. base' ) ,
            AS(  True , 21 , 'LDAP+ / DB+ / BSS-' , '#f2ffba' ,
                '''Un compte LDAP existe, ainsi qu'une entrée en base pour ce
                   compte. L'entrée en base ne correspond pas au compte LDAP,
                   et aucune correspondante n'est présente sur Partage.''' ,
                'suppr. base' ) ,
            AS(  True , 22 , 'LDAP+ BSS+ / DB-' , '#f2ffba' ,
                '''Un compte LDAP existe et est présent sur Partage avec les
                   mêmes données; en revanche, aucune entrée n'existe en
                   base.''' ,
                'ajout base' ) ,
            AS(  True , 23 , 'LDAP- BSS~ / DB-' , '#f2ffba' ,
                '''Un compte correspondant aux critères de présuppression est
                   présent sur Partage mais n'existe ni dans la base, ni dans le
                   LDAP.''' ,
                'ajout base' ) ,
            AS(  True , 24 , 'LDAP- DB~ / BSS~' , '#f2ffba' ,
                '''Un compte présupprimé, présent à la fois sur le serveur
                   Partage et en base de données, a des données
                   discordantes.''' ,
                'm.a.j. base' ) ,
            AS(  True , 25 , 'LDAP- BSS- / DB*' , '#f2ffba' ,
                '''Un compte n'est présent que dans la base de données; rien n'y
                   correspond dans le LDAP ou sur Partage.''' ,
                'suppr. base' ) ,
            # États erronés sans correction auto
            AS(  True , 30 , 'LDAP+ / DB- / BSS+' , '#ffbaba' ,
                '''Un compte existe dans le LDAP avec certaines données, et sur
                   Partage avec d'autres données, sans être présent en
                   base.''' ) ,
            AS(  True , 31 , 'LDAP- DB~ / BSS+' , '#ffbaba' ,
                '''Un compte est enregistré comme présupprimé dans la base de
                   données, n'existe pas dans le LDAP, mais le compte Partage
                   correspondant n'est pas dans un état de présuppression et ses
                   données sont différentes de celles de la base.''' ) ,
            AS(  True , 32 , 'LDAP- BSS~ / DB+' , '#ffbaba' ,
                '''Un compte est marqué comme présupprimé sur Partage, n'est pas
                   présent dans le LDAP, et existe dans la base de données sans
                   marqueur de présuppression et avec des données
                   différentes.''' ) ,
            AS(  True , 33 , 'LDAP+ DB+ / BSS+' , '#ffbaba' ,
                '''Les données d'un compte sont cohérentes entre la base de
                   données et l'annuaire LDAP, mais elles diffèrent sur le
                   serveur Partage.''' ) ,
            AS(  True , 34 , 'LDAP+ / DB? / BSS?' , '#ff9090' ,
                '''Un compte existe sur les trois systèmes mais aucune des trois
                   entrées ne correspond aux autres. <span
                   style="font-size:1pt">It's the end of the world as we know
                   it...</span>''' ) ,
            AS(  True , 35 , 'LDAP- / DB+ / BSS+' , '#ff9090' ,
                '''Un compte existe avec des données différentes dans la base de
                   données et sur Partage, sans marqueur de présuppression; il
                   n'existe pas dans l'annuaire LDAP. <span
                   style="font-size:1pt">It's the end of the world as we know
                   it...</span>''' ) ,
            # Erreur interne / aucun résultat de vérif.
            AS(  True , 98 , 'Erreur interne' , '#ff94c5' ,
                '''Une erreur du script s'est produite pendant la vérification
                   de ce compte. Veuillez lire le journal.''' ) ,
            AS(  True , 99 , 'Pas de résultat' , '#ff94c5' ,
                '''Le script a vérifié ce compte mais n'a pas renvoyé de
                   résultat.''' ) ,
        ) }

    #---------------------------------------------------------------------------

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
        """
        Tente de lire la liste des comptes définis sur le serveur Partage et de
        télécharger leurs informations complètes.

        :return: un dictionnaire associant à chaque EPPN présent sur le \
                serveur Partage un enregistrement SyncAccount le décrivant
        """
        import requests.packages.urllib3.exceptions as rpue
        import requests.exceptions as re
        import xml.etree.ElementTree as et
        import urllib.error as ue
        import http.client as hc
        from time import sleep

        failed = False
        accounts = {}

        for mail in self.list_bss_accounts( ):
            # Lecture
            account_attempts = 0
            qr = None
            while qr is None and account_attempts < 10:
                try:
                    qr = BSSAction( BSSQuery( 'getAccount' ) , mail )
                except ( rpue.HTTPError , re.HTTPError , re.ConnectionError ,
                         et.ParseError , ue.HTTPError , hc.HTTPException ) as e:
                    account_attempts += 1
                    Logging( 'bss' ).warning(
                            'Erreur lors de la lecture du compte {}: {} '
                            '(tentative {})'.format(
                                mail , e , account_attempts ) )
                    sleep( 30 )
            if not qr:
                Logging( 'bss' ).error(
                        'Échec de la lecture du compte {}'.format( mail ) )
                failed = True
                continue
            data = qr.get( )

            # Conversion en données de synchro
            account = SyncAccount( self.cfg )
            try:
                account.from_bss_account( data , self.reverse_coses )
            except AccountStateError as e:
                Logging( 'bss' ).warning( str( e ) )
            accounts[ account.eppn ] = account

        if failed:
            return None
        Logging( 'bss' ).info( '{} comptes lus'.format( len( accounts ) ) )
        return accounts

    def init( self ):
        """
        Télécharge les informations des comptes depuis le serveur Partage. Si
        le téléchargement échoue, on ne poursuit pas.
        """
        self.bss_accounts = self.fetch_bss_data( )
        if self.bss_accounts is None:
            raise FatalError( 'Échec de la lecture de la liste des comptes' )

    #---------------------------------------------------------------------------

    def list_eppns_in( self , **kwargs ):
        """
        Génère un ensemble d'EPPNs correspondant aux paramètres. Pour l'une des
        sources (bss, ldap ou db), on spécifie un booléen ou None. On calcule
        l'intersection des comptes sur les sources pour lesquelles la valeur est
        True, on y enlève les sources pour lesquelles la valeur est False, et on
        ignore les sources pour lesquelles la valeur est None.

        :return: l'ensemble des EPPNs qui correspondent aux critères indiqués
        """
        sources = ( 'bss' , 'ldap' , 'db' )
        assert kwargs
        assert not ( set( kwargs ) - set( sources ) )
        assert False not in ( isinstance( v , bool ) for v in kwargs.values( ) )

        eppns = set( )
        first = True
        for k , v in kwargs.items( ):
            if not v:
                continue
            this_set = getattr( self , '{}_accounts'.format( k ) ).keys( )
            if first:
                eppns.update( this_set )
                first = False
            else:
                eppns.intersection_update( this_set )

        for k , v in kwargs.items( ):
            if v is None or v: continue
            eppns.difference_update( getattr( self ,
                        '{}_accounts'.format( k ) ).keys( ) )

        return eppns

    def run_account_check( self , eppn , func ):
        """
        Exécute une vérification sur un EPPN.

        :param str eppn: l'EPPN à vérifier
        :param func: une fonction prenant 4 paramètres (EPPN, compte LDAP, \
                entrée BDD, compte Partage) qui effectue les vérifications
        """
        la = ( self.ldap_accounts[ eppn ] if eppn in self.ldap_accounts
                else None )
        db = ( self.db_accounts[ eppn ] if eppn in self.db_accounts
                else None )
        bss = ( self.bss_accounts[ eppn ] if eppn in self.bss_accounts
                else None )

        try:
            func( eppn , la , db , bss )
        except Exception as e:
            Logging( 'cns' ).critical(
                    'Exception pendant vérification de {} ({})'.format(
                            eppn , repr( e ) ) )
            self.result( eppn , 98 )
        else:
            if eppn not in self.checked:
                self.result( eppn , 99 )

    def run_check( self , func , **kwargs ):
        """
        Exécute une fonction de vérification sur un ensemble de comptes.

        :param func: une fonction prenant 4 paramètres (EPPN, compte LDAP, \
                entrée BDD, compte Partage) qui effectue les vérifications
        :param ldap: inclure/exclure les comptes LDAP ?
        :param db: inclure/exclure les entrées de base de données ?
        :param bss: inclure/exclure les comptes Partage ?
        """
        for eppn in self.list_eppns_in( **kwargs ):
            if eppn in self.checked:
                continue
            self.run_account_check( eppn , func )

    def result( self , eppn , code ):
        """
        Enregistre et journalise le résultat des vérifications pour un EPPN.

        :param str eppn: l'EPPN pour lequel la vérification a été effectuée
        :param int code: le code de l'état pour cet EPPN
        """
        assert code in self.states
        state = self.states[ code ]
        s = "Compte {} - code {:0>2} - {}{}".format( eppn , code , state.text ,
                ' ({})'.format( state.action )
                        if state.action is not None
                        else '' )
        if state.is_error:
            Logging( 'cns' ).error( s )
        else:
            Logging( 'cns' ).info( s )
        self.results[ eppn ] = state
        self.checked.add( eppn )

    def process( self ):
        """
        Lance les différentes vérifications et stocke les résultats. Ceux-ci
        seront exploités dans la phase suivante pour générer rapports et mails
        d'avertissement.
        """
        self.checked = set( )
        self.results = dict( )

        def check_common_accounts_( eppn , ldap , db , bss ):
            """
            Vérification des comptes existants dans les 3 bases (cas 01, 11, 13,
            33, 34)
            """
            if db.bss_equals( bss ):
                if db == ldap:
                    self.result( eppn , 1 )
                elif db.markedForDeletion is None:
                    self.result( eppn , 11 )
                else:
                    self.result( eppn , 13 )
            elif db == ldap:
                self.result( eppn , 33 )
            else:
                self.result( eppn , 34 )

        def check_noldap_accounts_( eppn , ldap , db , bss ):
            """
            Vérification des comptes présents sur DB et BSS, mais pas en LDAP
            (cas 02, 12, 24, 31, 32, 35)
            """
            if db.bss_equals( bss ):
                if db.markedForDeletion is not None:
                    self.result( eppn , 2 )
                else:
                    self.result( eppn , 12 )
            elif db.is_predeleted( ) == bss.is_predeleted( ):
                if db.is_predeleted( ):
                    db.copy_details_from( bss )
                    db.aliases = set( bss.aliases ) if bss.aliases else None
                    db.passwordHash = b'{SSHA}invalide'
                    self.save_account( db )
                    self.result( eppn , 24 )
                else:
                    self.result( eppn , 35 )
            elif db.is_predeleted( ):
                self.result( eppn , 31 )
            else:
                self.result( eppn , 32 )

        def check_nodb_accounts_( eppn , ldap , db , bss ):
            """
            Vérification des comptes figurant dans le LDAP et dans le BSS, mais
            pas dans la base (cas 22, 30)
            """
            if ldap.bss_equals( bss ):
                opw = ldap.passwordHash
                ldap.passwordHash = b'{SSHA}invalide'
                self.save_account( ldap )
                ldap.passwordHash = opw
                self.result( eppn , 22 )
            else:
                self.result( eppn , 30 )

        def check_nobss_accounts_( eppn , ldap , db , bss ):
            """
            Vérification des comptes figurant dans le LDAP et la DB, mais pas
            sur le BSS. (cas 20, 21)
            """
            if ldap == db:
                self.result( eppn , 20 )
            else:
                self.result( eppn , 21 )
            self.remove_account( ldap )

        def handle_dbonly_accounts_( eppn , ldap , db , bss ):
            """
            Gestion des comptes figurant uniquement dans la base de données (cas
            25).
            """
            self.remove_account( db )
            self.result( eppn , 25 )

        def check_bssonly_accounts_( eppn , ldap , db , bss ):
            """
            Vérirification des comptes figurant uniquement sur le serveur
            Partage (cas 03, 23)
            """
            if bss.is_predeleted( ):
                self.save_account( bss )
                self.result( eppn , 23 )
            else:
                self.result( eppn , 3 )

        self.run_check( check_common_accounts_ ,
                ldap = True , db = True , bss = True )
        self.run_check( check_noldap_accounts_ ,
                ldap = False , db = True , bss = True )
        self.run_check( check_nodb_accounts_ ,
                ldap = True , db = False , bss = True )
        self.run_check( check_nobss_accounts_ ,
                ldap = True , db = True , bss = False )
        self.run_check( lambda e , l , d , b : self.result( e , 10 ) ,
                ldap = True , db = False , bss = False )
        self.run_check( handle_dbonly_accounts_ ,
                ldap = False , db = True , bss = False )
        self.run_check( check_bssonly_accounts_ ,
                ldap = False , db = False , bss = True )

    #---------------------------------------------------------------------------

    def find_report_name( self , base_dir ):
        """
        Génère un nom de fichier pour le rapport. Le nom de fichier contiendra
        la date du jour, ainsi qu'un numéro de série qui sera incrémenté à
        chaque nouveau rapport généré ce jour-là.

        :param str base_dir: le chemin du répertoire dans lequel les rapports \
                seront générés

        :return: un nom de fichier pour le rapport
        """
        from datetime import date
        from pathlib import Path
        import os.path
        n = date.today( )
        pattern = 'aolpsync-cns-{:0>4}-{:0>2}-{:0>2}{}'.format(
                n.year , n.month , n.day , '-{:0>2}.html' )
        i = 0
        while True:
            fn = pattern.format( i )
            fp = Path( os.path.join( base_dir , fn ) )
            if not fp.exists( ):
                return fn
            i += 1

    def generate_html_report( self ):
        """
        Génère et enregistre le rapport au format HTML correspondant à cette
        exécution du script.

        :return: le nom du fichier du rapport, ou None si la génération \
                ou l'enregistrement ont échoué
        """
        base_dir = self.cfg.get( 'consolidate' , 'report-path' )
        base_url = self.cfg.get( 'consolidate' , 'report-url' )
        from pathlib import Path
        if base_dir is None or not Path( base_dir ).is_dir( ):
            Logging( 'cns' ).critical( 'Chemin du rapport mal configuré!' )
            return None
        if base_url is None:
            Logging( 'cns' ).critical( 'URL du rapport manquante!' )
            return None

        report_name = self.find_report_name( base_dir )
        Logging( 'cns' ).info( 'Génération du rapport HTML dans {}'.format(
                report_name ) )

        # Liste des cas
        cases = sorted( set(( e for e in self.results.values( ) )) ,
                key = lambda case : case.code )
        case_counts = {
            c.code : len([ k for k , v in self.results.items( )
                        if v is c ])
                for c in cases
        }
        case_template = self.load_template( 'cns-case-template.html' )
        cases_html = ''.join(
            case_template.format(
                code = c.code ,
                action = '-' if c.action is None else c.action ,
                count = case_counts[ c.code ] ,
                bg_color = c.color ,
                description = c.description ,
            ) for c in cases
        )

        # Sections
        section_template = self.load_template( 'cns-section-template.html' )
        sections = ''.join(
            section_template.format(
                    code = c.code ,
                    state = c.text ,
                    count = case_counts[ c.code ] ,
                    cases = '<li>{}</li>'.format( '</li>\n<li>'.join( sorted(
                            k for k , v in self.results.items( )
                                if v is c
                        ) ) )
            ) for c in cases
        )

        # Page finale
        from datetime import datetime
        now = datetime.now( )
        n_errors = len([ 1 for v in self.results.values( ) if v.is_error ])
        report = self.load_template( 'cns-report-template.html' ).format(
                day = now.day , month = now.month , year = now.year ,
                hours = now.hour , minutes = now.minute , seconds = now.second ,
                c_total = len( self.results ) ,
                c_ldap = len( self.ldap_accounts ) ,
                c_db = len( self.db_accounts ) ,
                c_bss = len( self.bss_accounts ) ,
                c_errors = n_errors ,
                bg_errors = 'red' if n_errors else 'green' ,
                cases_table = cases_html ,
                sections = ''.join( sections ) ,
            )

        # Sauvegarde
        import os.path
        try:
            with open( os.path.join( base_dir , report_name ) , 'w' ) as f:
                print( report , file = f )
        except IOError as e:
            Logging( 'cns' ).critical(
                    "Impossible de créer le rapport {}: {}".format(
                        report_name , str( e ) ) )
            return None
        return report_name

    #---------------------------------------------------------------------------

    def connect_smtp( self ):
        """
        Établit la connexion au serveur SMTP tel que configuré dans la section
        'consolidate' du fichier de configuration.

        :return: une connexion au serveur SMTP établie via smtplib, ou None \
                si la connexion a échoué.
        """
        Logging( 'cns' ).debug( 'Connexion au serveur SMTP' )
        try:
            import smtplib
            server = smtplib.SMTP(
                    self.cfg.get( 'consolidate' , 'smtp-server' ) ,
                    int( self.cfg.get( 'consolidate' , 'smtp-port' , 25 ) ) )
            server.ehlo( )
            if self.cfg.has_flag( 'consolidate' , 'smtp-tls' ):
                server.starttls( )
                server.ehlo( )
            user = self.cfg.get( 'consolidate' , 'smtp-user' , '' )
            if user:
                server.login( user ,
                        self.cfg.get( 'consolidate' , 'smtp-password' ) )
        except Exception as e:
            Logging( 'cns' ).critical(
                    'Impossible de se connecter au serveur SMTP: {}'.format(
                        repr( e ) ) )
            return None
        return server

    def send_mail( self , text ):
        """
        Envoit un mail d'avertissement aux adresses spécifiées dans la
        configuration.

        :param str text: le texte, encodé en UTF-8, du corps du mail.
        """
        from_addr = self.cfg.get( 'consolidate' , 'mail-from' , 'root' )
        from_name = self.cfg.get( 'consolidate' , 'mail-from-name' , 'Script' )
        to_addresses = [ a.strip( ) for a in self.cfg.get(
                'consolidate' , 'mail-to' , 'root' ).split( ',' ) ]

        from email.message import EmailMessage
        from email.headerregistry import Address

        msg = EmailMessage( )
        msg[ 'Subject' ] = "LDAP/Partage > Problèmes lors de la consolidation"
        msg[ 'To' ] = ', '.join( to_addresses )
        msg[ 'From' ] = Address( from_name , from_addr )
        msg.set_content( text )

        server = self.connect_smtp( )
        if not server: return
        Logging( 'cns' ).info( 'Envoi du rapport d\'erreur' )
        try:
            server.send_message( msg , from_addr = from_addr ,
                    to_addrs = to_addresses )
        except Exception as e:
            Logging( 'cns' ).critical(
                    'Échec de l\'envoi du rapport d\'erreur: {}'.format(
                        repr( e ) ) )
        server.quit( )

    def send_email_report( self , report_name ):
        """
        Génère et envoit un mail d'avertissement.

        :param str report_name: le nom du fichier HTML contenant le rapport \
                complèt
        """
        errors = { k : v for k , v in self.results.items( ) if v.is_error }
        err_eppns = sorted( errors.keys( ) ,
                key = lambda x : errors[ x ].code )

        if report_name is None:
            report_info = 'Par ailleurs, la génération du rapport a échoué.'
        else:
            base = self.cfg.get( 'consolidate' , 'report-url' )
            report_info = '''\
Un rapport détaillé est disponible via l\'URL ci-dessous:
{}{}{}'''.format( base , '' if base.endswith( '/' ) else '/' , report_name )

        text = '''\
Des erreurs ont été détectées lors de la consolidation du système de
synchronisation entre l'annuaire LDAP et le serveur Partage.

{}

Nombre d'EPPNs vérifiés ... {:>4}
Nombre d'erreurs .......... {:>4}

Les erreurs listées ci-dessous ont été détectées.

'''.format( report_info , len( self.results ) , len( errors ) )

        prev_code = -1
        for eppn in err_eppns:
            error = errors[ eppn ]
            if error.code != prev_code:
                hdr = 'Code {:0>2} - {}{}'.format( error.code , error.text ,
                        ' (action corrective: {})'.format( error.action )
                            if error.action is not None
                            else '' )
                fmt = '\n{}\n{:->' + str( len( hdr ) + 1 ) + '}\n'
                text += fmt.format( hdr , '' )
                prev_code = error.code
            text += '  {}\n'.format( eppn )

        self.send_mail( text )

    #---------------------------------------------------------------------------

    def has_failures( self ):
        """
        Vérifie si des problèmes ont été détectés.

        :return: True s'il y a des problèmes, False sinon
        """
        return True in ( s.is_error for s in self.results.values( ) )

    def postprocess( self ):
        """
        Si des problèmes ont été détectés ou que la génération du rapport est
        imposée, crée un rapport au format HTML, puis si nécessaire envoit un
        mail d'avertissement.
        """
        hf = self.has_failures( )
        if hf or self.cfg.has_flag( 'consolidate' , 'always-generate-report' ):
            report_name = self.generate_html_report( )
        else:
            report_name = None
        if hf:
            self.send_email_report( report_name )


#-------------------------------------------------------------------------------


try:
    Consolidator( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )
