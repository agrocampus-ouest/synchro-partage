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

    def __init__( self , is_err , code , text , action = None ):
        self.is_error = is_err
        self.code = code
        self.text = text
        self.action = action


class Consolidator( ProcessSkeleton ):

    def cli_description( self ):
        return '''Tente de consolider les données présentes dans la base locale
                  à partir des informations du serveur de Renater.

                  Ce script devrait être utilisé afin de s'assurer que les
                  modifications effectuées à partir de l'interface Web ne sont
                  pas perdues, et aussi pour vérifier la cohérence des diverses
                  sources d'information.'''

    #---------------------------------------------------------------------------

    def preinit( self ):
        AS = AccountState
        self.states = { a.code : a for a in (
            # États OK
            AS( False ,  1 , 'LDAP+ DB+ BSS+' ) ,
            AS( False ,  2 , 'LDAP- DB~ BSS~' ) ,
            AS( False ,  3 , 'LDAP- DB- / BSS+' ) ,
            # États en attente de synchro
            AS( False , 10 , 'LDAP+ / DB- BSS-' ) ,
            AS( False , 11 , 'LDAP+ / DB+ BSS+' ) ,
            AS( False , 12 , 'LDAP- / DB+ BSS+' ) ,
            AS( False , 13 , 'LDAP+ / DB~ BSS~' ) ,
            # États erronés avec correction auto
            AS(  True , 20 , 'LDAP+ DB+ / BSS-' , 'suppr. base' ) ,
            AS(  True , 21 , 'LDAP+ / DB+ / BSS-' , 'suppr. base' ) ,
            AS(  True , 22 , 'LDAP+ BSS+ / DB-' , 'ajout base' ) ,
            AS(  True , 23 , 'LDAP- BSS~ / DB-' , 'ajout base' ) ,
            AS(  True , 24 , 'LDAP- DB~ / BSS~' , 'm.a.j. base' ) ,
            # États erronés sans correction auto
            AS(  True , 30 , 'LDAP+ / DB- / BSS+' ) ,
            AS(  True , 31 , 'LDAP- DB~ / BSS+' ) ,
            AS(  True , 32 , 'LDAP- BSS~ / DB+' ) ,
            AS(  True , 33 , 'LDAP+ DB+ / BSS+' ) ,
            AS(  True , 34 , 'LDAP+ / DB? / BSS?' ) ,
            AS(  True , 35 , 'LDAP- / DB+ / BSS+' ) ,
            # Erreur interne / aucun résultat de vérif.
            AS(  True , 98 , 'Erreur interne' ) ,
            AS(  True , 99 , 'Pas de résultat' ) ,
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
        failed = False
        accounts = {}

        for mail in self.list_bss_accounts( ):
            # Lecture
            qr = BSSAction( BSSQuery( 'getAccount' ) , mail )
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

        if not failed:
            Logging( 'bss' ).info( '{} comptes lus'.format( len( accounts ) ) )
        return None if failed else accounts

    def init( self ):
        self.bss_accounts = self.fetch_bss_data( )
        if self.bss_accounts is None:
            raise FatalError( 'Échec de la lecture de la liste des comptes' )

    #---------------------------------------------------------------------------

    def list_eppns_in( self , **kwargs ):
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
        for eppn in self.list_eppns_in( **kwargs ):
            if eppn in self.checked:
                continue
            self.run_account_check( eppn , func )

    def result( self , eppn , code ):
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

    def run_checks( self ):
        # Vérification comptes existants dans les 3 bases
        #               (cas 01, 11, 13, 33, 34)
        def check_common_accounts_( eppn , ldap , db , bss ):
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

        # Vérification des comptes présents sur DB et BSS, mais pas en LDAP
        #               (cas 02, 12, 24, 31, 32, 35)
        def check_noldap_accounts_( eppn , ldap , db , bss ):
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

        # Comptes figurant dans le LDAP et dans le BSS, mais pas dans la base.
        #               (cas 22, 30)
        def check_nodb_accounts_( eppn , ldap , db , bss ):
            if ldap.bss_equals( bss ):
                opw = ldap.passwordHash
                ldap.passwordHash = b'{SSHA}invalide'
                self.save_account( ldap )
                ldap.passwordHash = opw
                self.result( eppn , 22 )
            else:
                self.result( eppn , 30 )

        # Comptes figurant dans le LDAP et la DB, mais pas sur le BSS.
        #               (cas 20, 21)
        def check_nobss_accounts_( eppn , ldap , db , bss ):
            if ldap == db:
                self.result( eppn , 20 )
            else:
                self.result( eppn , 21 )
            self.remove_account( ldap )

        # Comptes figurant uniquement dans la base de données
        #               (cas 25)
        def handle_dbonly_accounts_( eppn , ldap , db , bss ):
            self.remove_account( db )
            self.result( eppn , 25 )

        # Comptes figurant uniquement sur le serveur Partage
        #               (cas 03, 23)
        def check_bssonly_accounts_( eppn , ldap , db , bss ):
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

    def has_failures( self ):
        return True in ( s.is_error for s in self.results.values( ) )

    #---------------------------------------------------------------------------

    def connect_smtp( self ):
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

    def send_email_report( self ):
        errors = { k : v for k , v in self.results.items( ) if v.is_error }
        err_eppns = sorted( errors.keys( ) ,
                key = lambda x : errors[ x ].code )

        text = '''\
Des erreurs ont été détectées lors de la consolidation du système de
synchronisation entre l'annuaire LDAP et le serveur Partage.

Nombre d'EPPNs vérifiés ... {:>4}
Nombre d'erreurs .......... {:>4}

Les erreurs listées ci-dessous ont été détectées.

'''.format( len( self.results ) , len( errors ) )

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

    def generate_html_report( self ):
        pass

    def process( self ):
        self.checked = set( )
        self.results = dict( )
        self.run_checks( )
        hf = self.has_failures( )
        if hf:
            self.send_email_report( )
        if hf or cfg.has_flag( 'consolidate' , 'always-generate-report' ):
            self.generate_html_report( )


#-------------------------------------------------------------------------------


try:
    Consolidator( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )
