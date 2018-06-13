#!/usr/bin/python3

from aolpsync import *


#-------------------------------------------------------------------------------


class Synchronizer( ProcessSkeleton ):
    """
    Cette classe implémente le script de synchronisation principal.
    """

    def cli_description( self ):
        return '''Éffectue la synchronisation depuis l'annuaire LDAP vers le
                  serveur de Partage.'''

    def add_aliases( self , account , aliases ):
        """
        Enregistre de nouveaux aliases pour un compte Partage, en synchronisant
        la base de données au fur et à mesure. Si un alias est déjà présent sur
        le compte, il sera ignoré.

        :param SyncAccount account: le compte auquel les aliases doivent être
        ajoutés
        :param aliases: la liste des aliases à ajouter
        """
        if aliases is None:
            return
        if account.aliases is None:
            account.aliases = set( )
        for alias in aliases:
            if alias in account.aliases:
                continue
            Logging( ).info( 'Ajout alias {} au compte {}'.format(
                    alias , account.mail ) )
            if BSSAction( 'addAccountAlias' , account.mail , alias ):
                account.aliases.add( alias )
                self.save_account( account )
                continue
            Logging( ).error(
                    'Échec d\'ajout de l\'alias {} au compte {}'.format(
                        alias , account.mail ) )

    def remove_aliases( self , account , aliases ):
        """
        Supprime des aliases pour un compte Partage, en synchronisant la base de
        données au fur et à mesure. Si un alias n'est pas défini sur le compte,
        il sera ignoré.

        :param SyncAccount account: le compte auquel les aliases doivent être
        ajoutés
        :param aliases: la liste des aliases à ajouter
        """
        if aliases is None or account.aliases is None:
            return
        for alias in aliases:
            if alias not in account.aliases:
                continue
            Logging( ).info( 'Suppression alias {} au compte {}'.format(
                    alias , account.mail ) )
            if BSSAction( 'removeAccountAlias' , account.mail , alias ):
                account.aliases.remove( alias )
                self.save_account( account )
                continue
            Logging( ).error(
                    'Échec de suppression de l\'alias {} au compte {}'.format(
                        alias , account.mail ) )

    def check_new_account( self , eppn ):
        """
        Tente de créer un nouveau compte Partage. Si la création réussit, le
        compte sera ajouté à la BDD sans alias, puis les aliases seront ajoutés
        un par un.

        :param str eppn: l'EPPN du compte à créer; les informations seront \
                lues depuis l'enregistrement LDAP
        """
        acc = self.ldap_accounts[ eppn ]
        bss_acc = acc.to_bss_account( self.coses )
        pwd_hash = acc.passwordHash.decode( 'ascii' )
        # Création via API
        Logging( ).info( 'Création du compte {}'.format( acc.mail ) )
        if not BSSAction( 'createAccountExt' , bss_acc , pwd_hash ):
            Logging( ).error( 'Impossible de créer le compte {}'.format(
                    acc.mail ) )
            return
        self.new_accounts.add( eppn )
        # On l'ajoute dans la base, sans ses aliases
        aliases = acc.aliases
        acc.aliases = set()
        self.save_account( acc )
        # On tente d'ajouter chaque alias
        self.add_aliases( acc , aliases )

    def check_undelete( self , eppn ):
        """
        Vérifie si un compte marqué comme devant être mis à jour était marqué
        pour suppression. Si c'est le cas, la date de marquage sera
        réinitialisée et le compte sera réactivé via l'API.

        Cette méthode, appelée pendant la séquence de mise à jour des comptes,
        ne modifie pas la base de données. En effet, le compte n'est pas
        réellement réactivé tant qu'il n'a pas été renommé.

        :param str eppn: l'EPPN du compte à vérifier
        :return: un booléen indiquant le succès de l'opération
        """
        dba = self.db_accounts[ eppn ]
        if dba.markedForDeletion is None:
            return True
        Logging( ).info( 'Réactivation du compte {}'.format( dba.mail ) )
        if BSSAction( 'activateAccount' , dba.mail ):
            dba.markedForDeletion = None
            return True
        Logging( ).error( 'Impossible de réactiver le compte {}'.format(
                dba.mail ) )
        return False

    def check_rename( self , eppn ):
        """
        Vérifie si un compte doit être renommé. Si c'est le cas, il sera renommé
        via l'API et la base de données sera mise à jour.

        Cette méthode est appelée pendant la séquence de mise à jour des
        comptes.

        :param str eppn: l'EPPN du compte à vérifier
        :return: un booléen indiquant le succès de l'opération
        """
        dba = self.db_accounts[ eppn ]
        la = self.ldap_accounts[ eppn ]
        if dba.mail == la.mail:
            return True
        Logging( ).info( "{}: à renommer en {}".format( dba.mail , la.mail ) )
        if BSSAction( 'renameAccount' , dba.mail , la.mail ):
            dba.mail = la.mail
            self.save_account( dba )
            return True
        Logging( ).error( 'Impossible de renommer le compte {}'.format(
                dba.mail ) )
        return False

    def check_password_change( self , eppn ):
        """
        Vérifie si le mot de passe d'un compte doit être mis à jour. Si c'est le
        cas, la modification sera effectuée via l'API puis la base de données
        sera mise à jour.

        Cette méthode est appelée pendant la séquence de mise à jour des
        comptes.

        :param str eppn: l'EPPN du compte à vérifier
        :return: un booléen indiquant le succès de l'opération
        """
        dba = self.db_accounts[ eppn ]
        la = self.ldap_accounts[ eppn ]
        if dba.passwordHash == la.passwordHash:
            return True
        Logging( ).info( "{}: mot de passe modifié".format( dba.mail ) )
        nhash = la.passwordHash.decode( 'ascii' )
        if BSSAction( 'modifyPassword' , dba.mail , nhash ):
            dba.passwordHash = la.passwordHash
            self.save_account( dba )
            return True
        Logging( ).error(
                'Impossible de changer le mot de passe pour {}'.format(
                    dba.mail ) )
        return False

    def check_details( self , eppn ):
        """
        Vérifie si les détails d'un compte ont changé. Si c'est le cas, la
        modification sera effectuée via l'API puis la base de données sera mise
        à jour.

        Cette méthode est appelée pendant la séquence de mise à jour des
        comptes.

        :param str eppn: l'EPPN du compte à vérifier
        :return: un booléen indiquant le succès de l'opération
        """
        dba = self.db_accounts[ eppn ]
        la = self.ldap_accounts[ eppn ]
        if not dba.details_differ( la ):
            return True
        Logging( ).info( "{}: détails modifiés".format( dba.mail ) )
        if BSSAction( 'modifyAccount' , la.to_bss_account( self.coses ) ):
            dba.copy_details_from( la )
            self.save_account( dba )
            return True
        Logging( ).error( 'Impossible de modifier les détails de {}'.format(
                dba.mail ) )
        return False

    def check_alias_changes( self , eppn ):
        """
        Vérifie si la liste des aliases correspondant à un compte a changé. Si
        c'est le cas, les modifications sont effectuées de manière incrémentale
        (chaque ajout ou suppression d'alias causera un appel à l'API et, en cas
        de réussite, une synchronisation de la base).

        Cette méthode est appelée pendant la séquence de mise à jour des
        comptes.

        :param str eppn: l'EPPN du compte à vérifier
        :return: un booléen indiquant le succès de l'opération
        """
        dba = self.db_accounts[ eppn ]
        la = self.ldap_accounts[ eppn ]
        if la.aliases is None: la.aliases = set( )
        if dba.aliases is None: dba.aliases = set( )
        if dba.aliases == la.aliases:
            return True
        n = la.aliases - dba.aliases
        o = dba.aliases - la.aliases
        if not ( n or o ):
            Logging( ).error( (
                    'Compte {}: différences trouvées sur les aliases '
                        + 'mais les différences d\'ensembles sont vides. WTF?!'
                    ).format( dba.mail ) )
            return False
        self.add_aliases( dba , n )
        self.remove_aliases( dba , o )
        return True

    def pre_delete( self , eppn ):
        """
        Effectue la pré-suppression d'un compte. Pour cela, le compte sera clos,
        puis un timestamp sera utilisé pour renommer le compte. Si les deux
        opérations réussissent, le compte sera mis à jour dans la base de
        données.

        :param str eppn: l'EPPN du compte à pré-supprimer.
        """
        dba = self.db_accounts[ eppn ]
        assert dba.markedForDeletion is None
        Logging( ).info( 'Compte {}: pré-suppression'.format( dba.mail ) )

        # On supprime les aliases
        if dba.aliases:
            self.remove_aliases( dba , set( dba.aliases ) )

        # On ferme le compte
        if not BSSAction( 'closeAccount' , dba.mail ):
            Logging( ).error( 'Compte {}: échec de la fermeture'.format(
                    dba.mail ) )
            return

        # Puis on le renomme
        import time
        dba.markedForDeletion = int( time.time( ) )
        del_addr = 'del-{}-{}'.format( dba.markedForDeletion , dba.mail )
        if not BSSAction( 'renameAccount' , dba.mail , del_addr ):
            Logging( ).error( 'Compte {}: impossible de renommer en {}'.format(
                    dba.mail , del_addr ) )
            return
        Logging( ).debug( 'Compte {} renommé en {}'.format(
                dba.mail , del_addr ) )

        # Et on sauvegarde
        dba.mail = del_addr
        self.save_account( dba )

    def process( self ):
        """
        Effectue les opérations sur les comptes, en synchronisant la base de
        données au fur et à mesure.
        """
        sdba = set( self.db_accounts.keys( ) )
        sla = set( self.ldap_accounts.keys( ) )

        # Créations de comptes
        new_accounts = sla - sdba
        self.new_accounts = set( )
        Logging( ).info( '{} nouveau(x) compte(s)'.format(
                len( new_accounts ) ) )
        for eppn in new_accounts:
            self.check_new_account( eppn )

        # Mises à jour de comptes existants
        common = sla & sdba
        Logging( ).debug(
                '{} comptes communs entre la BDD et l\'annuaire'.format(
                    len( common ) ) )
        updated = set([ a for a in common
                if self.ldap_accounts[ a ] != self.db_accounts[ a ] ])
        Logging( ).info( '{} compte(s) à mettre à jour'.format(
                len( updated ) ) )
        ops = ( 'undelete' , 'rename' , 'password_change' , 'details' ,
                'alias_changes' )
        d = self.__class__.__dict__
        for eppn in updated:
            for op in ops:
                if not d[ 'check_' + op ].__call__( self , eppn ): break

        # (Pré-)suppressions de comptes
        db_only = sdba - sla
        Logging( ).debug( '{} compte(s) en BDD uniquement'.format(
                len( db_only ) ) )
        deleted = set([ a for a in db_only
                if self.db_accounts[ a ].markedForDeletion is None ])
        Logging( ).info( '{} compte(s) à pré-supprimer'.format(
                len( deleted ) ) )
        for eppn in deleted:
            self.pre_delete( eppn )

    def postprocess( self ):
        """
        Si des comptes ont été créés sur le serveur, tente d'ajouter les emplois
        du temps correspondants.
        """
        if not self.new_accounts or not self.cfg.has_section( 'calendars' ):
            return
        Logging( 'sync' ).info(
                'Synchronisation des calendriers pour les nouveaux comptes' )
        CalendarSync( self.cfg ).synchronize( self.db_accounts ,
                self.new_accounts )


#-------------------------------------------------------------------------------


try:
    Synchronizer( )
except FatalError as e:
    import sys
    Logging( ).critical( str( e ) )
    sys.exit( 1 )
