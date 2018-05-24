from .configuration import Config
from .logging import Logging


class AliasCommands:

    def __init__( self , cfg ):
        s = cfg.get_section( 'aliases' )
        self.commands = dict( )
        for cn in s:
            self.commands[ cn ] = s[ cn ].replace(
                    '!configdir!' , Config.CONFIG_DIR )
            Logging( 'alias' ).debug(
                    'Commande de liste d\'aliases {}: {}'.format(
                        cn , self.commands[ cn ] ) )

    def get_aliases( self ):
        if not hasattr( self , 'fetched_' ):
            self.fetched_ = self.fetch_( )
        return self.fetched_

    def fetch_( self ):
        import subprocess
        aliases = {}
        for command in self.commands:
            Logging( 'alias' ).info( 'Récupération des aliases: {}'.format(
                    command ) )

            child = subprocess.Popen( self.commands[ command ] ,
                    shell = True ,
                    stdout = subprocess.PIPE ,
                    stderr = subprocess.PIPE )
            ev = child.wait( )
            output = [ l for l in child.stdout ]

            if ev != 0:
                Logging( 'alias' ).error(
                    'Erreur lors de l\'exécution de `{}`: {}'.format(
                        self.commands[ command ] , ev ) )
                dump_err = lambda l : Logging( 'alias' ).error( l )
            else:
                dump_err = lambda l : Logging( 'alias' ).warning( l )
            for l in child.stderr:
                dump_err( l )
            if ev != 0:
                continue

            self.process_alias_lines( aliases , output )
        return aliases

    def process_alias_lines( self , aliases , output ):
        import re
        for line in output:
            try:
                line = line.decode( 'utf-8' )
            except UnicodeDecodeError:
                Logging( 'alias' ).error( 'Contenu non-UTF-8' )
                return
            line = re.sub( r'#.*$' , '' , line ).strip( )
            if not line: continue
            bits = line.split( ':' )

            if len( bits ) != 2:
                if ( len( bits ) != 4 or bits[ 1 ] != ''
                        or bits[ 2 ] != 'include' ):
                    ws = 'Ligne avec format inconnu: {}'.format( line )
                else:
                    ws = 'Fichier inclus ignoré: {}'.format( bits[ 3 ] )
                Logging( 'alias' ).warning( ws )
                continue

            ( alias , addresses ) = bits
            if alias in aliases:
                Logging( 'alias' ).warning(
                        'Alias {}: doublon'.format( alias ) )
                continue
            aliases[ alias ] = set(
                    re.sub( r'\s+' , '' , addresses ).split( ',' ) )
            Logging( 'alias' ).debug( 'Alias {} lu -> {}'.format(
                    alias , ', '.join( aliases[ alias ] ) ) )


#-------------------------------------------------------------------------------


class AliasesMap:
    """
    Cette classe permet de représenter et mettre à jour la liste des aliases.
    """

    def __init__( self , cfg , accounts ):
        """
        Initialise la liste des aliases en se basant sur la liste de comptes
        fournie.

        :param accounts: les comptes à traiter
        """
        self.aliases_ = {}
        self.reverseAliases_ = {}
        mail_domain = '@{}'.format( cfg.get( 'ldap' , 'mail-domain' ) )
        # Initialisation
        for eppn in accounts:
            account = accounts[ eppn ]
            if ( account.ldapMail is None or account.ldapMail == eppn
                    or not account.ldapMail.endswith( mail_domain ) ):
                continue
            self.add_alias( eppn , account.ldapMail )

        # Charge les aliases supplémentaires
        extra_aliases = cfg.alias_commands( ).get_aliases( )
        mail_dom = '@{}'.format( cfg.get( 'ldap' , 'mail-domain' ) )
        for alias in extra_aliases:
            targets = extra_aliases[ alias ]
            alias = alias + mail_dom
            if len( targets ) != 1:
                Logging( 'alias' ).info( 'Alias multiple {} ignoré'.format(
                        alias ) )
                continue
            target = tuple( targets )[ 0 ]
            if '@' not in target:
                target = target + mail_dom
            if not target.endswith( mail_dom ):
                Logging( 'alias' ).info( 'Alias externe {} ignoré'.format(
                        alias ) )
                continue
            self.add_alias( target , alias )

        # Vérification
        adn = self.get_aliased_accounts( ) - set([
                a.mail for a in accounts.values( ) ])
        if len( adn ):
            Logging( 'ldap' ).warning( 'Alias définis sans compte cible: '
                    + ','.join( adn ) )
        Logging( 'ldap' ).info( '{} aliases définis'.format(
            len( self.aliases_ ) ) )

    def add_alias( self , target , alias ):
        """
        Ajoute un alias.

        :param str target: l'adresse cible de l'alias
        :param str alias: l'alias lui-même
        :raises AliasError: si une boucle infinie ou un doublon sont détectés
        """
        Logging( 'ldap' ).debug( 'Alias {} -> {}'.format( alias , target ) )
        # Si la cible spécifiée est un alias, on récupère sa destination
        oriTarget = target
        while target in self.aliases_:
            target = self.aliases_[ target ]
            if target == oriTarget:
                raise AliasError( "{}: boucle infinie".format( target ) )

        # Cible et alias identiques -> rien à faire
        if target == alias:
            return

        # Doublon?
        if alias in self.aliases_:
            if self.aliases_[ alias ] == target:
                return
            raise AliasError( "{}: doublon (ancien {}, nouveau {})".format(
                            alias , self.aliases_[ alias ] , target ) )

        # On ajoute le nouvel alias et son mapping inverse
        if target not in self.reverseAliases_:
            self.reverseAliases_[ target ] = set( )
        self.aliases_[ alias ] = target
        self.reverseAliases_[ target ].add( alias )

        # Si le nouvel alias figure dans les mapping inverse, on remplace tous
        # les alias pointant vers celui-ci par un alias pointant vers la
        # nouvelle cible.
        if alias in self.reverseAliases_:
            for old_alias in self.reverseAliases_[ alias ]:
                if old_alias == target:
                    self.aliases_.pop( old_alias )
                else:
                    self.aliases_[ old_alias ] = target
            self.reverseAliases_[ target ].update(
                    self.reverseAliases_[ alias ] )
            self.reverseAliases_.pop( alias )

    def get_aliased_accounts( self ):
        """
        Renvoie l'ensemble des adresses en directions desquelles un alias
        existe.

        :return: l'ensemble des adresses cible
        """
        return set( self.reverseAliases_.keys( ) )

    def getAllAliases( self ):
        """
        :return: l'ensemble des aliases
        """
        return set( self.aliases_.keys( ) )

    def get_main_account( self , address ):
        """
        Tente de récupérer l'adresse réelle correspondant à une adresse. Si un
        alias correspondant existe, l'adresse cible de cet alias sera renvoyée;
        dans le cas contraire, l'adresse spécifiée sera renvoyée sans autre
        vérification.

        :param str address: l'adresse à examiner
        :return: le compte correspondant à l'adresse
        """
        if address in self.aliases_:
            return self.aliases_[ address ]
        return address

    def get_aliases( self , address ):
        """
        Récupère l'ensemble des aliases pour un compte donné.

        :param str address: l'adresse du compte
        :return: l'ensemble des aliases définis
        """
        if address not in self.reverseAliases_:
            if address in self.aliases_:
                raise AliasError( '{}: est un alias'.format( address ) )
            return set()
        return set( self.reverseAliases_[ address ] )