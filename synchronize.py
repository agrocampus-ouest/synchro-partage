#!/usr/bin/python3

import json , lmdb , time , ldap3 , sys , os.path
from configparser import ConfigParser


class AttributeDefError( Exception ):
    """
    Exception utilisée pour représenter un problème de définition d'attribut
    supplémentaire (par exemple un doublon ou une référence à un attribut
    inexistant).
    """
    pass

class AccountStateError( Exception ):
    """
    Exception utilisée pour indiquer qu'une opération a été demandée sur des
    données dont l'état est incompatible avec l'opération.
    """
    pass

class AliasError( Exception ):
    """
    Exception utilisée pour représenter un problème avec les aliases.
    """
    pass

class FatalError( Exception ):
    """
    Une exception indiquant qu'un message devrait être écrit dans le log et que
    l'exécution devrait se terminer.
    """
    pass

class RuleError( Exception ):
    """
    Une exception qui indique qu'une règle d'attribution de classe de service
    est incorrectement configurée.
    """
    pass

#-------------------------------------------------------------------------------

def json_load( data ):
    """
    Charge des données JSON contenant éventuellement des ensembles ou des
    tableaux d'octets.

    :param str data: le JSON à décoder
    :return: les données décodées
    :raises TypeError: des données étendues (ensembles, octets) n'ont pas pu   \
            décodées car le type spécifié est invalide
    """
    def json_decoder_( dct ):
        """
        Décodeur JSON permettant de récupérer, en plus des types habituels,
        des tableaux d'octets (type Python bytes) ou des ensembles (type Python
        set)
        """
        if '__ext__' not in dct:
            return dct
        ct = dct[ '__ext__' ]
        if ct == 'bytes':
            return bytes( dct[ 'data' ] )
        elif ct == 'set':
            return set( dct[ 'data' ] )
        raise TypeError( ct )
    return json.loads( data , object_hook = json_decoder_ )


def json_dump( data ):
    """
    Sérialise des données contenant éventuellement des ensembles ou des données
    binaires vers du JSON.

    :param data: les données à encoder
    :return: les données sous la forme de JSON
    """
    class JSONSetEncoder_( json.JSONEncoder ):
        """
        Encodeur JSON qui transforme les ensembles et les binaires en
        dictionnaires contenant un champ '__ext__' et les données
        correspondantes.
        """
        def default( self , obj ):
            if type( obj ) in ( set , bytes ):
                return { '__ext__' : type( obj ).__name__ ,
                        'data' : list( obj ) }
            return json.JSONEncoder.default( self , obj )
    return json.dumps( data ,
            separators = ( ',' , ':' ) ,
            cls = JSONSetEncoder_ )

#-------------------------------------------------------------------------------

class Logging:
    """
    Classe qui est utilisée pour configurer et récupérer les loggers que l'on
    utilise dans le reste du script.
    """

    FILE_NAME = 'partage-sync-logging.ini'

    DEFAULT_CONFIG = {
            'version' : 1 ,
            'disable_existing_loggers' : False ,
            'formatters' : {
                'normal' : {
                    'format' : '%(asctime)s %(levelname)-8s %(name)-15s %(message)s' ,
                } ,
            } ,
            'handlers' : {
                'console' : {
                    'level' : 'ERROR' ,
                    'class' : 'logging.StreamHandler' ,
                    'formatter' : 'normal' ,
                } ,
            } ,
            'loggers' : {
                'root' : {
                    'handlers' : [] ,
                    'propagate' : False ,
                } ,
                'psync' : {
                    'handlers' : [ 'console' ] ,
                    'propagate' : False ,
                    'level' : 'INFO' ,
                } ,
            }
    }

    def __new__( self , name = None ):
        """
        Essaie de récupérer un logger avec le nom spécifié sous la hiérarchie
        "psync". S'il s'agit du premier appel à cette méthode, la configuration
        par défaut sera mise en place puis, s'il existe, le fichier de
        configuration sera lu.

        :param name: le nom du logger (ou None pour utiliser psync)
        :return: le logger
        """
        if not hasattr( self , 'configured_' ):
            import logging.config
            logging.config.dictConfig( Logging.DEFAULT_CONFIG )
            try:
                with open( Logging.FILE_NAME , 'r' ) as cfg:
                    logging.config.fileConfig( cfg ,
                            disable_existing_loggers = False )
            except FileNotFoundError:
                pass
            except ( KeyError , ValueError ) as e:
                logging.getLogger( 'psync' ).error(
                        'Erreurs dans la configuration du journal' ,
                        exc_info = e )
            Logging.configured_ = True

        if name is None:
            name = 'psync'
        else:
            name = 'psync.' + name
        import logging
        return logging.getLogger( name )


#-------------------------------------------------------------------------------

class RuleParser:
    """
    Implémentation de l'extraction des règles d'assignement de classe de
    service.
    """

    class ConstantChecker:
        """
        Vérificateur qui renvoit une valeur prédéfinie (vrai ou faux)
        """
        def __init__( self , word ):
            self.value = ( word == 'true' )
        def check( self , account ):
            return self.value
        def __repr__( self ):
            return 'true' if self.value else 'false'

    class AttrValueChecker:
        """
        Vérification de la valeur d'un attribut (règles ne et eq)
        """
        def __init__( self , word , attr_name , value ):
            self.eq = ( word == 'eq' )
            self.attr_name = attr_name
            self.value = value
            if attr_name not in SyncAccount.STORAGE:
                raise RuleError( 'Attribut {} inexistant'.format( attr_name ) )
        def check( self , account ):
            val = getattr( account , self.attr_name )
            if not isinstance( val , str ):
                return False
            return self.eq == ( val == self.value )
        def __repr__( self ):
            return '({} {} {})'.format(
                    'eq' if self.eq else 'ne' ,
                    self.attr_name , self.value )

    class AttrNoneChecker:
        """
        Vérification du fait qu'un attribut soit vide.
        """
        def __init__( self , word , attr_name ):
            self.attr_name = attr_name
            if attr_name not in SyncAccount.STORAGE:
                raise RuleError( 'Attribut {} inexistant'.format( attr_name ) )
        def check( self , account ):
            v = getattr( account , self.attr_name )
            return v is None or not v
        def __repr__( self ):
            return '(empty {})'.format( self.attr_name )

    class AttrContainsChecker:
        """
        Vérifie si un attribut de type liste ou ensemble contient une valeur.
        Si l'attribut est une simple chaîne, cet opérateur est équivallent à
        l'opérateur "eq".
        """
        def __init__( self , word , attr_name , value ):
            self.attr_name = attr_name
            self.value = value
            if attr_name not in SyncAccount.STORAGE:
                raise RuleError( 'Attribut {} inexistant'.format( attr_name ) )
        def check( self , account ):
            v = getattr( account , self.attr_name )
            if v is None: return False
            if isinstance( v , str ): return v == self.value
            return self.value in v
        def __repr__( self ):
            return '(contains {} {})'.format( self.attr_name , self.value )

    class LogicalNotChecker:
        """
        Inversion d'une condition
        """
        def __init__( self , word , rule ):
            self.rule = rule
        def check( self , account ):
            return not self.rule.check( account )
        def __repr__( self ):
            return '(not {})'.format( repr( self.rule ) )

    class LogicalBinaryChecker:
        """
        Opération logique binaire (opérateurs and, or et xor)
        """
        def __init__( self , word , rule1 , rule2 , *rules ):
            self.word = word
            if word == 'and':
                self.check_op = lambda r : False not in r
            elif word == 'or':
                self.check_op = lambda r : True in r
            else: # word == 'xor'
                self.check_op = lambda r : 1 == len([
                        x for x in r if r ])
            self.rules = ( rule1 , rule2 ) + rules
        def check( self , account ):
            checks = [ r.check( account ) for r in self.rules ]
            return self.check_op( checks )
        def __repr__( self ):
            return '({} {})'.format( self.word ,
                    ' '.join([ repr( r ) for r in self.rules ] ) )

    # Opérateurs supportés; dictionnaire avec le format suivant:
    #   'texte' : ( Classe , 'forme' ) ,
    # La forme est une chaîne contenant les lettres W et R pour indiquer un
    # mot ou une règle, respectivement. Si elle finit par '+', le dernier
    # élément peut être répété.
    OPS = {
        'true' : ( ConstantChecker , '' ) ,
        'false' : ( ConstantChecker , '' ) ,
        'eq' : ( AttrValueChecker , 'WW' ) ,
        'ne' : ( AttrValueChecker , 'WW' ) ,
        'empty' : ( AttrNoneChecker , 'W' ) ,
        'contains' : ( AttrContainsChecker , 'WW' ) ,
        'not' : ( LogicalNotChecker , 'R' ) ,
        'and' : ( LogicalBinaryChecker , 'RR+' ) ,
        'or' : ( LogicalBinaryChecker , 'RR+' ) ,
        'xor' : ( LogicalBinaryChecker , 'RR+' ) ,
    }

    def __init__( self , name , rule ):
        """
        Extrait la règle depuis la chaîne spécifiée.

        :param str name: le nom de la règle, à afficher en cas d'erreur
        :param str rule: le texte de la règle
        """
        self.name = name
        self.rule = rule
        ast = self.read_ast_(  )
        self.out = self.rdp_( ast )

    def rdp_( self , ast ):
        """
        Analyseur grammatical par descente récursive qui génère la règle en se
        basant sur l'arbre abstrait.

        :param list ast: la liste correspondant à la règle à transformer
        :return: un objet avec une méthode check() correspondant à la règle
        """
        def parse_error_( text ):
            """
            Génère une erreur de syntaxe

            :param str text: le texte de l'erreur
            :return: une RuleError pouvant être levée
            """
            return RuleError( 'Règle {}, erreur de syntaxe: {}'.format(
                    self.name , text ) )

        if not len( ast ):
            raise parse_error_( 'liste vide' )
        if not isinstance( ast[ 0 ] , str ):
            raise parse_error_( 'liste ne commençant pas par un mot' )
        if ast[ 0 ] not in RuleParser.OPS:
            raise parse_error_( 'opérateur {} inconnu'.format( ast[ 0 ] ) )
        ( cls , pattern ) = RuleParser.OPS[ ast[ 0 ] ]
        pos = 1
        args = []
        while pos < len( ast ):
            ppos = min( pos , len( pattern ) ) - 1
            e_type = pattern[ ppos ]
            if e_type == '+':
                e_type = pattern[ ppos - 1 ]
            elif pos - 1 >= len( pattern ):
                raise parse_error_(
                        'opérateur {}: seulement {} opérande(s) attendue(s)'
                            .format( ast[ 0 ] , len( pattern ) ) )

            check = ast[ pos ]
            if e_type == 'W' and not isinstance( check , str ):
                raise parse_error_(
                        'opérateur {}, opérande {}, mot attendu'.format(
                            ast[ 0 ] , pos ) )
            elif e_type == 'R' and not isinstance( check , list ):
                raise parse_error_(
                        'opérateur {}, opérande {}, liste attendue'.format(
                            ast[ 0 ] , pos ) )
            if e_type == 'R':
                check = self.rdp_( check )
            args.append( check )
            pos = pos + 1
        return cls( ast[ 0 ] , *args )

    def read_ast_( self ):
        """
        Transforme le texte de la règle en un arbre syntaxique abstrait.

        :return: la racine de l'arbre
        """
        pos = 0
        def rule_error_( text ):
            return RuleError( 'Règle {}, caractère {}: {}'.format(
                    self.name , pos , text ) )

        # On transforme la règle en un AST
        state = 0
        accum = ''
        ast = []
        stack = []
        started = -1
        while pos < len( self.rule ):
            char = self.rule[ pos ]
            pos = pos + 1
            # État -1: on veut en finir :'(
            if state == -1:
                if not char.isspace( ):
                    raise rule_error_( 'caractère après fin de règle' )
            # État 0: on attend une liste ou du blanc
            elif state == 0:
                if char == '(':
                    state = 1
                    stack = [ ast ]
                elif not char.isspace( ):
                    raise rule_error_( 'parenthèse ouvrante attendue' )
            # État 1: on attend des caractères alpha ou du blanc
            elif state == 1:
                if char.isalpha( ):
                    accum = char
                    state = 2
                elif not char.isspace( ):
                    raise rule_error_( 'nom attendu' )
            # État 2: on attend des caractères alpha, du blanc, ou une
            # parenthèse.
            elif state == 2:
                if char.isalnum( ) or char in '_-':
                    accum += char
                    continue
                stack[ -1 ].append( accum )
                accum = ''
                if char.isspace( ):
                    state = 3
                elif char == ')':
                    stack = stack[ :-1 ]
                    if stack:
                        state = 3
                    else:
                        state = -1
                elif char == '(':
                    stack[ -1 ].append( [] )
                    stack.append( stack[ -1 ][ -1 ] )
                    state = 1
                else:
                    raise rule_error_( 'caractère invalide' )
            # État 3: on attend un autre mot ou une parenthèse
            elif state == 3:
                if char.isspace( ):
                    continue
                if char.isalpha( ):
                    state = 2
                    accum = char
                elif char == '"':
                    state = 4
                elif char == ')':
                    stack = stack[ :-1 ]
                    if not stack:
                        state = -1
                elif char == '(':
                    stack[ -1 ].append( [] )
                    stack.append( stack[ -1 ][ -1 ] )
                    state = 1
                else:
                    raise rule_error_( 'lettre, \'"\' ou parenthèse attendu' )
            # État 4: on est dans une chaîne de caractères délimitée
            elif state == 4:
                if char == '"':
                    state = 5
                else:
                    accum += char
            # État 5: une chaîne de caractères vient de se terminer. On
            # attend un espace, une parenthèse ou encore une autre chaîne.
            elif state == 5:
                if char == '"':
                    accum += '"'
                    state = 4
                else:
                    stack[ -1 ].append( accum )
                    accum = ''
                    # On utilise l'état 3 pour vérifier la suite, car on a
                    # déjà éliminé la possibilité de '"'
                    pos -= 1
                    state = 3

        if len( stack ):
            raise rule_error_( 'parenthèse(s) fermante(s) requise(s)' )
        return ast


class Rule:
    """
    Règle utilisable pour déterminer les classes de services associées aux
    comptes.
    """

    def __new__( cls , name , rule ):
        """
        Crée une règle, sous la forme d'un objet implémentant une méthode
        check(), à partir d'une chaîne de caractères.

        :param str name: le nom de la règle
        :param str rule: le texte de la règle
        :return: la règle 'compilée'
        """
        parsed = RuleParser( name , rule ).out
        Logging( 'cfg' ).debug( 'Régle {} lue: {}'.format(
                name , repr( parsed ) ) )
        return parsed

#-------------------------------------------------------------------------------

class LDAPAttr:
    """
    Décrit un attribut pouvant être importé ou généré à partir des
    informations contenues dans l'annuaire LDAP.

    Une description d'attribut peut indiquer une source LDAP, une
    fonction de génération, ou encore les deux.

    Si seule une source LDAP est indiquée, la valeur sera lue depuis le
    champ correspondant.

    Si seule une fonction de génération est spécifiée, la valeur sera
    systématiquement générée par la fonction.

    Si les deux paramètres sont présents, l'attribut LDAP sera utilisé
    en priorité, et la fonction ne sera appelée que si l'attribut est
    manquant.
    """

    def __init__( self , local , ldap = None , gen = None , opt = False ):
        """
        Initialise la description d'attribut.

        :param str local: le nom de l'attribut
        :param ldap: le nom de l'attribut LDAP à lire, ou None si \
                l'attribut doit être généré
        :param gen: une fonction qui peut transformer les données en \
                provenance du LDAP afin de générer la valeur du champ.
        :param bool opt: indique si l'attribut est optionel.
        """
        assert isinstance( local , str )
        assert ldap is None or isinstance( ldap , str )
        assert gen is None or callable( gen )
        self.local = local
        if ldap is None:
            self.ldap = local
        else:
            self.ldap = ldap
        self.gen = gen
        self.optional = opt

    def __call__( self , syncAccount , ldapEntry ):
        """
        Lit ou génère la valeur de l'attribut depuis une entrée LDAP puis
        la stocke dans une instance de compte de synchronisation.

        :param SyncAccount syncAccount: l'instance de compte de \
                synchronisation vers laquelle les données seront stockées.
        :param ldap3.Entry ldapEntry: l'entrée LDAP depuis laquelle les \
                données seront extraites.
        :raises AttributeError: un attribut non optionnel n'a pas été \
                trouvé dans l'entrée LDAP d'origine et/ou n'a pas pu \
                être généré.
        """
        value = None
        if self.ldap != '':
            value = getattr( ldapEntry , self.ldap , None )
        if value is not None:
            value = value.values
            l = len( value )
            if l == 1:
                value = value[0]
            elif l == 0:
                value = None
        elif self.gen is not None:
            value = self.gen( ldapEntry )
        if value is None and not self.optional:
            raise AttributeError( self.local )
        setattr( syncAccount , self.local , value )

#-------------------------------------------------------------------------------

class SyncAccount:
    """
    Classe servant à représenter un compte devant être synchronisé entre le LDAP
    et le serveur Partage.
    """

    # Attributs devant être stockés.
    STORAGE = None
    # Attributs LDAP
    LDAP = None
    # Correspondances BSS -> champs locaux
    BSS = None
    # Liste des champs de détail
    DETAILS = None

    @staticmethod
    def init_storage_( cfg ):
        """
        Initialise la liste des attributs à stocker en ajoutant aux attributs
        par défauts les attributs en provenance de la configuration.

        :param Config cfg: la configuration
        :raises AttributeDefError: un attribut configuré a le même nom que \
                l'un des attributs par défaut
        """
        assert SyncAccount.STORAGE is None

        # Attributs par défaut
        attrs = set([
            'eppn' , 'surname' , 'givenName' ,
            'displayName' , 'mail' , 'passwordHash' , 'groups' ,
            'ldapMail' , 'markedForDeletion' , 'aliases' , 'cos'
        ])

        # Attributs configurés
        for ea in cfg.get_list( 'extra-attributes' , () ):
            if ea in attrs:
                raise AttributeDefError( 'Attribut {}: doublon'.format( ea ) )
            attrs.add( ea )
        Logging( 'cfg' ).debug( 'Attributs définis: ' + ', '.join( attrs ) )
        SyncAccount.STORAGE = attrs

    @staticmethod
    def init_ldap_attrs_( cfg ):
        """
        Initialise les attributs LDAP et les convertisseurs associés. Une partie
        de cette configuration est mise en place systématiquement, puis des
        attributs supplémentaires sont lus depuis la section
        ldap-extra-attributes du fichier de configuration.

        :param Config cfg: la configuration
        :raises AttributeDefError: si un attribut listé dans la configuration \
                porte le même nom que l'un des attributs par défaut, ou bien \
                si un attribut n'est pas défini.
        """
        eppn_dom = cfg.get( 'ldap' , 'eppn-domain' )
        mail_dom = cfg.get( 'ldap' , 'mail-domain' )
        extra_attrs = cfg.get_section( 'ldap-extra-attributes' , True )

        # On génère la liste des attributs par défaut
        LA = LDAPAttr
        ldap_attrs = [
            LA( 'eppn' , 'eduPersonPrincipalName' ,
                gen = lambda e : "{}@{}".format( str( e.uid ) , eppn_dom ) ) ,
            LA( 'mail' , '' ,
                gen = lambda e : "{}@{}".format( str( e.uid ) , mail_dom ) ) ,
            LA( 'surname' , 'sn' ) ,
            LA( 'givenName' ) ,
            LA( 'displayName' ,
                gen = lambda e : "{} {}".format( str( e.givenName ) ,
                        str( e.sn ) ) ) ,
            LA( 'ldapMail' , 'mail' ) ,
            LA( 'passwordHash' , 'userPassword' ) ,
        ]

        # On rajoute les attributs configurés
        defined_attrs = set([ a.local for a in ldap_attrs ])
        for ea in extra_attrs:
            if ea in defined_attrs:
                raise AttributeDefError( 'Attribut {}: doublon'.format( ea ) )
            if ea not in SyncAccount.STORAGE:
                raise AttributeDefError(
                        'Attribut {}: non défini'.format( ea ) )
            ldap_attrs.append( LA( ea , extra_attrs[ ea ] , opt = True ) )

        SyncAccount.LDAP = tuple( ldap_attrs )

    @staticmethod
    def init_bss_attrs_( cfg ):
        """
        Initialise la liste des correspondances entre les champs de
        synchronisation et les champs de l'API BSS. Établit par ailleurs la
        liste des champs "de détail".

        :param Config cfg: l'instance de configuration
        :raises AttributeDefError: l'un des champs personnalisés est en fait \
                un champ par défaut ou ne correspond à aucun champ local
        """
        # On génère le dictionnaire des attributs et la liste des champs de
        # détail par défaut.
        details = set([ 'surname' , 'givenName' , 'displayName' , 'cos' ])
        bss_attrs = {
            'carLicense' : 'eppn' ,
            'sn' : 'surname' ,
            'givenName' : 'givenName' ,
            'displayName' : 'displayName' ,
        }

        # On y ajoute les attributs supplémentaires
        for ea in cfg.get_section( 'bss-extra-attributes' , True ):
            if ea not in SyncAccount.STORAGE:
                raise AttributeDefError(
                        'Attribut {}: non défini'.format( ea ) )
            bss_ea = cfg.get( 'bss-extra-attributes' , ea , ea )
            if bss_ea in bss_attrs:
                raise AttributeDefError(
                        'Attribut {}: doublon'.format( ea ) )
            bss_attrs[ bss_ea ] = ea
            details.add( ea )

        Logging( 'cfg' ).debug( 'Champs de détail: ' + ', '.join( details ) )
        SyncAccount.DETAILS = tuple( details )
        Logging( 'cfg' ).debug( 'Correspondances BSS: ' + ', '.join([
            '{} -> {}'.format( x , bss_attrs[ x ] )
                for x in bss_attrs ]) )
        SyncAccount.BSS = bss_attrs

    #---------------------------------------------------------------------------

    def __init__( self , cfg ):
        """
        Initialise les données de synchronisation en initialisant tous les
        attributs à None.

        :param Config cfg: la configuration
        """
        if SyncAccount.STORAGE is None:
            SyncAccount.init_storage_( cfg )
            SyncAccount.init_ldap_attrs_( cfg )
            SyncAccount.init_bss_attrs_( cfg )
        self.clear( )

    #---------------------------------------------------------------------------

    def clear( self ):
        """
        Réinitialise tous les attributs à None.
        """
        for attr in SyncAccount.STORAGE:
            setattr( self , attr , None )

    def from_ldap_entry( self , entry ):
        """
        Initialise les attributs à partir d'une entrée LDAP.

        :param ldap3.Entry entry: l'entrée LDAP depuis laquelle les données \
                seront lues
        :return: l'instance de synchronisation
        """
        self.clear( )
        for attr in self.LDAP:
            attr( self , entry )
        return self

    def from_json( self , data ):
        """
        Initialise les attributs à partir d'un enregistrement JSON.

        :param str data: l'enregistrement JSON

        :return: l'instance de synchronisation
        """
        self.clear( )
        d = json_load( data )
        for a in SyncAccount.STORAGE:
            if a in d:
                v = d[ a ]
            else:
                v = None
            setattr( self , a , v )
        return self

    def copy_details_from( self , other ):
        """
        Copie les champs de détails d'un compte vers l'instance actuelle.

        :param SyncAccount other: l'instance depuis laquelle on veut copier \
                les champs de détails
        """
        for d in SyncAccount.DETAILS:
            setattr( self , d , getattr( other , d ) )

    def clear_empty_sets( self ):
        """
        'Corrige' les attributs en remplaçant les ensembles vides par des
        valeurs non définies.
        """
        for attr in SyncAccount.STORAGE:
            av = getattr( self , attr )
            if isinstance( av , set ) and not av:
                setattr( self , attr , None )

    #---------------------------------------------------------------------------

    def to_json( self ):
        """
        Convertit les données de synchronisation en un enregistrement JSON. Les
        attributs vides (valeurs None ou bien listes/ensembles/dictionnaires
        vides) seront ignorés.

        :return: les données au format JSON
        """
        d = {}
        for a in SyncAccount.STORAGE:
            av = getattr( self , a )
            if av is None:
                continue
            if type( av ) in ( list , set , dict ) and not av:
                continue
            d[ a ] = av
        return json_dump( d )

    def to_bss_account( self , coses ):
        """
        Crée une instance de compte Partage contenant les informations requises
        pour décrire le compte.

        :return: l'instance de compte Partage
        :raises AccountStateError: le compte est marqué pour suppression
        """
        if self.markedForDeletion:
            raise AccountStateError(
                    "compte {} marqué pour suppression".format( self.eppn ) )
        from lib_Partage_BSS.models import Account
        ra = Account( self.mail )
        # Copie des attributs
        for bss_attr in SyncAccount.BSS:
            setattr( ra , bss_attr ,
                    getattr( self , SyncAccount.BSS[ bss_attr ] ) )
        # Attribution de la classe de service
        if self.cos is not None:
            ra.zimbraCOSId = coses[ self.cos ]
        return ra

    #---------------------------------------------------------------------------

    def add_group( self , group ):
        """
        Ajoute un groupe au compte.

        :param str group: le nom du groupe à ajouter
        """
        if self.groups is None:
            self.groups = set( )
        self.groups.add( group )


    #---------------------------------------------------------------------------

    def __repr__( self ):
        return 'SyncAccount(' + ','.join( [
            a + '=' + repr( getattr( self , a ) )
                for a in SyncAccount.STORAGE ] )

    def __str__( self ):
        if self.eppn is None:
            return '(compte invalide)'
        return self.eppn

    def __eq__( self , other ):
        if type( other ) != type( self ):
            return False
        return False not in [
                getattr( self , a , None ) == getattr( other , a , None )
                    for a in SyncAccount.STORAGE
            ]

    def __ne__( self , other ):
        return not self.__eq__( other )

    def details_differ( self , other ):
        """
        Vérifie si des champs à importer dans le compte Partage diffèrent entre
        cette instance et une autre.

        :param SyncAccount other: l'instance avec laquelle on doit comparer
        :return: True si des différences existent, False dans le cas contraire
        """
        return False in ( getattr( self , d ) == getattr( other , d )
                                for d in SyncAccount.DETAILS )

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

#-------------------------------------------------------------------------------

class Config:
    """
    Cette classe permet de charger, de vérifier et d'utiliser la configuration
    du script.
    """

    # Nom de fichier par défaut; sera remplacé lors de l'initialisation du
    # programme afin d'utiliser le chemin du script.
    FILE_NAME = 'partage-sync.ini'

    def __init__( self , fname = None ):
        """
        Charge le fichier de configuration et vérifie que toutes les entrées
        requises sont présentes.

        :param str fname: le nom du fichier de configuration
        :raises FatalError: le fichier n'existe pas, ou la configuration est \
                incomplète
        """
        if fname is None:
            fname = Config.FILE_NAME
        config = ConfigParser( allow_no_value = True )
        try:
            config.read_file( open( fname ) )
        except FileNotFoundError:
            raise FatalError(
                    'Fichier de configuration "{}" non trouvé'.format( fname )
                )
        param_checks = (
            ( 'ldap' , (
                'host' , 'user' , 'pass' ,
                'people-dn' , 'groups-dn' ,
                'mail-domain' , 'eppn-domain'
            ) ) ,
            ( 'db' , (
                'path' ,
            ) ) ,
            ( 'bss' , (
                'domain' , 'token' , 'default-cos'
            ) ) ,
            ( 'ldap-people-classes' , ( ) ) ,
        )
        for section in param_checks:
            ( sn , items ) = section
            if sn not in config:
                raise FatalError( 'Section {} manquante'.format( sn ) )
            for item in items:
                if item in config[ sn ]:
                    continue
                raise FatalError( 'Section {}: élément {} manquant'.format(
                    sn , item ) )
        self.cfg_ = config

    def parse_cos_rules( self ):
        rules = dict( )
        if 'cos-rules' not in self.cfg_:
            return rules
        SyncAccount( self ) # Initialise la configuration des comptes
        section = self.cfg_[ 'cos-rules' ]
        try:
            for r in section:
                rules[ r ] = Rule( r , section[ r ] )
        except RuleError as e:
            Logging( 'cfg' ).critical( str( e ) )
            raise FatalError( 'Erreur dans les règles d\'attribution de CoS' )
        return rules

    def check_coses( self , coses ):
        to_check = [ self.get( 'bss' , 'default-cos' ) ] + self.get_list(
                'cos-rules' , [] )
        for n in to_check:
            if n not in coses:
                raise FatalError(
                    'Classe de service {} non trouvée sur le serveur'
                        .format( n ) )

    #---------------------------------------------------------------------------

    def get( self , section , value , default = None ):
        """
        Lit une valeur depuis la configuration.

        :param str section: le nom de la section de configuration
        :param str value: le nom du paramètre de configuration
        :param default: la valeur par défaut à renvoyer si la valeur ne peut \
                être trouvée

        :return: la valeur de l'entrée, ou la valeur par défaut
        """
        if section not in self.cfg_:
            return default
        return self.cfg_[ section ].get( value , default )

    def has_flag( self , section , name ):
        """
        Vérifie si un drapeau est actif. Dans le fichier de configuration, un
        drapeau est défini par une entrée sans valeur, qui sera présente ou non.

        :param str section: le nom de la section
        :param str name: le nom du drapeau

        :return: True si le drapeau est présent, False s'il ne l'est pas.
        """
        if section not in self.cfg_:
            return False
        return name in self.cfg_[ section ]

    def get_list( self , section , default = None ):
        """
        Lit les clés d'une section de configuration, sous la forme d'une liste.

        :param str section: le nom de la section à transformer en liste
        :param default: la valeur par défaut; si ce paramètre est None et que \
                la section n'existe pas, une erreur sera provoquée

        :return: la liste des clés de la section de configuration spécifiée, \
                ou la valeur par défaut si elle est définie.

        :raises FatalError: la section n'existe pas et aucune valeur par \
                défaut n'a été spécifiée.
        """
        if section in self.cfg_:
            return [ k for k in self.cfg_[ section ] ]
        if default is None:
            raise FatalError( 'Section {} vide'.format( section ) )
        return default

    def get_section( self , section , allow_empty = False ):
        """
        Lit une section de configuration sous la forme d'un dictionnaire.

        :param str section: le nom de la section de configuration devant être \
                lue
        :param bool allow_empty: ce paramètre détermine ce qu'il se passe si \
                la section n'existe pas. S'il est à True, un dictionnaire vide \
                sera renvoyé; sinon, une exception sera levée.

        :return: le dictionnaire des entrées de la section, ou un dictionnaire \
                vide si la section n'existe pas mais que le paramètre \
                allow_empty a la valeur True.

        :raises FatalError: la section n'existe pas et allow_empty a la valeur \
                False
        """
        if section in self.cfg_:
            s = self.cfg_[ section ]
            return { k : s[ k ] for k in s }
        if allow_empty:
            return dict()
        raise FatalError( 'Section {} non trouvée'.format( section ) )

    #---------------------------------------------------------------------------

    def ldap_server( self ):
        """
        Crée l'instance qui représente le serveur LDAP à partir de la
        configuration.

        :return: une instance de ldap3.Server configurée
        """
        lc = self.cfg_[ 'ldap' ]
        return ldap3.Server( lc[ 'host' ] ,
                port = int( lc.get( 'port' , 636 ) ) ,
                use_ssl = bool( int( lc.get( 'ssl' , '1' ) ) ) ,
                get_info = 'ALL' )

    def ldap_connection( self ):
        """
        Établit la connexion au serveur LDAP, en utilisant la configuration.

        :return: la connexion
        """
        server = self.ldap_server( )
        Logging( 'ldap' ).info( 'Connexion au serveur LDAP: ' + str( server ) )
        lc = self.cfg_[ 'ldap' ]
        return ldap3.Connection( self.ldap_server( ) ,
                lc[ 'user' ] , lc[ 'pass' ] , auto_bind = True )

    def lmdb_env( self ):
        """
        Initialise l'environnement pour LightningDB à partir de la
        configuration.

        :return: l'environnement LightningDB
        """
        db = self.get( 'db' , 'path' )
        Logging( 'db' ).info( 'Initialisation base de données: ' + db )
        return lmdb.Environment( subdir = True , path = db , mode = 0o700 ,
                map_size = int( self.get( 'db' , 'map-size' ,
                    str( 200 * 1024 * 1024 ) ) ) )

    def bss_connection( self ):
        """
        Configure la connexion à l'API BSS.

        :raises FatalError: la connexion ou l'authentification ont échoué
        """
        from lib_Partage_BSS.services.BSSConnexionService import BSSConnexion
        from lib_Partage_BSS.exceptions import BSSConnexionException
        dom = self.get( 'bss' , 'domain' )
        Logging( 'bss' ).info( 'Connexion à l\'API BSS, domaine: ' + dom )
        cn = BSSConnexion()
        cn.setDomainKey({ dom : self.get( 'bss' , 'token' ) })
        try:
            cn.token( dom )
        except BSSConnexionException as e:
            Logging( 'bss' ).error( "Connexion BSS - erreur: " + str( e ) )
            raise FatalError( "Échec de la connexion au service BSS" )


#-------------------------------------------------------------------------------

class LDAPData:

    def __init__( self , cfg ):
        """
        Charge les données en provenance du serveur LDAP.

        :param Config cfg: la configuration du script
        """

        with cfg.ldap_connection( ) as ldap_conn:
            def get_def_( names ):
                """
                Lit les définitions de classes LDAP afin de lister tous les
                attributs devant être extraits.

                :param names: la liste des noms de classes LDAP
                :return: une définition contenant tous les attributs \
                        correspondant aux classes listées
                :raises FatalError: une classe LDAP listée n'a pas pu être \
                        trouvée
                """
                ( first , rest ) = ( names[ 0 ] , names[ 1: ] )
                try:
                    dfn = ldap3.ObjectDef( first , ldap_conn )
                    for other in rest:
                        for attr in ldap3.ObjectDef( other , ldap_conn ):
                            dfn += attr
                except KeyError as e:
                    raise FatalError( 'Classe LDAP {} inconnue'.format(
                            str( e ) ) )
                return dfn

            def read_accounts_( ):
                """
                Lit la liste des comptes depuis l'annuaire LDAP.

                :return: un dictionnaire contenant les comptes; les EPPN sont \
                        utilisés comme clés.
                """
                people_dn = cfg.get( 'ldap' , 'people-dn' )
                mail_domain = '@{}'.format( cfg.get( 'ldap' , 'mail-domain' ) )
                obj_person = get_def_( cfg.get_list( 'ldap-people-classes' ) )

                reader = ldap3.Reader( ldap_conn , obj_person , people_dn )
                cursor = reader.search_paged( 10 , True )
                accounts = {}

                # On lit les comptes, en se limitant si nécessaire via la
                # variable de configuration 'limit'
                limit = int( cfg.get( 'ldap' , 'limit' , 0 ) )
                if limit > 0:
                    Logging( 'ldap' ).warning(
                            'synchronisation limitée à {} comptes'.format(
                                limit ) )
                for entry in cursor:
                    try:
                        a = SyncAccount( cfg ).from_ldap_entry( entry )
                    except AttributeError as e:
                        Logging( 'ldap' ).error(
                            'Compte LDAP {}: erreur sur attribut {}'.format(
                                str( entry.uid ) , str( e ) ) )
                        continue

                    # Redirection?
                    if not a.mail.endswith( mail_domain ):
                        Logging( 'ldap' ).warning(
                                'Compte LDAP {}: redirection vers {}' .format(
                                    str( entry.uid ) , a.mail ) )
                        continue

                    accounts[ a.eppn ] = a
                    Logging( 'ldap' ).debug( 'Compte {} chargé'.format(
                            a.eppn ) )
                    if len( accounts ) == limit:
                        break

                Logging( 'ldap' ).info( '{} comptes chargés'.format(
                        len( accounts ) ) )
                return accounts

            def read_groups_( ):
                """
                Lit la liste des groupes depuis l'annuaire LDAP.

                :return: un dictionnaire associant à chaque groupe la liste \
                        des comptes qui en font partie
                """
                obj_group = get_def_( cfg.get_list(
                        'ldap-group-classes' , ( 'posixGroup' , ) ) )
                group_dn = cfg.get( 'ldap' , 'groups-dn' )
                reader = ldap3.Reader( ldap_conn , obj_group , group_dn )
                cursor = reader.search_paged( 10 , True )
                groups = {}
                for entry in cursor:
                    groups[ entry.cn.value ] = set([ m.strip( )
                            for m in entry.memberUid.values ])
                    Logging( 'ldap' ).debug( 'Groupe {} chargé'.format(
                            entry.cn.value ) )
                return groups

            def set_account_groups_( ):
                """
                Parcourt la liste des groupes afin d'ajouter à chaque compte les
                groupes dont il est membre.
                """
                eppn_domain = cfg.get( 'ldap' , 'eppn-domain' )
                # On ajoute les groupes aux comptes
                for g in self.groups:
                    for uid in self.groups[ g ]:
                        eppn = '{}@{}'.format( uid , eppn_domain )
                        if eppn in self.accounts:
                            self.accounts[ eppn ].add_group( g )
                            continue
                        Logging( 'ldap' ).warning(
                                'Groupe {} - utilisateur {} inconnu'.format( g ,
                                    eppn ) )

            def set_account_cos_( ):
                def_cos = cfg.get( 'bss' , 'default-cos' )
                cos_rules = cfg.parse_cos_rules( )
                for a in self.accounts.values( ):
                    a.cos = def_cos
                    for r in cos_rules:
                        if cos_rules[ r ].check( a ):
                            a.cos = r
                            break
                    Logging( 'ldap' ).debug( 'Compte {} - CoS {}'.format(
                            a.eppn , a.cos ) )

            self.groups = read_groups_( )
            self.accounts = read_accounts_( )
            set_account_groups_( )
            set_account_cos_( )

    def remove_alias_accounts( self ):
        """
        Supprime de la liste des comptes ceux qui ont été identifiés comme étant
        de simples redirections.
        """
        ignored_accounts = [ a
                for a in self.accounts
                if self.accounts[ a ].mail != a
                    and self.accounts[ a ].mail in self.accounts ]
        if not ignored_accounts:
            return
        for ia in ignored_accounts:
            Logging( 'ldap' ).info( 'Compte {} ignoré'.format( ia ) )
            self.accounts.pop( ia )
        Logging( 'ldap' ).info( '{} comptes ignorés'.format(
                len( ignored_accounts ) ) )

    def set_account_aliases_( self , account , aliases , found ):
        """
        Méthode interne qui identifie les aliases correspondant à un compte LDAP
        et les ajoute à celui-ci.

        :param SyncAccount account: le compte
        :param AliasesMap aliases: l'instance de stockage des aliases
        :param set found: la liste des comptes ayant déjà été mis à jour
        """
        mn = aliases.get_main_account( account )
        if mn in found:
            Logging( 'ldap' ).error(
                    'Compte {} trouvé pour plus d\'une adresse' .format( mn ) )
            return
        found.add( mn )
        self.accounts[ account ].aliases = aliases.get_aliases( mn )
        if not self.accounts[ account ].aliases:
            return
        Logging( 'ldap' ).debug( 'Aliases pour le compte '
                + self.accounts[ account ].mail + ': '
                + ', '.join( self.accounts[ account ].aliases ) )

    def set_aliases( self , aliases ):
        """
        Initialise les aliases pour l'ensemble des comptes.

        :param AliasesMap aliases: l'instance de stockage des aliases
        """
        found = set( )
        for account in self.accounts:
            self.set_account_aliases_( account , aliases , found )

    def fix_mail_domain( self , cfg ):
        """
        Remplace le nom de domaine provenant du LDAP pour les adresses mail
        par celui configuré pour l'API de Partage. Normalement cette méthode
        ne fait rien, mais elle est nécessaire pour tourner en mode "test" avec
        un domaine différent.

        :param Config cfg: la configuration
        """
        ldap_dom = '@{}'.format( cfg.get( 'ldap' , 'mail-domain' ) )
        bss_dom = '@{}'.format( cfg.get( 'bss' , 'domain' ) )
        if ldap_dom == bss_dom:
            return

        def fix_it_( addr ):
            if not addr.endswith( ldap_dom ):
                return addr
            return addr[ :-len( ldap_dom ) ] + bss_dom

        Logging( 'bss' ).warning( 'Domaine mail: {} -> {}'.format(
                ldap_dom , bss_dom ) )
        for account in self.accounts.values( ):
            account.mail = fix_it_( account.mail )
            account.aliases = set([ fix_it_( a ) for a in account.aliases ])

    def clear_empty_sets( self ):
        """
        Remplace les ensembles (d'aliases et de groupes) vides par une valeur
        nulle.
        """
        for a in self.accounts.values( ):
            a.clear_empty_sets( )


#-------------------------------------------------------------------------------

class BSSAction:
    """
    Encapsulation d'un appel au service BSS permettant de réaliser facilement
    des appels en ne testant que la réussite ou l'échec (il reste cependant
    possible de récupérer les valeurs de retour si nécessaire).
    """

    # Si cette valeur est vraie, les actions ne seront pas effectuées
    SIMULATE = False

    def __init__( self , action , *args , **kwargs ):
        """
        Effectue un appel à l'API, en initialisant les champs appropriés. Tous
        les paramètres supplémentaires seront passés à la librairie.

        :param str action: le nom de l'appel à effectuer
        """
        from lib_Partage_BSS.services import AccountService
        import lib_Partage_BSS.exceptions as bsse
        self.ok_ = False

        mode = 'simulé ' if BSSAction.SIMULATE else ''
        Logging( 'bss' ).debug( 'Appel ' + mode + action
                + ': arguments ' + repr( args )
                + ' / par nom ' + repr( kwargs ) )

        if BSSAction.SIMULATE:
            self.data_ = None
            self.ok_ = True
            return

        func = AccountService.__dict__[ action ]
        try:
            self.data_ = func.__call__( *args , **kwargs )
        except ( bsse.NameException , bsse.DomainException ,
                bsse.ServiceException ) as error:
            Logging( 'bss' ).error( "Erreur appel BSS {}: {}".format(
                    action , repr( error ) ) )
            self.data_ = None
        else:
            self.ok_ = True

    def __bool__( self ):
        """
        Vérifie si l'appel a réussi.

        :return: True si l'appel a réussi, False s'il a échoué.
        """
        return self.ok_

    def get( self ):
        """
        Lit les données renvoyées par l'appel à l'API.

        :return: les données renvoyées (ou None si l'appel a échoué)
        """
        return self.data_

#-------------------------------------------------------------------------------

class Processor:

    def load_cos( self ):
        """
        Charge les classes de services depuis le serveur BSS et construit un
        dictionnaire associant les noms de classes à leurs identifiants.
        """
        import lib_Partage_BSS.services.COSService as bsssc
        import lib_Partage_BSS.exceptions as bsse
        try:
            coses = bsssc.getAllCOS( self.cfg.get( 'bss' , 'domain' ) )
        except ( bsse.NameException , bsse.DomainException ,
                bsse.ServiceException ) as error:
            Logging( 'bss' ).error( "Erreur lecture CoS: {}".format(
                    repr( error ) ) )
            raise FatalError( 'Impossible de lire la liste des CoS' )
        self.coses = { c.cn : c.id for c in coses }
        Logging( 'bss' ).debug( 'Classes de service: {}'.format(
                ', '.join([ '{} (ID {})'.format( str( c.cn ) , str( c.id ) )
                        for c in coses ]) ) )

    def load_from_ldap( self ):
        """
        Charge les données depuis le serveur LDAP. Les comptes et groupes
        seront chargés, puis la liste des aliases sera établie. Les comptes ne
        devant pas être synchronisés car ils correspondent à un alias seront
        ôtés de la liste. Enfin, si le domaine BSS et le domaine mail sont
        différents (parce que l'on est sur le serveur de test, par exemple),
        corrige toutes les adresses.
        """
        ldap_data = LDAPData( self.cfg )
        aliases = AliasesMap( self.cfg , ldap_data.accounts )
        ldap_data.remove_alias_accounts( )
        ldap_data.set_aliases( aliases )
        ldap_data.fix_mail_domain( self.cfg )
        ldap_data.clear_empty_sets( )

        # Sélection des comptes
        try:
            match_rule = Rule( 'account selection' ,
                    self.cfg.get( 'ldap' , 'match-rule' , '(true)' ) )
        except RuleError as e:
            Logging( 'cfg' ).critical( str( e ) )
            raise FatalError( 'Erreur dans la règle de sélection des comptes' )
        self.ldap_accounts = {}
        for eppn in ldap_data.accounts:
            a = ldap_data.accounts[ eppn ]
            if match_rule.check( a ):
                self.ldap_accounts[ eppn ] = a
            else:
                Logging( 'ldap' ).debug( 'Compte {} éliminé via règle'.format(
                        eppn ) )


    def load_db_accounts( self , txn ):
        """
        Lit l'intégralité des comptes depuis la base de données.

        :param txn: la transaction LightningDB
        :return: la liste des comptes lus depuis la base
        """
        def d_( x ): return x.decode( 'utf-8' )
        def na_( x ): return SyncAccount( self.cfg ).from_json( d_( x ) )
        with txn.cursor( ) as cursor:
            acc = { d_( a[ 0 ] ) : na_( a[ 1 ] ) for a in cursor }
        for x in acc.values( ): x.clear_empty_sets( )
        Logging( 'db' ).info( '{} comptes chargés depuis la BDD'.format(
                len( acc ) ) )
        self.db_accounts = acc

    def save_account( self , account ):
        """
        Sauvegarde les informations d'un compte dans la base de données. Si le
        drapeau de simulation est présent dans la configuration, l'opération ne
        sera pas réellement effectuée.

        :param SyncAccount account: le compte à sauvegarder
        """
        sim = self.cfg.has_flag( 'bss' , 'simulate' )
        mode = 'simulée ' if sim else ''
        Logging( 'db' ).debug( 'Sauvegarde {}du compte {} (mail {})'.format(
                mode , account.eppn, account.mail ) )
        if sim: return

        db_key = account.eppn.encode( 'utf-8' )
        account.clear_empty_sets( )
        with self.db.begin( write = True ) as txn:
            txn.put( db_key , account.to_json( ).encode( 'utf-8' ) )

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

    def process_accounts( self ):
        """
        Effectue les opérations sur les comptes, en synchronisant la base de
        données au fur et à mesure.
        """
        sdba = set( self.db_accounts.keys( ) )
        sla = set( self.ldap_accounts.keys( ) )

        # Créations de comptes
        new_accounts = sla - sdba
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

    def __init__( self ):
        self.cfg = Config( )
        self.cfg.bss_connection( )
        self.load_cos( )
        self.cfg.check_coses( self.coses )
        self.load_from_ldap( )
        BSSAction.SIMULATE = self.cfg.has_flag( 'bss' , 'simulate' )
        if BSSAction.SIMULATE:
            Logging( ).warn( 'Mode simulation activé' )
        with self.cfg.lmdb_env( ) as db:
            self.db = db
            with db.begin( write = False ) as txn:
                self.load_db_accounts( txn )
            self.process_accounts( )


#-------------------------------------------------------------------------------

own_path = os.path.dirname( os.path.realpath( __file__ ) )
Logging.FILE_NAME = os.path.join( own_path , 'partage-sync-logging.ini' )
Config.FILE_NAME = os.path.join( own_path , 'partage-sync.ini' )
try:
    Processor( )
except FatalError as e:
    Logging( ).critical( str( e ) )
    sys.exit( 1 )
