from .data import SyncAccount
from .logging import Logging

class RuleError( Exception ):
    """
    Une exception qui indique qu'une règle d'attribution de classe de service
    est incorrectement configurée.
    """
    pass

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

#-------------------------------------------------------------------------------

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
