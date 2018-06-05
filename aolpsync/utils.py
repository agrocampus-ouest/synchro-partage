import json

class FatalError( Exception ):
    """
    Une exception indiquant qu'un message devrait être écrit dans le log et que
    l'exécution devrait se terminer.
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


def multivalued_check_equals( a , b ):
    # Valeurs égales => identique
    if a == b: return True
    # On ne poursuit que si l'une des deux valeurs est un ensemble ou une liste
    ( ais , bis ) = ( ( isinstance( v , set ) or isinstance( v , list ) )
            for v in ( a , b ) )
    if ( ais and bis ) or not ( ais or bis ): return False
    # On inverse les valeurs si nécessaire pour que a soit l'ensemble et b la
    # valeur
    if bis: ( a , b ) = ( b , a )
    # Valeur non définie vs ensemble vide => identique
    if b is None and not a: return True
    # Valeur vs ensemble d'un élement contenant la valeur => identique
    if b is not None and len( a ) == 1 and b in a: return True
    # Sinon différent
    return False


#-------------------------------------------------------------------------------


class BSSQuery:
    """
    Une classe qui peut être passée comme paramètre d'action à BSSAction afin
    de distinguer les demandes d'informations des actions de modification.
    """
    def __init__( self , action ):
        self.action = action
    def __str__( self ):
        return self.action
    def __bool__( self ):
        return False


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

        :param action: le nom de l'appel à effectuer, ou un objet de type \
                BSSQuery encapsulant ce nom
        """
        from lib_Partage_BSS.services import AccountService
        import lib_Partage_BSS.exceptions as bsse
        self.ok_ = False
        is_action = bool( action )
        action = str( action )
        simulate = BSSAction.SIMULATE and is_action

        mode = 'simulé ' if simulate else ''
        from .logging import Logging
        Logging( 'bss' ).debug( 'Appel ' + mode + action
                + ': arguments ' + repr( args )
                + ' / par nom ' + repr( kwargs ) )

        if simulate:
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
