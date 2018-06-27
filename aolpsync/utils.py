from .logging import Logging
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

def get_address_fixer( cfg ):
    """
    Génère une fonction capable de corriger les adresses mail pour l'utilisation
    d'un domaine de test via l'API.

    :param cfg: la configuration

    :return: une fonction qui corrige les noms de domaines dans les adresses \
            mail si le domaine mail est différent du domaine BSS et que la \
            correction n'est pas désactivée, ou une fonction "identité" dans \
            le cas contraire
    """
    ldap_dom = '@{}'.format( cfg.get( 'ldap' , 'mail-domain' ) )
    if not cfg.has_flag( 'bss' , 'dont-fix-domains' ):
        bss_dom = '@{}'.format( cfg.get( 'bss' , 'domain' ) )
        if ldap_dom != bss_dom:
            return lambda addr : (
                    addr if not addr.endswith( ldap_dom )
                        else ( addr[ :-len( ldap_dom ) ] + bss_dom )
                )
    return lambda addr : addr


#-------------------------------------------------------------------------------


def run_shell_command( command ):
    """
    Exécute une commande via le shell.

    :param str command: la ligne de commande à exécuter

    :return: un tuple contenant le code de retour, la sortie standard sous la \
            forme d'une liste, et l'erreur standard sous la forme d'une liste
    """
    import subprocess
    child = subprocess.Popen( command , shell = True ,
            stdout = subprocess.PIPE ,
            stderr = subprocess.PIPE )
    ev = child.wait( )
    output = [ l for l in child.stdout ]
    errors = [ l for l in child.stderr ]
    return ( ev , output , errors )


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


#-------------------------------------------------------------------------------

class ZimbraError( Exception ):
    """
    Classe de base pour les exceptions en rapport avec l'API de Zimbra.
    """
    pass

class ZimbraConnectionError( ZimbraError ):
    """
    Erreur lors de l'authentification à un compte Zimbra ou lors de la
    communication.
    """
    pass

class ZimbraRequestError( ZimbraError):
    """
    Erreur lors de l'exécution d'une requête API Zimbra.
    """
    pass


#-------------------------------------------------------------------------------


class Zimbra:
    """
    Classe utilitaire pour l'accès à l'API de Zimbra.
    """

    def __init__( self , cfg ):
        """
        Initialise l'accès à l'API en lisant les paramètres de connexion dans la
        configuration et en créant l'instance de communication.
        """
        self.url_ = cfg.get( 'bss' , 'zimbra-url' ,
                'https://webmail.partage.renater.fr/service/soap' )
        self.domain_ = cfg.get( 'bss' , 'domain' )
        self.dkey_ = cfg.get( 'bss' , 'token' )
        self.fake_it_ = cfg.has_flag( 'bss' , 'simulate' )
        self.user_ = None
        self.token_ = None
        from pythonzimbra.communication import Communication
        self.comm_ = Communication( self.url_ )

    def terminate( self ):
        """
        Termine la session actuelle, s'il y en a une, en envoyant une requête
        'EndSession'.
        """
        if self.user_ is None:
            return
        Logging( 'zimbra' ).debug( 'Déconnexion de {}'.format( self.user_ ) )
        self.send_request_( 'Account' , 'EndSession' )
        self.user_ = None
        self.token_ = None

    def set_user( self , user_name ):
        """
        Tente de se connecter à l'API Zimbra avec un utilisateur donné. Si le
        nom d'utilisateur n'inclut pas un domaine, il sera rajouté.

        :param str user_name: le nom d'utilisateur

        :raises ZimbraError: une erreur de communication s'est produite
        """
        if '@' not in user_name:
            user_name = '{}@{}'.format( user_name , self.domain_ )

        if self.user_ is not None and self.user_ == user_name:
            return
        self.terminate( ) # ...Yes dear, you can self-terminate.

        from pythonzimbra.tools import auth
        from pythonzimbra.exceptions.auth import AuthenticationFailed
        Logging( 'zimbra' ).debug( 'Connexion pour {}'.format( user_name ) )
        try:
            ttok = auth.authenticate(
                    self.url_ , user_name ,
                    self.dkey_ , raise_on_error = True )
        except AuthenticationFailed as e:
            Logging( 'zimbra' ).error(
                    'Échec de la connexion: {}'.format( str( e ) ) )
            raise ZimbraConnectionError( e ) #FIXME
        Logging( 'zimbra' ).debug( 'Connexion réussie; jeton: {}'.format(
                ttok ) )

        assert ttok is not None
        self.user_ = user_name
        self.token_ = ttok

    def get_folder( self , path = '/' , recursive = True ):
        """
        Lit les informations d'un dossier (au sens Zimbra du terme) et
        éventuellement de ses sous-dossiers.

        :param str path: le chemin absolu du dossier sur lequel on veut \
                des informations
        :param bool recursive: les informations des sous-dossiers seront \
                également lues si ce paramètre est True

        :raises ZimbraError: une erreur de communication ou de requête \
                s'est produite
        """
        Logging( 'zimbra' ).debug( 'Récupération{} du dossier {}'.format(
                ' récursive' if recursive else '' , path ) )
        ls = self.send_request_( 'Mail' , 'GetFolder' , {
                'path' : path ,
                'depth' : -1 if recursive else 0
            } )
        return ls[ 'folder' ] if 'folder' in ls else None

    def create_folder( self , name , parent_id , folder_type ,
            color = None , url = None , flags = None , others = None ):
        """
        Demande la création d'un dossier. Si le mode simulé est activé, l'appel
        ne sera pas effectué.

        :param str name: le nom du dossier à créer
        :param parent_id: l'identifiant du dossier parent
        :param str folder_type: le type de dossier à créer (conversation, \
                message, contact, appointment, task, wiki, document)
        :param str color: la couleur, sous la forme d'un code RGB hexadécimal \
                ('#RRGGBB')
        :param str url: l'URL de synchronisation, s'il y en a une
        :param str flags: les drapeaux Zimbra du dossier
        :param dict others: un dictionnaire contenant des valeurs \
                supplémentaires à associer au dossier

        :raises ZimbraError: une erreur de communication ou de requête \
                s'est produite
        """
        Logging( 'zimbra' ).debug(
                'Création{} du dossier {}, parent {}, type {}'.format(
                    ' simulée' if self.fake_it_ else '' ,
                    name , parent_id , folder_type ) )

        if self.fake_it_:
            return {}
        data = {
            'name' : name ,
            'view' : folder_type ,
            'l' : parent_id ,
            'fie' : '1' ,
        }
        if others is not None: data.update( others )
        if 'acl' not in data: data[ 'acl' ] = {}
        if color is not None: data[ 'rgb' ] = color
        if url is not None: data[ 'url' ] = url
        if flags is not None: data[ 'f' ] = flags
        return self.send_request_( 'Mail' , 'CreateFolder' , {
                'folder' : data
            } )

    def move_folder( self , folder_id , to_id ):
        """
        Demande le déplacement d'un dossier. Si le mode simulé est activé,
        l'appel ne sera pas effectué.

        :param folder_id: l'identifiant du dossier à déplacer
        :param to_id: l'identifiant du nouveau dossier parent

        :raises ZimbraError: une erreur de communication ou de requête \
                s'est produite
        """
        Logging( 'zimbra' ).debug(
                'Déplacement{} du dossier #{} vers dossier #{}'.format(
                    ' simulé' if self.fake_it_ else '' ,
                    folder_id , to_id ) )
        if self.fake_it_:
            return {}
        return self.send_request_( 'Mail' , 'FolderAction' , {
                'action' : {
                    'op' : 'move' ,
                    'id' : folder_id ,
                    'l' : to_id ,
                }
            } )

    def rename_folder( self , folder_id , name ):
        """
        Demande à l'API de renommer un dossier. Si le mode simulé est activé,
        l'appel ne sera pas effectué.

        :param folder_id: l'identifiant du dossier à renommer
        :param str name: le nouveau nom du dossier

        :raises ZimbraError: une erreur de communication ou de requête \
                s'est produite
        """
        Logging( 'zimbra' ).debug(
                'Renommage{} du dossier #{}; nouveau nom "{}"'.format(
                    ' simulé' if self.fake_it_ else '' ,
                    folder_id , name ) )
        if self.fake_it_:
            return {}
        return self.send_request_( 'Mail' , 'FolderAction' , {
                'action' : {
                    'op' : 'rename' ,
                    'id' : folder_id ,
                    'name' : name ,
                }
            } )

    def send_request_( self , namespace , request , data = None ):
        """
        Envoie une requête au serveur Zimbra

        :param str namespace: l'espace de nommage de l'appel, par exemple Mail
        :param str request: le nom de la requête, par exemple GetFolder

        :return: la réponse renvoyée par le serveur

        :raises ZimbraRequestError: une erreur de communication ou de requête \
                s'est produite
        """
        assert self.token_ is not None
        Logging( 'zimbra.request' ).debug(
                'Requête {}.{}( {} )'.format(
                    namespace , request , repr( data ) ) )
        if data is None:
            data = dict()
        req = self.comm_.gen_request( token = self.token_ )
        req.add_request( request + 'Request' , data , 'urn:zimbra' + namespace )
        response = self.comm_.send_request( req )
        if response.is_fault( ):
            raise ZimbraRequestError( 'appel {}.{}: {} (code {})'.format(
                    namespace , request , response.get_fault_message( ) ,
                    response.get_fault_code( ) ) )
        rv = response.get_response( )
        return rv[ request + 'Response' ]
