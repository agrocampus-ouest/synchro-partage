from .logging import Logging
from .utils import FatalError


#-------------------------------------------------------------------------------


class DbConnections:
    """
    Classe utilisée comme singleton, permettant la mise en cache des connexions
    aux bases SQL supplémentaires. Les connexions ne sont initialisées que
    lorsqu'elles sont utilisées.
    """

    # Instance unique de la classe
    INSTANCE = None

    def __init__( self , cfg ):
        """
        Initialise le cache de connexions et garde une référence à la
        configuration.
        """
        self.cfg_ = cfg
        self.connections_ = {}

    def query( self , name , sql ):
        """
        Exécute une requête SQL sur une base.

        :param str name: l'identifiant de la base SQL dans la configuration
        :param str sql: la requête SQL à exécuter

        :return: l'ensemble des lignes lues, sous la forme d'une liste de \
                tuples

        :raises FatalError: la connexion n'est pas configurée (ou mal \
                configurée), ou une erreur s'est produite lors de son \
                établissement
        :raises Exception: une exception produite lors de l'exécution de \
                la requête
        """
        if name not in self.connections_:
            self.connect_( name )
        Logging( 'sqldb' ).debug( 'Requête sur {}: {}'.format( name , sql ) )
        cur = self.connections_[ name ].cursor( )
        cur.execute( sql )
        return cur.fetchall( )

    def connect_( self , name ):
        """
        Établit une connexion à une base de donnée et stocke cette connexion
        pour réutilisation ultérieure.

        :param str name: l'identifiant de la base de données

        :return: l'ensemble des lignes lues, sous la forme d'une liste de \
                tuples

        :raises FatalError: la connexion n'est pas configurée (ou mal \
                configurée), ou une erreur s'est produite lors de son \
                établissement
        """
        assert name not in self.connections_

        # Accède à la configuration pour cette connexion
        config = self.cfg_.get_section( 'sqldb-{}'.format( name ) )
        if 'python-module' not in config:
            raise FatalError(
                    'Connexion SQL {}: clause "python-module" requise'.format(
                            name ) )

        # Chargement du module
        mod_name = config[ 'python-module' ]
        Logging( 'sqldb' ).debug( 'Connexion à {}: module {}'.format(
                name , mod_name ) )
        try:
            module = __import__( mod_name )
        except ImportError:
            raise FatalError( ( 'Connexion SQL {}: impossible d\'importer '
                    + 'le module "{}"' ).format( name , mod_name ) )

        mod_connect = { m : config[ m ]
                            for m in config
                            if m != 'python-module' }
        try:
            self.connections_[ name ] = module.connect( **mod_connect )
        except Exception as e:
            raise FatalError(
                    'Connexion SQL {}: échec de la connexion ({})'.format(
                            name , repr( e ) ) )


#-------------------------------------------------------------------------------


def init( cfg ):
    """
    Initialise l'instance de gestion des connexions SQL. Ne doit être appelée
    qu'une seule fois.

    :param aolpsync.configuration.Config cfg: la configuration
    """
    assert DbConnections.INSTANCE is None
    DbConnections.INSTANCE = DbConnections( cfg )

def query( name , query ):
    """
    Exécute une requête SQL sur une base.

    :param str name: l'identifiant de la base SQL
    :param str query: la requête à exécuter

    :return: l'ensemble des lignes lues, sous la forme d'une liste de \
            tuples

    :raises FatalError: la connexion n'est pas configurée (ou mal \
            configurée), ou une erreur s'est produite lors de son \
            établissement
    :raises Exception: une exception produite lors de l'exécution de \
            la requête
    """
    assert DbConnections.INSTANCE is not None
    return DbConnections.INSTANCE.query( name , query )
