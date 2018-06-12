from .logging import Logging


#-------------------------------------------------------------------------------


class DbConnections:

    INSTANCE = None

    def __init__( self , cfg ):
        self.cfg_ = cfg
        self.connections_ = {}

    def query( self , name , sql ):
        if name not in self.connections_:
            self.connect_( name )
        Logging( 'sqldb' ).debug( 'Requête sur {}: {}'.format( name , sql ) )
        cur = self.connections_[ name ].cursor( )
        cur.execute( sql )
        return cur.fetchall( )

    def connect_( self , name ):
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

def init( cfg ):
    assert DbConnections.INSTANCE is None
    DbConnections.INSTANCE = DbConnections( cfg )

def query( name , query ):
    assert DbConnections.INSTANCE is not None
    return DbConnections.INSTANCE.query( name , query )
