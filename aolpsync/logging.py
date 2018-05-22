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

