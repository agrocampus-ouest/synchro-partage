from .utils import Zimbra , ZimbraError


class CalendarSync:

    def __init__( self , cfg , accounts ):
        self.enabled = cfg.has_section( 'calendars' ):
        if not self.enabled: return
        self.source_db_ = cfg.get( 'calendars' , 'source' ,
                raise_missing = True )
        self.source_query_ = cfg.get( 'calendars' , 'query' ,
                raise_missing = True )
        self.folder_name_ = cfg.get( 'calendars' , 'folder-name' ,
                raise_missing = True )
        self.folder_color_ = cfg.get( 'calendars' , 'folder-color' )
        self.blocking_ = cfg.has_flag( 'calendars' , 'blocking-events' )
        self.zimbra_ = Zimbra( cfg )

    def synchronize( self , accounts , sync_set = None ):
        if not self.enabled: return

        address_map = self.get_address_map_( accounts )
        calendars = self.get_calendars_( address_map )

        # Génère l'ensemble des comptes à synchroniser
        sync = calendars.keys( )
        if sync_set is not None:
            sync.intersection_update( sync_set )

        # Pour chaque compte, on ajoute l'emploi du temps
        for eppn in sync:
            try:
                self.add_calendar_( eppn , calendars[ eppn ] )
            except ZimbraError as e:
                # FIXME log the fuck
                pass


    def get_address_map_( self , accounts):
        # On génère les correspondances comptes / adresses
        address_map = dict( )
        for eppn in accounts:
            address_map[ eppn ] = eppn
            account = accounts[ eppn ]
            if not account.aliases:
                continue
            for alias in account.aliases:
                address_map[ alias ] = eppn
        return address_map

    def get_calendars_( self , address_map ):
        from .sqldb import query as sql_query
        calendars_list = sql_query( self.source_db_ , self.source_query_ )
        calendars = dict( )
        for row in calendars_list:
            ( addr , url ) = row
            if addr not in address_map:
                continue
            eppn = address_map[ addr ]
            if eppn in calendars:
                # FIXME warning
                continue
            calendars[ eppn ] = url
        return calendars

    def add_calendar_( self , eppn , url ):
        self.zimbra_.set_user( eppn )
        root_folder = self.zimbra_.get_folder( )
        found = self.find_calendar_( root_folder , url )

        if not found:
            # Aucune correspondance -> on crée
            n = self.gen_calendar_name_( root_folder )
            f = ( '' if self.blocking_ else '' ) + '#i'
            self.zimbra_.create_folder( name = n ,
                    parent_id = root_folder[ 'id' ] ,
                    folder_type = 'appointment',
                    color = self.folder_color_ ,
                    flags = f ,
                    url = url ,
                    others = { 'sync' : 1 } )
            return

        # Le calendrier existe et n'est pas à la poubelle -> on ne touche à rien
        if False in ( v[ 0 ] for v in found.values( ) ):
            return

        # Le calendrier est à la poubelle, on le restaure
        ( restore , *junk ) = found.keys( )
        f_data = found[ restore ][ 1 ]
        n = self.gen_calendar_name_( root_folder )
        if n != f_data[ 'name' ]:
            self.zimbra_.rename_folder( f_data[ 'id' ] , n )
        self.zimbra_.move_folder( f_data[ 'id' ] , user_root[ 'id' ] )


    def find_calendar_( self , folder , url , in_trash = False , v = None ):
        if v is None:
            v = dict( )
        if 'url' in folder and folder[ 'url' ] == url:
            v[ folder[ 'absFolderPath' ] ] = ( in_trash , folder )
            return v
        if 'folder' in folder:
            in_trash = in_trash or folder[ 'absFolderPath' ] == '/Trash'
            for f in folder[ 'folder' ]:
                find_folders_with_url( url , f , in_trash , v )
        return v
