from .logging import Logging


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
    # Attributs devant être synchronisés lors de la création du compte
    # uniquement
    CREATE_ONLY = None
    # Attributs LDAP
    LDAP = None
    # Correspondances BSS -> champs locaux
    BSS = None
    # Champs BSS sur lesquels on ignore la validation par lib_Partage_BSS
    BSS_BYPASS = None
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
            'uid' , 'eppn' , 'surname' , 'givenName' ,
            'displayName' , 'mail' , 'passwordHash' , 'groups' ,
            'ldapMail' , 'markedForDeletion' , 'aliases' , 'cos'
        ])
        once = set()

        # Attributs configurés
        ea_defs = cfg.get_section( 'extra-attributes' , allow_empty = True )
        for ea , v in ea_defs.items( ):
            if ea in attrs or ea in once:
                raise AttributeDefError( 'Attribut {}: doublon'.format( ea ) )
            if v == 'once':
                once.add( ea )
            else:
                attrs.add( ea )
        Logging( 'cfg' ).debug( 'Attributs synchronisés: '
                + ', '.join( attrs ) )
        Logging( 'cfg' ).debug( 'Attributs de création: ' + ', '.join( once ) )
        SyncAccount.STORAGE = attrs
        SyncAccount.CREATE_ONLY = once

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
        bss_dom = cfg.get( 'bss' , 'domain' )
        mail_dom = cfg.get( 'ldap' , 'mail-domain' )
        eppn_dom = cfg.get( 'ldap' , 'eppn-domain' )
        LA = LDAPAttr

        if bss_dom == mail_dom or cfg.has_flag( 'bss' , 'dont-fix-domains' ):
            eppn_attr = LA( 'eppn' , 'eduPersonPrincipalName' ,
                    gen = lambda e : "{}@{}".format( str( e.uid ) , eppn_dom ) )
        else:
            from .utils import get_eppn_fixer
            eppn_fixer = get_eppn_fixer( cfg )
            def gen_eppn_( la ):
                la_eppn = getattr( la , 'eduPersonPrincipalName' , None )
                if la_eppn is None:
                    return '{}@{}'.format( str( la.uid ) , bss_dom )
                return eppn_fixer( str( la_eppn ) )
            eppn_attr = LA( 'eppn' , '' , gen = gen_eppn_ )

        extra_attrs = cfg.get_section( 'ldap-extra-attributes' , True )

        # On génère la liste des attributs par défaut
        ldap_attrs = [
            LA( 'uid' ) ,
            eppn_attr ,
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
        all_attrs = SyncAccount.STORAGE | SyncAccount.CREATE_ONLY
        for ea in extra_attrs:
            if ea in defined_attrs:
                raise AttributeDefError( 'Attribut {}: doublon'.format( ea ) )
            if ea not in all_attrs:
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
        all_attrs = SyncAccount.STORAGE | SyncAccount.CREATE_ONLY
        for ea in cfg.get_section( 'bss-extra-attributes' , True ):
            if ea not in all_attrs:
                raise AttributeDefError(
                        'Attribut {}: non défini'.format( ea ) )
            bss_ea = cfg.get( 'bss-extra-attributes' , ea , ea )
            if bss_ea in bss_attrs:
                raise AttributeDefError(
                        'Attribut {}: doublon'.format( ea ) )
            bss_attrs[ bss_ea ] = ea
            if ea not in SyncAccount.CREATE_ONLY:
                details.add( ea )

        Logging( 'cfg' ).debug( 'Champs de détail: ' + ', '.join( details ) )
        SyncAccount.DETAILS = tuple( details )
        Logging( 'cfg' ).debug( 'Correspondances BSS: ' + ', '.join([
            '{} -> {}'.format( x , bss_attrs[ x ] )
                for x in bss_attrs ]) )
        SyncAccount.BSS = bss_attrs
        SyncAccount.BSS_BYPASS = set([ x
            for x in cfg.get( 'bss' , 'bypass-acc-checks' , '' ).split( ' ' )
            if x != '' ])

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
        for attr in SyncAccount.STORAGE | SyncAccount.CREATE_ONLY:
            setattr( self , attr , None )

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
        for attr in SyncAccount.STORAGE | SyncAccount.CREATE_ONLY:
            av = getattr( self , attr )
            if isinstance( av , set ) and not av:
                setattr( self , attr , None )

    def add_group( self , group ):
        """
        Ajoute un groupe au compte.

        :param str group: le nom du groupe à ajouter
        """
        if self.groups is None:
            self.groups = set( )
        self.groups.add( group )

    #---------------------------------------------------------------------------

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

    #---------------------------------------------------------------------------

    def from_json_record( self , data ):
        """
        Initialise les attributs à partir d'un enregistrement désérialisé depuis
        du JSON.

        :param str data: l'enregistrement JSON désérialisé

        :return: l'instance de synchronisation
        """
        self.clear( )
        for a in SyncAccount.STORAGE:
            if a in data:
                v = data[ a ]
            else:
                v = None
            setattr( self , a , v )
        return self

    def from_json( self , data ):
        """
        Initialise les attributs à partir d'un enregistrement JSON.

        :param str data: l'enregistrement JSON

        :return: l'instance de synchronisation
        """
        from .utils import json_load
        return self.from_json_record( json_load( data ) )

    def to_json_record( self ):
        """
        Convertit les données de synchronisation en un enregistrement destiné à
        être sauvegardé sous forme de JSON. Les attributs vides (valeurs None ou
        bien listes/ensembles/dictionnaires vides) seront ignorés.

        :return: les données sous la forme d'un dictionnaire pouvant être \
                sérialisé en JSON
        """
        d = {}
        for a in SyncAccount.STORAGE:
            av = getattr( self , a )
            if av is None:
                continue
            if type( av ) in ( list , set , dict ) and not av:
                continue
            d[ a ] = av
        return d

    def to_json( self ):
        """
        Convertit les données de synchronisation en un enregistrement JSON. Les
        attributs vides (valeurs None ou bien listes/ensembles/dictionnaires
        vides) seront ignorés.

        :return: les données au format JSON
        """
        from .utils import json_dump
        return json_dump( self.to_json_record( ) )

    #---------------------------------------------------------------------------

    def to_bss_account( self , coses , create = False ):
        """
        Crée une instance de compte Partage contenant les informations requises
        pour décrire le compte.

        :param coses: le dictionnaire associant à chaque nom de classe de \
                service un UUID
        :param bool create: les données vont-elle servir à créer le compte?
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
            mapped_from = SyncAccount.BSS[ bss_attr ]
            if mapped_from in SyncAccount.CREATE_ONLY and not create:
                continue
            value = getattr( self , mapped_from )
            if value is None:
                value = ''
            if bss_attr in SyncAccount.BSS_BYPASS:
                setattr( ra , '_{}'.format( bss_attr ) , value )
            else:
                setattr( ra , bss_attr , value )
        # Attribution de la classe de service
        if self.cos is not None:
            ra.zimbraCOSId = coses[ self.cos ]
        return ra

    def from_bss_account( self , account , rev_coses ):
        """
        Convertit un compte Partage dont les détails ont été lus via l'API BSS
        en un compte de synchronisation. Le mail sera lu depuis le nom du
        compte, les détails seront copiés, la classe de service retrouvée grâce
        au dictionnaire, et le ou les aliases, s'il y en a, seront initialisés.

        :param account: le compte Partage dont il faut copier les détails
        :param rev_coses: le dictionnaire inversé (i.e. ID -> nom) des \
                classes de service

        :raises AccountStateError: l'identifiant de la classe de service est \
                inconnu

        :return: l'instance
        """
        # Initialisation
        self.clear( )
        self.mail = account.name

        # État de pré-suppression
        import re
        re_match = re.match( r'^del-(\d+)-.*' , account.name )
        if account.zimbraAccountStatus == 'closed' and re_match:
            self.markedForDeletion = int( re_match.group( 1 ) )

        # Copie des attributs
        for bss_attr in SyncAccount.BSS:
            v = getattr( account , bss_attr )
            if v is not None:
                mapped_from = SyncAccount.BSS[ bss_attr ]
                if mapped_from not in SyncAccount.CREATE_ONLY:
                    if mapped_from in SyncAccount.DETAILS and type( v ) == int:
                        v = str( v )
                    setattr( self , mapped_from , v )
        # Aliases
        aliases = account.zimbraMailAlias
        if aliases is not None and aliases:
            if isinstance( aliases , str ):
                self.aliases = set([ aliases ])
            else:
                self.aliases = set( aliases )
        # Classe de service
        cosId = account.zimbraCOSId
        if cosId is None:
            Logging( 'bss' ).info( 'compte {}: pas de CoS'.format( self.eppn ) )
        else:
            if cosId not in rev_coses:
                raise AccountStateError(
                        "compte {}: identifiant CoS {} inconnu".format(
                            self.eppn , cosId ) )
            self.cos = rev_coses[ cosId ]
        return self

    #---------------------------------------------------------------------------

    def __repr__( self ):
        return 'SyncAccount({})'.format( ','.join( [
                    a + '=' + repr( getattr( self , a ) )
                        for a in SyncAccount.STORAGE | SyncAccount.CREATE_ONLY
            ] ) )

    def __str__( self ):
        if self.eppn is None:
            return '(compte invalide)'
        return self.eppn

    def compare_( self , other , attributes ):
        from .utils import multivalued_check_equals as mce
        def eq_check_( attr ):
            va = getattr( self , attr , None )
            vb = getattr( other , attr , None )
            return mce( va , vb )
        return False not in ( eq_check_( a ) for a in attributes )

    def __eq__( self , other ):
        if type( other ) != type( self ):
            return False
        return self.compare_( other , SyncAccount.STORAGE )

    def __ne__( self , other ):
        return not self.__eq__( other )

    def details_differ( self , other ):
        """
        Vérifie si des champs à importer dans le compte Partage diffèrent entre
        cette instance et une autre.

        :param SyncAccount other: l'instance avec laquelle on doit comparer
        :return: True si des différences existent, False dans le cas contraire
        """
        return not self.compare_( other , SyncAccount.DETAILS )

    def bss_equals( self , other ):
        """
        Vérifie si deux comptes sont équivalents par rapport aux détails
        disponibles via l'API Partage. Cela vérifie le champ mail, les détails
        et les aliases.

        :param SyncAccount other: l'instance avec laquelle on doit comparer
        :return: True si les deux comptes sont équivalents, False dans le \
                cas contraire
        """
        from .utils import multivalued_check_equals as mce
        return ( mce( self.mail , other.mail )
                and self.markedForDeletion == other.markedForDeletion
                and not self.details_differ( other )
                and mce( self.aliases , other.aliases ) )

    def is_predeleted( self ):
        return self.markedForDeletion is not None


#-------------------------------------------------------------------------------


class LDAPData:

    def __init__( self , cfg , query = "" ):
        """
        Charge les données en provenance du serveur LDAP, et permet leur
        modification ultérieure pour adapter les données des comptes en fonction
        de la configuration, des aliases, etc.

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
                    from ldap3 import ObjectDef
                    dfn = ObjectDef( first , ldap_conn )
                    for other in rest:
                        for attr in ObjectDef( other , ldap_conn ):
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

                from ldap3 import Reader
                reader = Reader( ldap_conn , obj_person , people_dn , query )
                cursor = reader.search_paged( 10 , True )
                all_uids = set( )
                accounts = {}

                # On lit les comptes, en se limitant si nécessaire via la
                # variable de configuration 'limit'
                limit = int( cfg.get( 'ldap' , 'limit' , 0 ) )
                if limit > 0:
                    Logging( 'ldap' ).warning(
                            'synchronisation limitée à {} comptes'.format(
                                limit ) )
                for entry in cursor:
                    all_uids.add( str( entry.uid ) )
                    try:
                        a = SyncAccount( cfg ).from_ldap_entry( entry )
                    except AttributeError as e:
                        Logging( 'ldap' ).warning(
                            'Compte LDAP {}: erreur sur attribut {}'.format(
                                str( entry.uid ) , str( e ) ) )
                        continue

                    # Redirection?
                    if isinstance( a.ldapMail , str ):
                        a.ldapMail = set([ a.ldapMail ])
                    remove = []
                    for ma in a.ldapMail:
                        if ma.endswith( mail_domain ):
                            continue
                        Logging( 'ldap' ).info( (
                                'Compte LDAP {}: redirection depuis {} vers '
                                + '{} ignorée'
                            ).format( str( entry.uid ) , a.mail , ma ) )
                        remove.append( ma )
                    a.ldapMail.difference_update( remove )
                    if not a.ldapMail:
                        Logging( 'ldap' ).info(
                                'Compte LDAP {}: purement externe'.format(
                                    entry.uid ) )
                        continue

                    accounts[ a.eppn ] = a
                    Logging( 'ldap' ).debug( 'Compte {} chargé'.format(
                            a.eppn ) )
                    if len( accounts ) == limit:
                        break

                Logging( 'ldap' ).info( '{} comptes chargés sur {} UIDs'.format(
                        len( accounts ) , len( all_uids ) ) )
                return ( all_uids , accounts )

            def read_groups_( ):
                """
                Lit la liste des groupes depuis l'annuaire LDAP.

                :return: un dictionnaire associant à chaque groupe la liste \
                        des comptes qui en font partie
                """
                from ldap3 import Reader
                group_types = cfg.get_section( 'ldap-group-classes' , True )
                if not group_types:
                    group_types = {
                            'posixGroup' : 'memberUid' ,
                            'groupOfNames' : 'member' ,
                            'groupOfUniqueNames' : 'uniqueMember' ,
                        }
                Logging( 'ldap' ).debug( 'Types de groupes : {}'.format(
                    ' - '.join( group_types.keys( ) ) ) )
                groups = {}
                group_dn = cfg.get( 'ldap' , 'groups-dn' )
                for group_type , member_attr in group_types.items( ):
                    # Chaque entrée correspond à une ou plusieurs classes
                    # séparées par des /
                    obj_classes = group_type.split( '/' )
                    obj_group = get_def_( obj_classes )
                    reader = Reader( ldap_conn , obj_group , group_dn )
                    cursor = reader.search_paged( 10 , True )

                    # On parcourt la liste
                    for entry in cursor:
                        group_name = entry.cn.value
                        if group_name in groups:
                            continue

                        # On extrait les membres pour chaque nouveau groupe
                        groups[ group_name ] = set( )
                        for member in getattr( entry , member_attr ).values:
                            member = member.strip( )

                            # Transformation des DN en UID
                            if '=' in member and ',' in member:
                                parts_of_dn = member.split( ',' )
                                attr_val = parts_of_dn[ 0 ].split( '=' )
                                member = attr_val[ 1 ]

                            groups[ group_name ].add( member )

                        Logging( 'ldap' ).debug( 'Groupe {} chargé'.format(
                                group_name ) )
                return groups

            def set_account_groups_( all_uids ):
                """
                Parcourt la liste des groupes afin d'ajouter à chaque compte les
                groupes dont il est membre.

                :param set all_uids: l'ensemble des UID valables, y compris \
                        ceux qui ne correspondent pas à des comptes
                """
                eppn_domain = cfg.get( 'ldap' , 'eppn-domain' )
                bss_domain = cfg.get( 'bss' , 'domain' )
                if bss_domain != eppn_domain:
                    eppn_domain = bss_domain
                # On ajoute les groupes aux comptes
                for g in self.groups:
                    for uid in self.groups[ g ]:
                        eppn = '{}@{}'.format( uid , eppn_domain )
                        if eppn in self.accounts:
                            self.accounts[ eppn ].add_group( g )
                            continue
                        if uid in all_uids or query:
                            continue
                        Logging( 'ldap' ).warning(
                                'Groupe {} - utilisateur {} inconnu'.format( g ,
                                    eppn ) )

            def set_account_cos_( ):
                """
                Applique les règles d'attribution de classes de service aux
                comptes.
                """
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

            if query:
                Logging( 'ldap' ).debug( 'Filtre LDAP: {}'.format( query ) )
            self.groups = read_groups_( )
            ( all_uids , self.accounts ) = read_accounts_( )
            set_account_groups_( all_uids )
            set_account_cos_( )

    def set_account_aliases_( self , account , aliases , found ):
        """
        Méthode interne qui identifie les aliases correspondant à un compte LDAP
        et les ajoute à celui-ci.

        :param SyncAccount account: le compte
        :param AliasesMap aliases: l'instance de stockage des aliases
        :param set found: la liste des comptes ayant déjà été mis à jour
        """
        mn = aliases.get_main_account( account )
        if mn != account:
            return
        if mn in found:
            Logging( 'ldap' ).warning(
                    'Compte {} trouvé pour plus d\'une adresse' .format( mn ) )
            return
        found.add( mn )
        self.accounts[ account ].aliases = aliases.get_aliases( mn )
        delete = []
        for alias in self.accounts[ account ].aliases:
            if alias in self.accounts:
                Logging( 'ldap' ).debug( 'Alias de compte {} ignoré'.format(
                    alias ) )
                delete.append( alias )
        self.accounts[ account ].aliases.difference_update( delete )
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
        if cfg.has_flag( 'bss' , 'dont-fix-domains' ): return

        ldap_dom = '@{}'.format( cfg.get( 'ldap' , 'mail-domain' ) )
        bss_dom = '@{}'.format( cfg.get( 'bss' , 'domain' ) )
        if ldap_dom == bss_dom:
            return

        Logging( 'bss' ).warning( 'Domaine mail: {} -> {}'.format(
                ldap_dom , bss_dom ) )
        from .utils import get_address_fixer
        fix_it = get_address_fixer( cfg )
        for account in self.accounts.values( ):
            account.mail = fix_it( account.mail )
            if account.aliases is not None:
                account.aliases = set([ fix_it( a ) for a in account.aliases ])

    def clear_empty_sets( self ):
        """
        Remplace les ensembles (d'aliases et de groupes) vides par une valeur
        nulle.
        """
        for a in self.accounts.values( ):
            a.clear_empty_sets( )
