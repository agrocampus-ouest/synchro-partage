Scripts de synchronisation Partage
===================================

Ces scripts sont utilisés au sein d'AGROCAMPUS OUEST pour la synchronisation des
comptes, des redirections vers les listes de diffusions et des calendriers entre
les éléments locaux du système d'information de l'établissement et la
plate-forme RENATER Partage.


Librairies requises
--------------------

Les scripts présents dans ce dépôt requièrent les librairies suivantes :

  * la librairie d'interface avec le BSS développée par l'Université Rennes 1,
disponible sur [son dépôt GitHub](https://github.com/dsi-univ-rennes1/libPythonBssApi),
ainsi que ses pré-requis ;

  * la librairie `lmdb` utilisée pour la base de données intermédiaire (à
installer via `pip`) ;

  * la librairie `ldap3` utilisée pour l'accès à l'annuaire LDAP (à installer
via `pip`) ;

  * la librairie `python-zimbra` pour l'accès à l'API Zimbra ;

  * additionnellement, toute librairie d'interface avec une base de données
depuis laquelle il serait nécessaire de lire des URL WebCal.

