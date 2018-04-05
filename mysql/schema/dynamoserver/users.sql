CREATE TABLE `users` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(64) COLLATE latin1_general_cs NOT NULL,
  `domain_id` int(10) unsigned NOT NULL,
  `email` varchar(128) COLLATE latin1_general_cs DEFAULT NULL,
  `dn` varchar(256) COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`,`domain_id`),
  UNIQUE KEY `dn` (`dn`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
