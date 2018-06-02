CREATE TABLE `fts_servers` (
  `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
  `url` varchar(128) COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
