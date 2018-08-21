CREATE TABLE `software_versions` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `config` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `version` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `version` (`config`,`version`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 CHECKSUM=1;
