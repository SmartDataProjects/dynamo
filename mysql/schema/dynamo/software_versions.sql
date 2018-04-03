CREATE TABLE `software_versions` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `cycle` int(10) unsigned NOT NULL,
  `major` int(10) unsigned NOT NULL,
  `minor` int(10) unsigned NOT NULL,
  `suffix` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `release` (`cycle`,`major`,`minor`,`suffix`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
