CREATE TABLE `unmanaged_deletions` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `site` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `url` varchar(512) COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`),
  KEY `sites` (`site`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
