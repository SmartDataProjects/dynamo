CREATE TABLE `fts_staging_queue` (
  `id` bigint(20) unsigned NOT NULL,
  `source` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `destination` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `checksum` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  `size` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
