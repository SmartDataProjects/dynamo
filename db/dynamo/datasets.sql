DROP TABLE IF EXISTS `datasets`;

CREATE TABLE `datasets` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `size` bigint(20) NOT NULL DEFAULT '-1',
  `num_files` int(10) unsigned NOT NULL DEFAULT '0',
  `status` enum('UNKNOWN','DELETED','DEPRECATED','INVALID','PRODUCTION','VALID','IGNORED') CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  `data_type` enum('UNKNOWN','ALIGN','CALIB','COSMIC','DATA','LUMI','MC','RAW','TEST','') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'UNKNOWN',
  `software_version_id` int(10) unsigned NOT NULL DEFAULT '0',
  `last_update` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `is_open` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
