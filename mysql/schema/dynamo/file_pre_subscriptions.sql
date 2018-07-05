CREATE TABLE `file_pre_subscriptions` (
  `file_name` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `site_name` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `created` datetime NOT NULL,
  `delete` tinyint(1) unsigned NOT NULL,
  UNIQUE KEY `subscription` (`file_name`,`site_name`,`delete`),
  KEY `delete` (`delete`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 CHECKSUM=1;
