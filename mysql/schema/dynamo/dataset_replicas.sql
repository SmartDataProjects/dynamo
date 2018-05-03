CREATE TABLE `dataset_replicas` (
  `dataset_id` int(11) unsigned NOT NULL,
  `site_id` int(11) unsigned NOT NULL,
  `completion` enum('full','incomplete','partial') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `is_custodial` tinyint(1) NOT NULL DEFAULT '0',
  `last_block_created` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (`dataset_id`,`site_id`),
  KEY `sites` (`site_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 CHECKSUM=1;
