CREATE TABLE `block_replicas` (
  `block_id` bigint(20) unsigned NOT NULL,
  `site_id` int(10) unsigned NOT NULL,
  `group_id` int(11) unsigned NOT NULL,
  `is_custodial` tinyint(1) NOT NULL DEFAULT 0,
  `last_update` datetime NOT NULL,
  `is_complete` tinyint(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`block_id`,`site_id`),
  KEY `sites` (`site_id`),
  KEY `groups` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 CHECKSUM=1;
