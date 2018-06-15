CREATE TABLE `block_replica_sizes` (
  `block_id` bigint(20) unsigned NOT NULL,
  `site_id` int(10) unsigned NOT NULL,
  `num_files` int(11) NOT NULL DEFAULT '0',
  `size` bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`block_id`,`site_id`),
  KEY `sites` (`site_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 CHECKSUM=1;
