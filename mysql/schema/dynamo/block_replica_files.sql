CREATE TABLE `block_replica_files` (
  `block_id` bigint(20) unsigned NOT NULL,
  `site_id` int(10) unsigned NOT NULL,
  `file_id` bigint(20) NOT NULL,
  UNIQUE KEY `filereplica` (`file_id`,`site_id`),
  KEY `blockreplica` (`block_id`,`site_id`),
  KEY `site` (`site_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 CHECKSUM=1;
