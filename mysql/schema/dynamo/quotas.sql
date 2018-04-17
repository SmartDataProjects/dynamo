CREATE TABLE `quotas` (
  `site_id` int(10) unsigned NOT NULL,
  `partition_id` int(10) unsigned NOT NULL,
  `storage` int(10) NOT NULL,
  PRIMARY KEY (`site_id`,`partition_id`),
  KEY `partitions` (`partition_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
