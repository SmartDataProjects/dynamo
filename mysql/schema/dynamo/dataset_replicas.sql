CREATE TABLE `dataset_replicas` (
  `dataset_id` int(11) unsigned NOT NULL,
  `site_id` int(11) unsigned NOT NULL,
  `growing` tinyint(1) unsigned NOT NULL,
  `group_id` int(11) unsigned NULL,
  PRIMARY KEY (`dataset_id`,`site_id`),
  KEY `sites` (`site_id`),
  KEY `groups` (`group_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 CHECKSUM=1;
