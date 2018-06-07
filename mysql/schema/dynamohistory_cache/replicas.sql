CREATE TABLE `replicas` (
  `site_id` int(10) unsigned NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  `size` bigint(20) unsigned NOT NULL,
  `decision` enum('delete','keep','protect') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `condition` int(10) unsigned NOT NULL,
  KEY `site_dataset` (`site_id`,`dataset_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
