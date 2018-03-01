


DROP TABLE IF EXISTS `replicas`;
CREATE TABLE `replicas` (
  `site_id` int(10) unsigned NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  `size` bigint(20) unsigned NOT NULL,
  `decision` enum('delete','keep','protect') NOT NULL,
  `condition` int(10) unsigned NOT NULL,
  KEY `site_dataset` (`site_id`,`dataset_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `replicas_snapshot_usage`;
CREATE TABLE `replicas_snapshot_usage` (
  `run_id` int(11) unsigned NOT NULL,
  `timestamp` datetime NOT NULL,
  KEY `runs` (`run_id`),
  KEY `timestamps` (`timestamp`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `sites`;
CREATE TABLE `sites` (
  `site_id` int(10) unsigned NOT NULL,
  `status` enum('ready','waitroom','morgue','unknown') NOT NULL,
  `quota` int(10) NOT NULL,
  PRIMARY KEY (`site_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `sites_snapshot_usage`;
CREATE TABLE `sites_snapshot_usage` (
  `run_id` int(11) unsigned NOT NULL,
  `timestamp` datetime NOT NULL,
  KEY `runs` (`run_id`),
  KEY `timestamps` (`timestamp`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


