DROP TABLE IF EXISTS `sites_snapshot_usage`;

CREATE TABLE `sites_snapshot_usage` (
  `run_id` int(11) unsigned NOT NULL,
  `timestamp` datetime NOT NULL,
  KEY `runs` (`run_id`),
  KEY `timestamps` (`timestamp`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
