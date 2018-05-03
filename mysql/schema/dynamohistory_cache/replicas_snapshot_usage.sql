CREATE TABLE `replicas_snapshot_usage` (
  `cycle_id` int(11) unsigned NOT NULL,
  `timestamp` datetime NOT NULL,
  KEY `cycles` (`cycle_id`),
  KEY `timestamps` (`timestamp`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
