CREATE TABLE `dataset_popularity_snapshots` (
  `cycle_id` int(10) unsigned NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  `popularity` float NOT NULL,
  UNIQUE KEY `unique` (`cycle_id`,`dataset_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
