CREATE TABLE `dataset_popularity_snapshots` (
  `run_id` int(10) unsigned NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  `popularity` float NOT NULL,
  UNIQUE KEY `unique` (`run_id`,`dataset_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
