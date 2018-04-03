CREATE TABLE `copied_replicas` (
  `copy_id` int(10) NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  UNIQUE KEY `copy` (`copy_id`,`dataset_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
