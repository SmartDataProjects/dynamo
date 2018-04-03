CREATE TABLE `deleted_replicas` (
  `deletion_id` int(11) NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  UNIQUE KEY `replica_deletion` (`deletion_id`,`dataset_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
