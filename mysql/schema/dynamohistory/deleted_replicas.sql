CREATE TABLE `deleted_replicas` (
  `deletion_id` int(11) NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  `size` bigint(20) NOT NULL DEFAULT '-1',
  UNIQUE KEY `replica_deletion` (`deletion_id`,`dataset_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
