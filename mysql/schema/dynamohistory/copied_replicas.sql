CREATE TABLE `copied_replicas` (
  `copy_id` int(10) NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  `size` bigint(20) NOT NULL DEFAULT '-1',
  `status` enum('enroute','complete','cancelled') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'enroute',
  KEY `copy` (`copy_id`,`dataset_id`),
  KEY `status` (`status`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
