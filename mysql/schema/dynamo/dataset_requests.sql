CREATE TABLE `dataset_requests` (
  `id` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  `queue_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `completion_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `nodes_total` int(11) NOT NULL DEFAULT '0',
  `nodes_done` int(11) NOT NULL DEFAULT '0',
  `nodes_failed` int(11) NOT NULL DEFAULT '0',
  `nodes_queued` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `dataset_id` (`dataset_id`),
  KEY `timestamp` (`queue_time`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
