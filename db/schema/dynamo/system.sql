CREATE TABLE `system` (
  `lock_host` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT '',
  `lock_process` int(11) NOT NULL DEFAULT '0',
  `last_update` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dataset_accesses_last_update` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dataset_requests_last_update` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  UNIQUE KEY `lock` (`lock_host`,`lock_process`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
