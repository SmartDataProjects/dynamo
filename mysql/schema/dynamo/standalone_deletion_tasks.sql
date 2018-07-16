CREATE TABLE `standalone_deletion_tasks` (
  `id` bigint(20) unsigned NOT NULL,
  `file` varchar(512) COLLATE latin1_general_cs NOT NULL,
  `status` enum('new','queued','active','done','failed','cancelled') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'new',
  `exitcode` smallint(5) DEFAULT NULL,
  `message` varchar(512) COLLATE latin1_general_cs DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `finish_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `status` (`status`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
