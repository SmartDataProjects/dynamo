CREATE TABLE `standalone_deletion_tasks` (
  `id` bigint(20) unsigned NOT NULL,
  `file` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `status` enum('new','queued','done','failed','cancelled') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'new',
  `exitcode` smallint(5) unsigned DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `finish_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `status` (`status`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
