CREATE TABLE `fts_transfer_batches` (
  `id` bigint(20) unsigned NOT NULL,
  `batch_id` bigint(20) unsigned NOT NULL,
  `task_type` enum('transfer','staging') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `fts_server_id` smallint(5) unsigned NOT NULL,
  `job_id` varchar(64) COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`),
  KEY `batch` (`batch_id`,`task_type`),
  KEY `fts` (`fts_server_id`,`job_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
