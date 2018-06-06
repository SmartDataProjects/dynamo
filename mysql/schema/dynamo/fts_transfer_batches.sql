CREATE TABLE `fts_transfer_batches` (
  `batch_id` bigint(20) unsigned NOT NULL,
  `fts_server_id` smallint(5) unsigned NOT NULL,
  `job_id` varchar(64) COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`batch_id`),
  KEY `fts` (`fts_server_id`,`job_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
