CREATE TABLE `fts_deletion_batches` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `batch_id` bigint(20) unsigned NOT NULL,
  `fts_server_id` smallint(5) unsigned NOT NULL,
  `job_id` varchar(64) COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`),
  KEY `batch` (`batch_id`),
  KEY `fts` (`fts_server_id`,`job_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
