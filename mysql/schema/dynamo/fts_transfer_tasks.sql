CREATE TABLE `fts_transfer_tasks` (
  `id` bigint(20) unsigned NOT NULL,
  `fts_batch_id` bigint(20) unsigned NOT NULL,
  `fts_file_id` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fts` (`fts_batch_id`,`fts_file_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
