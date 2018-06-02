CREATE TABLE `fts_deletion_files` (
  `deletion_id` bigint(20) unsigned NOT NULL,
  `batch_id` bigint(20) unsigned NOT NULL,
  `fts_file_id` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`deletion_id`),
  KEY `fts` (`batch_id`, `fts_file_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
