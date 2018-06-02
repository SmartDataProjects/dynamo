CREATE TABLE `fts_transfer_files` (
  `transfer_id` bigint(20) unsigned NOT NULL,
  `batch_id` bigint(20) unsigned NOT NULL,
  `fts_file_id` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`transfer_id`),
  KEY `fts` (`batch_id`,`fts_file_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
