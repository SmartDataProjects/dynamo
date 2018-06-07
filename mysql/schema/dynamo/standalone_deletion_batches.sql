CREATE TABLE `standalone_deletion_batches` (
  `batch_id` bigint(20) unsigned NOT NULL,
  `site` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  PRIMARY KEY (`batch_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
