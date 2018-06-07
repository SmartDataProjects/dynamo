CREATE TABLE `standalone_transfer_batches` (
  `batch_id` bigint(20) unsigned NOT NULL,
  `source_site` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `destination_site` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  PRIMARY KEY (`batch_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
