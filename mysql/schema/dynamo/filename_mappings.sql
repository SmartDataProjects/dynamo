CREATE TABLE `filename_mappings` (
  `site_id` int(11) unsigned NOT NULL,
  `protocol` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `chain_id` int(11) unsigned NOT NULL,
  `index` int(11) unsigned NOT NULL,
  `lfn_pattern` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `pfn_pattern` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  KEY (`site_id`, `protocol`, `chain_id`, `index`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 CHECKSUM=1;
