CREATE TABLE `filename_mappings` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `site_id` int(11) unsigned NOT NULL,
  `protocol_id` int(10) unsigned NOT NULL,
  `chain_id` int(11) unsigned NOT NULL,
  `index` int(11) unsigned NOT NULL,
  `lfn_pattern` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `pfn_pattern` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `links` (`site_id`,`protocol_id`,`chain_id`,`index`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 CHECKSUM=1;
