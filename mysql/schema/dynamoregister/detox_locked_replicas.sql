CREATE TABLE `detox_locked_replicas` (
  `item` varchar(512) COLLATE latin1_general_cs NOT NULL,
  `site` varchar(64) COLLATE latin1_general_cs DEFAULT NULL,
  UNIQUE KEY `lock` (`item`, `site`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
