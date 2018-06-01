CREATE TABLE `directory_cleaning_queue` (
  `site_id` int(11) unsigned NOT NULL,
  `directory` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  UNIQUE KEY `pdn` (`site_id`,`directory`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
