CREATE TABLE `directory_cleaning_tasks` (
  `site_id` int(11) unsigned NOT NULL,
  `directory` varchar(512) COLLATE latin1_general_cs NOT NULL,
  UNIQUE KEY `pdn` (`site_id`,`directory`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
