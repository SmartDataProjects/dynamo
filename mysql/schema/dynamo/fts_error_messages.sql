CREATE TABLE `fts_error_messages` (
  `code` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `message` varchar(128) COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`code`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
