CREATE TABLE `invalidations` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `cmd` enum('update','delete') NOT NULL,
  `obj` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
