CREATE TABLE `data_injections` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `cmd` enum('update','delete') NOT NULL,
  `obj` text CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
