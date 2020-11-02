CREATE TABLE `transfer_protocols` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `protocol` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `protocol` (`protocol`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 CHECKSUM=1;
