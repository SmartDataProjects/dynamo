DROP TABLE IF EXISTS `authorized_executables`;

CREATE TABLE `authorized_executables` (
  `user_id` int(10) unsigned NOT NULL,
  `title` varchar(128) COLLATE latin1_general_cs NOT NULL,
  `checksum` binary(16) NOT NULL,
  PRIMARY KEY (`user_id`,`title`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
