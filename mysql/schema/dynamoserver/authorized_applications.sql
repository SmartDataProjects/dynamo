CREATE TABLE `authorized_applications` (
  `user_id` int(10) unsigned NOT NULL,
  `title` varchar(128) COLLATE latin1_general_cs NOT NULL,
  `checksum` binary(16) NOT NULL,
  PRIMARY KEY (`user_id`,`title`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
