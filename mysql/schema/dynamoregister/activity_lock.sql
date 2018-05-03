CREATE TABLE `activity_lock` (
  `user` varchar(64) COLLATE latin1_general_cs NOT NULL,
  `role` varchar(32) COLLATE latin1_general_cs NOT NULL,
  `application` enum('detox','dealer') COLLATE latin1_general_cs NOT NULL,
  `timestamp` datetime NOT NULL,
  `note` text COLLATE latin1_general_cs,
  KEY `lock` (`user`,`role`,`application`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
