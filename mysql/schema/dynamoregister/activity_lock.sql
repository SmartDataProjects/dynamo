CREATE TABLE `activity_lock` (
  `user_id` int(11) unsigned NOT NULL,
  `role_id` int(10) unsigned NOT NULL,
  `application` enum('detox','dealer') COLLATE latin1_general_cs NOT NULL,
  `timestamp` datetime NOT NULL,
  `note` text COLLATE latin1_general_cs,
  KEY `lock` (`user_id`,`role_id`,`application`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
