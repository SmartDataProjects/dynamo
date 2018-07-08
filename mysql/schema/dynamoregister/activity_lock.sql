CREATE TABLE `activity_lock` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int(11) unsigned NOT NULL,
  `service_id` int(10) unsigned NOT NULL,
  `application` varchar(32) COLLATE latin1_general_cs NOT NULL,
  `timestamp` datetime NOT NULL,
  `note` text COLLATE latin1_general_cs,
  PRIMARY KEY (`id`),
  KEY `lock` (`user_id`,`service_id`,`application`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
