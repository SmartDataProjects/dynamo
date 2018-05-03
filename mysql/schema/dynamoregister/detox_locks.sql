CREATE TABLE `detox_locks` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `item` varchar(512) COLLATE latin1_general_cs NOT NULL,
  `sites` varchar(64) COLLATE latin1_general_cs DEFAULT NULL,
  `groups` varchar(64) COLLATE latin1_general_cs DEFAULT NULL,
  `lock_date` datetime NOT NULL,
  `unlock_date` datetime DEFAULT NULL,
  `expiration_date` datetime NOT NULL,
  `user_id` int(11) unsigned NOT NULL,
  `role_id` int(10) unsigned NOT NULL,
  `comment` mediumtext COLLATE latin1_general_cs,
  PRIMARY KEY (`id`),
  KEY `unlocked` (`unlock_date`),
  KEY `locked` (`lock_date`),
  KEY `expires` (`expiration_date`),
  KEY `lock_data` (`item`,`sites`,`groups`),
  KEY `user_id` (`user_id`,`role_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
