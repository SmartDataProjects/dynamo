


DROP TABLE IF EXISTS `action`;
CREATE TABLE `action` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `write_request` tinyint(1) NOT NULL,
  `title` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `path` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `args` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `status` enum('new','run','done','notfound','authfailed','failed','killed') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `exit_code` int(10) unsigned DEFAULT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `type` enum('executable','deletion_policy') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `email` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `activity_lock`;
CREATE TABLE `activity_lock` (
  `user_id` int(10) unsigned NOT NULL,
  `service_id` int(10) unsigned NOT NULL,
  `application` enum('detox','dealer') COLLATE latin1_general_cs NOT NULL,
  `timestamp` datetime NOT NULL,
  `note` text COLLATE latin1_general_cs,
  UNIQUE KEY `lock` (`user_id`, `service_id`, `application`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;


DROP TABLE IF EXISTS `authorized_executables`;
CREATE TABLE `authorized_executables` (
  `user_id` int(10) unsigned NOT NULL,
  `title` varchar(128) COLLATE latin1_general_cs NOT NULL,
  `checksum` binary(16) NOT NULL,
  PRIMARY KEY (`user_id`,`title`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;


DROP TABLE IF EXISTS `authorized_users`;
CREATE TABLE `authorized_users` (
  `user_id` int(10) unsigned NOT NULL,
  `service_id` int(10) unsigned NOT NULL,
  UNIQUE KEY `user` (`user_id`,`service_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;


DROP TABLE IF EXISTS `deletion_queue`;
CREATE TABLE `deletion_queue` (
  `reqid` int(10) unsigned NOT NULL DEFAULT '0',
  `file` varchar(512) COLLATE latin1_general_cs NOT NULL,
  `site` varchar(32) COLLATE latin1_general_cs NOT NULL,
  `status` enum('new','done','failed') COLLATE latin1_general_cs NOT NULL,
  UNIQUE KEY `file` (`file`,`site`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;


DROP TABLE IF EXISTS `detox_locks`;
CREATE TABLE `detox_locks` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `item` varchar(512) COLLATE latin1_general_cs NOT NULL,
  `sites` varchar(64) COLLATE latin1_general_cs DEFAULT NULL,
  `groups` varchar(64) COLLATE latin1_general_cs DEFAULT NULL,
  `lock_date` datetime NOT NULL,
  `unlock_date` datetime DEFAULT NULL,
  `expiration_date` datetime NOT NULL,
  `user_id` int(11) unsigned NOT NULL,
  `service_id` int(10) unsigned NOT NULL,
  `comment` mediumtext COLLATE latin1_general_cs,
  PRIMARY KEY (`id`),
  KEY `unlocked` (`unlock_date`),
  KEY `locked` (`lock_date`),
  KEY `expires` (`expiration_date`),
  KEY `lock_data` (`item`,`sites`,`groups`),
  KEY `user_id` (`user_id`,`service_id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;


DROP TABLE IF EXISTS `domains`;
CREATE TABLE `domains` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(32) COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;


DROP TABLE IF EXISTS `executables`;
CREATE TABLE `executables` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `title` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `path` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `body` mediumtext CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `write_request` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `invalidations`;
CREATE TABLE `invalidations` (
  `item` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `user_id` int(10) unsigned NOT NULL DEFAULT '0',
  `timestamp` datetime NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `transfer_requests`;
CREATE TABLE `transfer_requests` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `item` varchar(512) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `site` varchar(32) NOT NULL,
  `group` varchar(32)  NOT NULL,
  `num_copies` tinyint(1) NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `first_request_time` datetime NOT NULL,
  `last_request_time` datetime NOT NULL,
  `request_count` int(10) unsigned NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`),
  UNIQUE KEY `request` (`item`,`site`,`user_id`),
  KEY `site` (`site`),
  KEY `user` (`user_id`),
  KEY `first_request_time` (`first_request_time`),
  KEY `last_request_time` (`last_request_time`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `deletion_requests`;
CREATE TABLE `deletion_requests` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `item` varchar(512) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `site` varchar(32) NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `timestamp` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `request` (`item`,`site`,`user_id`),
  KEY `site` (`site`),
  KEY `user` (`user_id`),
  KEY `timestamp` (`timestamp`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `active_transfers`;
CREATE TABLE `active_transfers` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `request_id` int(10) unsigned NOT NULL,
  `item` varchar(512) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `site` varchar(32) NOT NULL,
  `group` varchar(32)  NOT NULL,
  `timestamp` datetime NOT NULL,
  `rank` int(10) unsigned DEFAULT '0',
  `status` enum('new','queued','failed','completed') NOT NULL,
  `created` datetime NOT NULL,
  `updated` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `request` (`request_id`),
  KEY `item` (`item`),
  KEY `site` (`site`),  
  KEY `timestamp` (`timestamp`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `active_deletions`;
CREATE TABLE `active_deletions` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `request_id` int(10) unsigned NOT NULL,
  `item` varchar(512) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `site` varchar(32) NOT NULL,
  `timestamp` datetime NOT NULL,
  `status` enum('new','queued') NOT NULL,
  `created` datetime NOT NULL,
  `updated` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `request` (`request_id`),
  KEY `item` (`item`),
  KEY `site` (`site`),  
  KEY `timestamp` (`timestamp`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `services`;
CREATE TABLE `services` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(32) COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;


DROP TABLE IF EXISTS `transfer_queue`;
CREATE TABLE `transfer_queue` (
  `reqid` int(10) unsigned NOT NULL,
  `file` varchar(512) COLLATE latin1_general_cs NOT NULL,
  `site_from` varchar(32) COLLATE latin1_general_cs NOT NULL,
  `site_to` varchar(32) COLLATE latin1_general_cs NOT NULL,
  `status` enum('new','done','failed') COLLATE latin1_general_cs NOT NULL,
  UNIQUE KEY `file` (`file`,`site_from`,`site_to`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;


DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(64) COLLATE latin1_general_cs NOT NULL,
  `domain_id` int(10) unsigned NOT NULL,
  `email` varchar(128) COLLATE latin1_general_cs DEFAULT NULL,
  `dn` varchar(256) COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`,`domain_id`),
  UNIQUE KEY `dn` (`dn`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;


