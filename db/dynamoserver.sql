


DROP TABLE IF EXISTS `authorized_executables`;
CREATE TABLE `authorized_executables` (
  `user_id` int(10) unsigned NOT NULL,
  `title` varchar(128) COLLATE latin1_general_cs NOT NULL,
  `checksum` binary(16) NOT NULL,
  PRIMARY KEY (`user_id`,`title`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;


DROP TABLE IF EXISTS `executables`;
CREATE TABLE `executables` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `write_request` tinyint(1) NOT NULL,
  `title` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `path` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `args` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `status` enum('new','assigned','run','done','notfound','authfailed','failed','killed') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `server_id` int(10) unsigned NOT NULL DEFAULT 0,
  `exit_code` int(10) unsigned DEFAULT NULL,
  `user` varchar(64) COLLATE latin1_general_cs NOT NULL,
  `type` enum('executable','deletion_policy') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `email` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `inventory_updates`;
CREATE TABLE `invalidations` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `cmd` enum('update','delete') NOT NULL,
  `obj` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `servers`;
CREATE TABLE `servers` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `hostname` varchar(32) COLLATE latin1_general_cs NOT NULL,
  `last_heartbeat` datetime NOT NULL,
  `status` enum('initial','starting','online','updating','error','outofsync') NOT NULL DEFAULT 'initial',
  `store_host` int(10) unsigned NOT NULL DEFAULT 0,
  `store_module` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,
  `store_config` varchar(1024) COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;


