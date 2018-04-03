CREATE TABLE `executables` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `write_request` tinyint(1) NOT NULL,
  `title` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `path` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `args` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `status` enum('new','assigned','run','done','notfound','authfailed','failed','killed') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `server_id` int(10) unsigned NOT NULL DEFAULT '0',
  `exit_code` int(10) unsigned DEFAULT NULL,
  `user` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `type` enum('executable','deletion_policy') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `email` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
