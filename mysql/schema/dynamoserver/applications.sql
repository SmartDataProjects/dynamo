CREATE TABLE `applications` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `auth_level` enum('noauth','auth','write') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `title` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `path` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `args` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `timeout` smallint(5) NOT NULL DEFAULT '0',
  `status` enum('new','assigned','run','done','notfound','authfailed','failed','killed') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `server` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `exit_code` int(10) DEFAULT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `user_host` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `email` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
