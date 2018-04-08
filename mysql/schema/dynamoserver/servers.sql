CREATE TABLE `servers` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `hostname` varchar(32) COLLATE latin1_general_cs NOT NULL,
  `last_heartbeat` datetime NOT NULL,
  `status` enum('initial','starting','online','updating','error','outofsync') COLLATE latin1_general_cs NOT NULL DEFAULT 'initial',
  `store_host` int(10) unsigned NOT NULL DEFAULT '0',
  `store_module` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,
  `store_config` varchar(1024) COLLATE latin1_general_cs DEFAULT NULL,
  `shadow_module` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,
  `shadow_config` varchar(1024) COLLATE latin1_general_cs DEFAULT NULL,
  `board_module` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,
  `board_config` varchar(1024) COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
