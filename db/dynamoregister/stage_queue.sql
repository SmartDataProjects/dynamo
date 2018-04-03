DROP TABLE IF EXISTS `stage_queue`;

CREATE TABLE `stage_queue` (
  `file` varchar(512) COLLATE latin1_general_cs NOT NULL,
  `site` varchar(32) COLLATE latin1_general_cs NOT NULL,
  `status` enum('new','done','failed','inbatch') COLLATE latin1_general_cs NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `start` bigint(20) unsigned DEFAULT NULL,
  `finish` bigint(20) unsigned DEFAULT NULL,
  `batchid` varchar(40) COLLATE latin1_general_cs DEFAULT NULL,
  UNIQUE KEY `file_site` (`file`,`site`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
