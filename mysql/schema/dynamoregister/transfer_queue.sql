CREATE TABLE `transfer_queue` (
  `reqid` int(10) unsigned NOT NULL,
  `file` varchar(512) COLLATE latin1_general_cs NOT NULL,
  `site_from` varchar(32) COLLATE latin1_general_cs NOT NULL,
  `site_to` varchar(32) COLLATE latin1_general_cs NOT NULL,
  `status` enum('new','done','failed','inbatch','instage','staged','phedex') COLLATE latin1_general_cs NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `start` bigint(20) unsigned DEFAULT NULL,
  `finish` bigint(20) unsigned DEFAULT NULL,
  `batchid` varchar(40) COLLATE latin1_general_cs DEFAULT NULL,
  `fsize` bigint(20) unsigned NOT NULL DEFAULT '0',
  `failcode` smallint(5) unsigned DEFAULT NULL,
  `ntries` smallint(5) unsigned NOT NULL DEFAULT '0',
  UNIQUE KEY `transferid` (`site_from`,`site_to`,`file`),
  KEY `requestind` (`reqid`),
  KEY `reqid_file` (`reqid`,`file`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
