CREATE TABLE `transfer_queue` (
  `reqid` int(10) unsigned NOT NULL,
  `file` varchar(512) COLLATE latin1_general_cs NOT NULL,
  `site_from` varchar(32) COLLATE latin1_general_cs NOT NULL,
  `site_to` varchar(32) COLLATE latin1_general_cs NOT NULL,
  `status` enum('new','done','failed','inbatch','instage','staged') COLLATE latin1_general_cs NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `start` bigint(20) unsigned DEFAULT NULL,
  `finish` bigint(20) unsigned DEFAULT NULL,
  `batchid` varchar(40) COLLATE latin1_general_cs DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
