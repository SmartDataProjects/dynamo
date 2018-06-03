CREATE TABLE `standalone_transfer_queue` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `status` enum('new','inprogress','done','failed') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'new',
  `exitcode` smallint(5) unsigned DEFAULT NULL,
  `finish_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `status` (`status`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
