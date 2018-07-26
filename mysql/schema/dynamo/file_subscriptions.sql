CREATE TABLE `file_subscriptions` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `file_id` bigint(20) unsigned NOT NULL,
  `site_id` int(11) unsigned NOT NULL,
  `status` enum('new','inbatch','done','retry','held','cancelled') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'new',
  `created` datetime NOT NULL,
  `last_update` datetime DEFAULT NULL,
  `delete` tinyint(1) unsigned NOT NULL,
  `hold_reason` enum('no_source','all_failed','site_unavailable') CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `subscription` (`file_id`,`site_id`,`delete`),
  KEY `delete` (`delete`),
  KEY `status` (`status`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 CHECKSUM=1;
