CREATE TABLE `file_desubscriptions` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `block_id` bigint(20) unsigned NOT NULL,
  `file_id` bigint(20) unsigned NOT NULL,
  `site_id` int(11) unsigned NOT NULL,
  `status` enum('new','inbatch','done','retry','held') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `created` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `subscription` (`file_id`,`site_id`),
  KEY `block` (`block_id`,`site_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 CHECKSUM=1;
