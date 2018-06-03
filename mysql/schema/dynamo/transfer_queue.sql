CREATE TABLE `transfer_queue` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `subscription_id` bigint(20) unsigned NOT NULL,
  `source_id` int(11) unsigned NOT NULL,
  `batch_id` bigint(20) unsigned NOT NULL,
  `created` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `transfer` (`subscription_id`),
  KEY `source` (`source_id`),
  KEY `batch` (`batch_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
