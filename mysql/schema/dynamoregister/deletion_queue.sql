CREATE TABLE `deletion_queue` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `desubscription_id` bigint(20) unsigned NOT NULL,
  `batch_id` bigint(20) unsigned NOT NULL,
  `created` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `desubscription` (`desubscription_id`),
  KEY `batch` (`batch_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
