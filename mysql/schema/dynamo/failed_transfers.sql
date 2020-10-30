CREATE TABLE `failed_transfers` (
  `id` bigint(20) unsigned NOT NULL,
  `subscription_id` int(10) unsigned NOT NULL,
  `source_id` int(11) unsigned NOT NULL,
  `exitcode` smallint(5) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `transfer` (`subscription_id`,`source_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
