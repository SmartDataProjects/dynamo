CREATE TABLE `file_transfers` (
  `id` bigint(20) unsigned NOT NULL,
  `file_id` bigint(20) unsigned NOT NULL,
  `source_id` int(10) unsigned NOT NULL,
  `destination_id` int(10) unsigned NOT NULL,
  `exitcode` smallint(5) unsigned NOT NULL,
  `batch_id` bigint(20) unsigned NOT NULL,
  `created` datetime NOT NULL,
  `completed` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `transfer` (`file_id`, `source_id`, `destination_id`),
  KEY `batch` (`batch_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
