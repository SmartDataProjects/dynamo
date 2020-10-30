CREATE TABLE `blocks` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `dataset_id` int(10) unsigned NOT NULL DEFAULT 0,
  `name` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `size` bigint(20) NOT NULL DEFAULT '-1',
  `num_files` int(11) NOT NULL DEFAULT 0,
  `is_open` tinyint(1) NOT NULL,
  `last_update` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `full_name` (`dataset_id`,`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 CHECKSUM=1;
