CREATE TABLE `files` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `block_id` bigint(20) unsigned NOT NULL DEFAULT '0',
  `size` bigint(20) NOT NULL DEFAULT '-1',
  `name` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  KEY `blocks` (`block_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 CHECKSUM=1;
