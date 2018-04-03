DROP TABLE IF EXISTS `files`;

CREATE TABLE `files` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `block_id` bigint(20) unsigned NOT NULL DEFAULT '0',
  `dataset_id` int(10) unsigned NOT NULL DEFAULT '0',
  `size` bigint(20) NOT NULL DEFAULT '-1',
  `name` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  KEY `datasets` (`dataset_id`),
  KEY `blocks` (`block_id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
