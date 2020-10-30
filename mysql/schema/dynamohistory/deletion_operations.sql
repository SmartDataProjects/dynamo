CREATE TABLE `deletion_operations` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `timestamp` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `site_id` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `site` (`site_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
