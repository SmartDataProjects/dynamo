CREATE TABLE `deletion_operations` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `cycle_id` int(10) NOT NULL,
  `timestamp` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `site_id` int(10) unsigned NOT NULL DEFAULT '0',
  KEY `id` (`id`),
  KEY `cyclesite` (`cycle_id`,`site_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
