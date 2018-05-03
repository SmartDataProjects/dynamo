CREATE TABLE `deletion_requests` (
  `id` int(10) NOT NULL,
  `cycle_id` int(10) NOT NULL,
  `timestamp` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `approved` tinyint(1) NOT NULL DEFAULT '0',
  `site_id` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `cycle` (`cycle_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
