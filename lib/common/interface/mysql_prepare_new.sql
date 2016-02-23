DROP TABLE IF EXISTS `block_replicas`;

CREATE TABLE `block_replicas` (
  `block_id` bigint(20) unsigned NOT NULL,
  `site_id` int(10) unsigned NOT NULL,
  `is_custodial` tinyint(1) NOT NULL DEFAULT '0'
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS `blocks`;

CREATE TABLE `blocks` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `dataset_id` int(10) unsigned NOT NULL DEFAULT '0',
  `name` varchar(128) NOT NULL,
  `size` bigint(20) NOT NULL DEFAULT '-1',
  `num_files` int(11) NOT NULL DEFAULT '0',
  `is_open` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS `dataset_replicas`;

CREATE TABLE `dataset_replicas` (
  `dataset_id` int(11) unsigned NOT NULL,
  `site_id` int(11) unsigned NOT NULL,
  `is_partial` tinyint(1) NOT NULL DEFAULT '0',
  `is_custodial` tinyint(1) NOT NULL DEFAULT '0',
  UNIQUE KEY `unique` (`dataset_id`,`site_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS `datasets`;

CREATE TABLE `datasets` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(512) NOT NULL,
  `size` bigint(20) NOT NULL DEFAULT '-1',
  `num_files` int(10) unsigned NOT NULL DEFAULT '0',
  `is_open` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS `sites`;

CREATE TABLE `sites` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(32) NOT NULL,
  `capacity` bigint(20) unsigned NOT NULL DEFAULT '0',
  `used_total` bigint(20) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;
