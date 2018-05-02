


DROP TABLE IF EXISTS `copied_replicas`;
CREATE TABLE `copied_replicas` (
  `copy_id` int(10) NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  `size` bigint(20) NOT NULL DEFAULT '-1',
  `status` enum('enroute','complete','cancelled') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'enroute',
  UNIQUE KEY `copy` (`copy_id`,`dataset_id`),
  KEY `status` (`status`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `copy_requests`;
CREATE TABLE `copy_requests` (
  `id` int(10) NOT NULL,
  `run_id` int(10) NOT NULL,
  `timestamp` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `approved` tinyint(1) NOT NULL DEFAULT '0',
  `site_id` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `run` (`run_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `dataset_popularity_snapshots`;
CREATE TABLE `dataset_popularity_snapshots` (
  `run_id` int(10) unsigned NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  `popularity` float NOT NULL,
  UNIQUE KEY `unique` (`run_id`,`dataset_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `datasets`;
CREATE TABLE `datasets` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `deleted_replicas`;
CREATE TABLE `deleted_replicas` (
  `deletion_id` int(11) NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  `size` bigint(20) NOT NULL DEFAULT '-1',
  UNIQUE KEY `replica_deletion` (`deletion_id`,`dataset_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `deletion_requests`;
CREATE TABLE `deletion_requests` (
  `id` int(10) NOT NULL,
  `run_id` int(10) NOT NULL,
  `timestamp` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `approved` tinyint(1) NOT NULL DEFAULT '0',
  `site_id` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `lock`;
CREATE TABLE `lock` (
  `lock_host` varchar(256) NOT NULL DEFAULT '',
  `lock_process` int(11) NOT NULL DEFAULT '0',
  UNIQUE KEY `host` (`lock_host`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `partitions`;
CREATE TABLE `partitions` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `policy_conditions`;
CREATE TABLE `policy_conditions` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `text` varchar(512) COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `text` (`text`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;


DROP TABLE IF EXISTS `runs`;
CREATE TABLE `runs` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `operation` enum('copy','deletion','copy_test','deletion_test') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `partition_id` int(10) unsigned NOT NULL,
  `policy_version` varchar(16) NOT NULL DEFAULT '',
  `comment` text,
  `time_start` datetime NOT NULL,
  `time_end` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (`id`),
  KEY `operations` (`operation`),
  KEY `partitions` (`partition_id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `sites`;
CREATE TABLE `sites` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(32) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


