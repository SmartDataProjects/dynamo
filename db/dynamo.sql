


DROP TABLE IF EXISTS `block_replica_sizes`;
CREATE TABLE `block_replica_sizes` (
  `block_id` bigint(20) unsigned NOT NULL,
  `site_id` int(10) unsigned NOT NULL,
  `size` bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`block_id`,`site_id`),
  KEY `sites` (`site_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `block_replicas`;
CREATE TABLE `block_replicas` (
  `block_id` bigint(20) unsigned NOT NULL,
  `site_id` int(10) unsigned NOT NULL,
  `group_id` int(11) unsigned NOT NULL,
  `is_complete` tinyint(1) NOT NULL DEFAULT '0',
  `is_custodial` tinyint(1) NOT NULL DEFAULT '0',
  `last_update` datetime NOT NULL,
  PRIMARY KEY (`block_id`,`site_id`),
  KEY `sites` (`site_id`),
  KEY `groups` (`group_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `blocks`;
CREATE TABLE `blocks` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `dataset_id` int(10) unsigned NOT NULL DEFAULT '0',
  `name` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `size` bigint(20) NOT NULL DEFAULT '-1',
  `num_files` int(11) NOT NULL DEFAULT '0',
  `is_open` tinyint(1) NOT NULL,
  `last_update` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  KEY `datasets` (`dataset_id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `dataset_accesses`;
CREATE TABLE `dataset_accesses` (
  `dataset_id` int(10) unsigned NOT NULL,
  `site_id` int(10) unsigned NOT NULL,
  `date` date NOT NULL DEFAULT '0000-00-00',
  `access_type` enum('local','remote') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'local',
  `num_accesses` int(11) NOT NULL DEFAULT '0',
  `cputime` float NOT NULL DEFAULT '0',
  PRIMARY KEY (`dataset_id`,`site_id`,`date`),
  KEY `sites` (`site_id`),
  KEY `dates` (`date`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `dataset_replicas`;
CREATE TABLE `dataset_replicas` (
  `dataset_id` int(11) unsigned NOT NULL,
  `site_id` int(11) unsigned NOT NULL,
  `completion` enum('full','incomplete','partial') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `is_custodial` tinyint(1) NOT NULL DEFAULT '0',
  `last_block_created` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (`dataset_id`,`site_id`),
  KEY `sites` (`site_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `dataset_requests`;
CREATE TABLE `dataset_requests` (
  `id` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  `queue_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `completion_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `nodes_total` int(11) NOT NULL DEFAULT '0',
  `nodes_done` int(11) NOT NULL DEFAULT '0',
  `nodes_failed` int(11) NOT NULL DEFAULT '0',
  `nodes_queued` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `dataset_id` (`dataset_id`),
  KEY `timestamp` (`queue_time`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `datasets`;
CREATE TABLE `datasets` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `size` bigint(20) NOT NULL DEFAULT '-1',
  `num_files` int(10) unsigned NOT NULL DEFAULT '0',
  `status` enum('UNKNOWN','DELETED','DEPRECATED','INVALID','PRODUCTION','VALID','IGNORED') CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  `data_type` enum('UNKNOWN','ALIGN','CALIB','COSMIC','DATA','LUMI','MC','RAW','TEST','') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'UNKNOWN',
  `software_version_id` int(10) unsigned NOT NULL DEFAULT '0',
  `last_update` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `is_open` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


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


DROP TABLE IF EXISTS `groups`;
CREATE TABLE `groups` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `olevel` enum('Dataset','Block') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'Block',
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `partitions`;
CREATE TABLE `partitions` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(16) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `quotas`;
CREATE TABLE `quotas` (
  `site_id` int(10) unsigned NOT NULL,
  `partition_id` int(10) unsigned NOT NULL,
  `storage` int(10) unsigned NOT NULL,
  PRIMARY KEY (`site_id`,`partition_id`),
  KEY `partitions` (`partition_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `sites`;
CREATE TABLE `sites` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `host` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  `storage_type` enum('disk','mss','buffer','unknown') CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  `backend` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  `status` enum('ready','waitroom','morgue','unknown') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'ready',
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `software_versions`;
CREATE TABLE `software_versions` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `cycle` int(10) unsigned NOT NULL,
  `major` int(10) unsigned NOT NULL,
  `minor` int(10) unsigned NOT NULL,
  `suffix` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `release` (`cycle`,`major`,`minor`,`suffix`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `system`;
CREATE TABLE `system` (
  `lock_host` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT '',
  `lock_process` int(11) NOT NULL DEFAULT '0',
  `last_update` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dataset_accesses_last_update` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dataset_requests_last_update` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  UNIQUE KEY `lock` (`lock_host`,`lock_process`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


