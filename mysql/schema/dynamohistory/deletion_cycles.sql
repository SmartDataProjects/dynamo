CREATE TABLE `deletion_cycles` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `operation` enum('deletion','deletion_test') NOT NULL,
  `partition_id` int(10) unsigned NOT NULL,
  `policy_id` varchar(16) NOT NULL DEFAULT '',
  `comment` text,
  `time_start` datetime NOT NULL,
  `time_end` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (`id`),
  KEY `operations` (`operation`),
  KEY `partitions` (`partition_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
