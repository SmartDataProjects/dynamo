CREATE TABLE `copy_requests` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `group` varchar(32) NOT NULL,
  `num_copies` tinyint(1) unsigned NOT NULL DEFAULT '1',
  `user` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `dn` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `first_request_time` datetime NOT NULL,
  `last_request_time` datetime NOT NULL,
  `request_count` int(10) unsigned NOT NULL DEFAULT '1',
  `status` enum('new','activated') NOT NULL DEFAULT 'new',
  PRIMARY KEY (`id`),
  KEY `first_request_time` (`first_request_time`),
  KEY `last_request_time` (`last_request_time`),
  KEY `request_count` (`request_count`),
  KEY `status` (`status`),
  KEY `user` (`user`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
