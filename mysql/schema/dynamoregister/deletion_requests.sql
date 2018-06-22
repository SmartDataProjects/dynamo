CREATE TABLE `deletion_requests` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `user` varchar(64) COLLATE latin1_general_cs NOT NULL,
  `dn` varchar(256) COLLATE latin1_general_cs DEFAULT NULL,
  `request_time` datetime NOT NULL,
  `status` enum('new','activated') COLLATE latin1_general_cs NOT NULL DEFAULT 'new',
  PRIMARY KEY (`id`),
  KEY `request_time` (`request_time`),
  KEY `status` (`status`),
  KEY `user` (`user`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
