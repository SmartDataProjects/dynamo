
CREATE TABLE `cached_copy_requests` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `sites` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `item` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `group` varchar(32) NOT NULL,
  `num_copies` tinyint(1) unsigned NOT NULL DEFAULT '1',
  `user` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `dn` varchar(256) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `request_time` datetime NOT NULL,
  `status` enum('new','activated') NOT NULL DEFAULT 'new',
  PRIMARY KEY (`id`),
  KEY `request_time` (`request_time`),
  KEY `status` (`status`),
  KEY `user` (`user`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
