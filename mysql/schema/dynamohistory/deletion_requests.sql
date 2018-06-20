CREATE TABLE `deletion_requests` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int(10) unsigned NOT NULL,
  `request_time` datetime NOT NULL,
  `status` enum('new','activated','completed','rejected','cancelled') NOT NULL DEFAULT 'new',
  `rejection_reason` text CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `user` (`user_id`),
  KEY `request_time` (`request_time`),
  KEY `status` (`status`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
