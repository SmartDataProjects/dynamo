CREATE TABLE `copy_requests` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `group` varchar(32) NOT NULL,
  `num_copies` tinyint(1) unsigned NOT NULL DEFAULT '1',
  `user_id` int(10) unsigned NOT NULL,
  `first_request_time` datetime NOT NULL,
  `last_request_time` datetime NOT NULL,
  `request_count` int(10) unsigned NOT NULL DEFAULT '1',
  `status` enum('new','activated','updated','completed','rejected','cancelled') NOT NULL DEFAULT 'new',
  `rejection_reason` text CHARACTER SET latin1 COLLATE latin1_general_cs,
  PRIMARY KEY (`id`),
  KEY `user` (`user_id`),
  KEY `first_request_time` (`first_request_time`),
  KEY `last_request_time` (`last_request_time`),
  KEY `request_count` (`request_count`),
  KEY `status` (`status`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
