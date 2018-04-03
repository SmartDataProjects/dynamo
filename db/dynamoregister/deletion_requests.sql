CREATE TABLE `deletion_requests` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int(10) unsigned NOT NULL,
  `timestamp` datetime NOT NULL,
  `status` enum('new','activated','completed','rejected','cancelled') NOT NULL DEFAULT 'new',
  `rejection_reason` text CHARACTER SET latin1 COLLATE latin1_general_cs,
  PRIMARY KEY (`id`),
  KEY `user` (`user_id`),
  KEY `timestamp` (`timestamp`),
  KEY `status` (`status`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
