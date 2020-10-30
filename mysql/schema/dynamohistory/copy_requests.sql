CREATE TABLE `copy_requests` (
  `id` int(10) unsigned NOT NULL,
  `group_id` varchar(32) NOT NULL,
  `num_copies` tinyint(1) unsigned NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `request_time` datetime NOT NULL,
  `status` enum('new','activated','completed','rejected','cancelled') NOT NULL DEFAULT 'new',
  `rejection_reason` text CHARACTER SET latin1 COLLATE latin1_general_cs,
  PRIMARY KEY (`id`),
  KEY `user` (`user_id`),
  KEY `request_time` (`request_time`),
  KEY `status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
