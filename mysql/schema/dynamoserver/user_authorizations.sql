CREATE TABLE `user_authorizations` (
  `user_id` int(10) unsigned NOT NULL,
  `role_id` int(10) unsigned NOT NULL DEFAULT 0,
  `target` enum('inventory','history','registry','application') COLLATE latin1_general_cs NOT NULL,
  UNIQUE KEY `auth` (`user_id`,`role_id`,`target`),
  KEY `user` (`user_id`,`role_id`),
  KEY `target` (`target`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
