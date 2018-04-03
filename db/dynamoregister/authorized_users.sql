DROP TABLE IF EXISTS `authorized_users`;

CREATE TABLE `authorized_users` (
  `user_id` int(10) unsigned NOT NULL,
  `service_id` int(10) unsigned NOT NULL,
  UNIQUE KEY `user` (`user_id`,`service_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
