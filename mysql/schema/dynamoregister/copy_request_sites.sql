CREATE TABLE `copy_request_sites` (
  `request_id` int(10) unsigned NOT NULL,
  `site` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  KEY `request` (`request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
