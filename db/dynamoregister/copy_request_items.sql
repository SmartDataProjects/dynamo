DROP TABLE IF EXISTS `copy_request_items`;

CREATE TABLE `copy_request_items` (
  `request_id` int(10) unsigned NOT NULL,
  `item` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  KEY `request` (`request_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
