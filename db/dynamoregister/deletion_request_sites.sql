DROP TABLE IF EXISTS `deletion_request_sites`;

CREATE TABLE `deletion_request_sites` (
  `request_id` int(10) unsigned NOT NULL,
  `site` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  KEY `request` (`request_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
