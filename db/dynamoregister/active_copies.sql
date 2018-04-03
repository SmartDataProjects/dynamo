CREATE TABLE `active_copies` (
  `request_id` int(10) unsigned NOT NULL,
  `item` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `site` varchar(32) NOT NULL,
  `status` enum('new','queued','failed','completed') NOT NULL DEFAULT 'new',
  `created` datetime NOT NULL,
  `updated` datetime DEFAULT NULL,
  KEY `request` (`request_id`),
  KEY `item` (`item`),
  KEY `site` (`site`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
