CREATE TABLE `active_deletions` (
  `request_id` int(10) unsigned NOT NULL,
  `item` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `site` varchar(32) NOT NULL,
  `timestamp` datetime NOT NULL,
  `status` enum('new','queued') NOT NULL DEFAULT 'new',
  `created` datetime NOT NULL,
  `updated` datetime DEFAULT NULL,
  KEY `request` (`request_id`),
  KEY `item` (`item`),
  KEY `site` (`site`),
  KEY `timestamp` (`timestamp`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
