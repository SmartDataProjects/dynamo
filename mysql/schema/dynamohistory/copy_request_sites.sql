CREATE TABLE `copy_request_sites` (
  `request_id` int(10) unsigned NOT NULL,
  `site_id` int(10) unsigned NOT NULL,
  KEY `request` (`request_id`)
  KEY `site` (`site_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
