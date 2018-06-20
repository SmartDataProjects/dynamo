CREATE TABLE `deletion_request_blocks` (
  `request_id` int(10) unsigned NOT NULL,
  `block_id` bigint(20) unsigned NOT NULL,
  KEY `request` (`request_id`),
  KEY `block` (`block_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
