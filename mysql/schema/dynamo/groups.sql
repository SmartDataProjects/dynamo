CREATE TABLE `groups` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(64) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `olevel` enum('Dataset','Block') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'Block',
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 CHECKSUM=1;
