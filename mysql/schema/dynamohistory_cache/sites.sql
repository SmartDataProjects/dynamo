CREATE TABLE `sites` (
  `site_id` int(10) unsigned NOT NULL,
  `status` enum('ready','waitroom','morgue','unknown') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `quota` int(10) NOT NULL,
  PRIMARY KEY (`site_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
