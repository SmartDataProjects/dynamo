DROP TABLE IF EXISTS `dataset_accesses`;

CREATE TABLE `dataset_accesses` (
  `dataset_id` int(10) unsigned NOT NULL,
  `site_id` int(10) unsigned NOT NULL,
  `date` date NOT NULL DEFAULT '0000-00-00',
  `access_type` enum('local','remote') CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'local',
  `num_accesses` int(11) NOT NULL DEFAULT '0',
  `cputime` float NOT NULL DEFAULT '0',
  PRIMARY KEY (`dataset_id`,`site_id`,`date`),
  KEY `sites` (`site_id`),
  KEY `dates` (`date`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
