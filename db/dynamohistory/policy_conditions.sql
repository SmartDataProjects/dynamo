DROP TABLE IF EXISTS `policy_conditions`;

CREATE TABLE `policy_conditions` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `text` varchar(512) COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `text` (`text`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
