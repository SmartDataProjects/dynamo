CREATE TABLE `deletion_policies` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `hash` binary(16) NOT NULL,
  `text` text,
  PRIMARY KEY (`id`),
  KEY `hash` (`hash`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
