CREATE TABLE `lock` (
  `lock_host` varchar(256) NOT NULL DEFAULT '',
  `lock_process` int(11) NOT NULL DEFAULT '0',
  UNIQUE KEY `host` (`lock_host`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
