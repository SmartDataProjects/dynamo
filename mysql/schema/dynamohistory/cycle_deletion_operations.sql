CREATE TABLE `cycle_deletion_operations` (
  `cycle_id` int(10) NOT NULL,
  `operation_id` int(10) NOT NULL,
  UNIQUE KEY `cycleop` (`cycle_id`,`operation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
