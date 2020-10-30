CREATE TABLE `deletion_request_datasets` (
  `request_id` int(10) unsigned NOT NULL,
  `dataset_id` int(10) unsigned NOT NULL,
  KEY `request` (`request_id`),
  KEY `dataset` (`dataset_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
