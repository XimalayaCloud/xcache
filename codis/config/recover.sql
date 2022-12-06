CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(255) NOT NULL,
  `password` varchar(255) NOT NULL,
  `usertype` int(11) NOT NULL,
  `comment` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `codis` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `codisname` varchar(255) NOT NULL,
  `dashboard` varchar(255) NOT NULL,
  `comment` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `codisname` (`codisname`),
  UNIQUE KEY `dashboard` (`dashboard`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `users` WRITE;
INSERT INTO `users` VALUES (1,'admin','31989864a3c1bffd27ff19cb400a3f38',1,'super admin');
UNLOCK TABLES;

CREATE TABLE `user_codis` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `userid` int(11) NOT NULL,
  `codisid` int(11) NOT NULL,
  `iswrite` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `userid` (`userid`,`codisid`),
  KEY `codisid` (`codisid`),
  FOREIGN KEY (`userid`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (`codisid`) REFERENCES `codis` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `codis_dashboard`;
CREATE TABLE `codis_dashboard` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `product_name` varchar(255) NOT NULL COMMENT 'codis名称',
  `node_type`  varchar(255) NOT NULL COMMENT 'node type',
  `node_name`  varchar(255) NOT NULL COMMENT 'node',
  `node_value` varchar(4096)  COMMENT 'node value',
  PRIMARY KEY (`id`),
  UNIQUE KEY (`product_name`, `node_type`, `node_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='codis集群在ZK中对应的节点信息';
