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

DELIMITER //
CREATE  TRIGGER tgr_codis_insert AFTER INSERT
ON codis FOR EACH ROW
BEGIN
INSERT INTO user_codis(userid, codisid, iswrite) SELECT users.id, codis.id, 1 FROM users,  codis WHERE users.usertype = 1 AND codis.codisname = new.codisname;
END;
CREATE  TRIGGER tgr_users_insert AFTER INSERT
ON users FOR EACH ROW
BEGIN
INSERT INTO user_codis(userid, codisid, iswrite) SELECT users.id, codis.id, 1 FROM users,  codis WHERE new.usertype = 1 AND users.username = new.username;
END;
//
COMMIT;
