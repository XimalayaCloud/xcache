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
