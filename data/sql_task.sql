# ************************************************************
# Sequel Pro SQL dump
# Version 5444
#
# https://www.sequelpro.com/
# https://github.com/sequelpro/sequelpro
#
# Host: 127.0.0.1 (MySQL 5.7.10-log)
# Database: z_pe
# Generation Time: 2021-07-14 07:30:39 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
SET NAMES utf8mb4;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table sql_tasks
# ------------------------------------------------------------

DROP TABLE IF EXISTS `sql_tasks`;

CREATE TABLE `sql_tasks` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '统计任务id',
  `from_app` varchar(50) DEFAULT NULL COMMENT '统计来源业务系统',
  `from_db_type` varchar(50) DEFAULT NULL COMMENT '读取的数据源类型',
  `from_db` varchar(20) DEFAULT NULL COMMENT '来自数据库',
  `to_app` varchar(50) DEFAULT NULL COMMENT '写入的业务系统',
  `to_db_type` varchar(20) DEFAULT NULL COMMENT '写入数据源类型',
  `to_db` varchar(20) DEFAULT NULL COMMENT '写入数据库',
  `to_table` varchar(255) DEFAULT NULL COMMENT '写入数据表',
  `static_rule` text COMMENT '统计规则',
  `params` text COMMENT '增量规则',
  `online_status` int(11) DEFAULT NULL COMMENT '统计状态0统计1不统计',
  `write_mode` varchar(50) DEFAULT NULL COMMENT '写入模式replace/insert/append/overwrite',
  `task_desc` text COMMENT '统计描述',
  `is_truncate` int(11) DEFAULT '1' COMMENT '是否truncate表(0-truncate)',
  `owner` varchar(100) DEFAULT NULL COMMENT '取数人',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `task_status` varchar(100) DEFAULT NULL COMMENT '任务状态',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='sql任务';

LOCK TABLES `sql_tasks` WRITE;
/*!40000 ALTER TABLE `sql_tasks` DISABLE KEYS */;

INSERT INTO `sql_tasks` (`id`, `from_app`, `from_db_type`, `from_db`, `to_app`, `to_db_type`, `to_db`, `to_table`, `static_rule`, `params`, `online_status`, `write_mode`, `task_desc`, `is_truncate`, `owner`, `create_time`, `task_status`, `update_time`)
VALUES
	(1,'local_dw','mysql','z_pe',NULL,NULL,NULL,NULL,'replace into test.orders(order_id,order_time,order_amount)\nSELECT concat(\"20210713\",FLOOR(RAND() * 10000),\"768621\") as order_id,now() as order_time,\nFLOOR(RAND() * 10000) as order_amount','{\"split\":{\"table\":\"test.userinfo\",\"pk\":\"id\",\"worker_num\":20,\"read_batch\":30,\"write_batch\":300}}',0,'insert','replace demo测试',0,'18811788263','2021-03-30 16:13:34','executed_success','2021-07-14 15:29:01'),
	(2,'local_dw','mysql','z_pe','local_dw','mysql','z_pe','temp_static_by_time_increase','select substr(order_time,1,10) as order_date,count(order_id) as orders,sum(order_amount) as order_amount\nfrom test.orders\nwhere order_time>=\"$1\" and order_time < \"$2\"\ngroup by substr(order_time,1,10)\n','{\"time_increase\":{\"$1\":\"$today\",\"$2\":\"$today+2\"}}',0,'insert','增量时间配置demo',0,'18811788263','2021-03-30 16:13:34','executed_success','2021-07-14 15:29:34'),
	(3,'local_dw','mysql','z_pe','local_dw','mysql','z_pe','temp_static_by_pool','select $start as start,$end as end,num\nfrom (\n	select count(1) as num\n	from $table \n        where $pk>$start and $pk<=$end \n) as a\n','{\"split\":{\"table\":\"test.userinfo\",\"pk\":\"id\",\"worker_num\":20,\"read_batch\":20000,\"write_batch\":300}}',0,'insert','大表切分demo',0,'18811788263','2021-03-30 16:13:34','executed_success','2021-07-14 15:29:24');

/*!40000 ALTER TABLE `sql_tasks` ENABLE KEYS */;
UNLOCK TABLES;



/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
