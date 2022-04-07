elasticsearch-php封装
=================


 
Version Matrix
| Elasticsearch-PHP Branch | PHP Version |
| ----------- | ------------------------ |
| >= 7.16.0, < 8.0.0  | >= 7.3.0, <= 8.1.99 |
| >= 7.12.0, < 8.0.0  | >= 7.3.0, <= 8.0.99 |
| >= 7.11.0, < 8.0.0  | >= 7.1.0, <= 8.0.99 |
| >= 7.0.0,  < 7.11.0 | >= 7.1.0, < 8.0.0 |






首先下载composer包，包中已部署 "elasticsearch/elasticsearch": "^7.1"无需再下载
--------
 ```
 composer require caoxu/encap-elastic  


 ```



安装完成后，修改es链接信息
--------
 ```
<?php

class ElasticSearch
 {     
         public $config;
         public $api;
         public $index_name;
         public $index_type;
         public $hosts = ['http://user:pass@127.0.0.1:9200']; // es端口号设置
         public $query =  [];
         public $sql = [];

         public function __construct($index_name = "", $index_type ="")
         {
              try{
              //构建客户端对象
              $this->api = ClientBuilder::create()->setHosts($this->hosts)->build();
              $this->index_name = $index_name;
              $this->index_type = $index_type;
         }catch (\Throwable $e){
              return $e->getMessage();
         }
}
          
 ```



PHP引入
 --------
 ```
<?php
use Elasticsearch\ElasticSearch;


 #原声sql的使用
 $query = new ElasticSearch();
 $res = $query->sql("SELECT 
                        SUM(count) as total,
                        SUM(case when status = 1 then count else 0 end) as success_count,
                        SUM(case when status = 2 then count else 0 end) as error_count,
                        route.keyword  as route
                      FROM hydra_access_count_logs 
                             where $where group by route.keyword "
 );




#封装的使用方法，不适用于复杂查询
$query = new ElasticSearch("hydra_access_logs", "_doc");
$query->select(["account_id","client_ip","status","route"]);
$query->whereArr(["id"=>"1"]);
$query->whereNotIN("id",[1,2,3])
$query->whereIn("id",$id);
$query->where("account_id", $account_id);
$query->where("route", $route);
$query->where("status", $status);
$query->whereBetween("create_stamp", [$start_date,$end_date]);
$query->orderBy("create_stamp", "desc");
$query->limit(0, 10000);
$res   =  $query->searchMulti();



#查询总数量
$query = new ElasticSearch();
$count= $query->count(" select coun(*) as count from hydra_access_count_logs where $where ");



#添加
$api = new ElasticSearch('hydra_access_count_logs', '_doc');
$list = DB::table("hydra.access_count_logs")
        ->whereBetween('create_time', ["2022-03-20", "2022-03-20 23:59:59"])
        ->orderBy("id","desc")->get()->map(function ($value) {
            return (array)$value;
        })->toArray();
 
foreach ($list as $v) {
  $data = [];
  $data["body"] = $v;
  $data["body"]["platform"] = "ssssss";
  $data["body"]["platform_id"] = 1;
  $data["body"]["nick"] = "123344455".rand(0,100);
  $data["body"]["create_stamp"] = strtotime($v["created_at"]);
  $return = $api->add($data);
  echo $return;
}



```





es-kibana常用命令
 --------
 
```
#查询
GET hydra_access_count_logs8/_search
{
  "query": {
    "match_all": {}
  }
}

#查看
GET hydra_access_logs/_count
{
  "query": {
    "match_all": {}
  }
} 


#常见索引
PUT  hydra_access_count_logs


#默认分页只允许查出10000条数据，设置无上限
PUT hydra_access_count_logs/_settings
{
  "index.max_result_window":100000000
}

PUT hydra_access_logs/_settings
{
  "index.max_result_window":100000000
}



GET _sql
{
  "query": """
        SELECT * FROM "hydra_access_logs1"  """
}


#使用sql
GET _sql
{
  "query": """
        select 
          YEAR(created_at) AS Y,MONTH_OF_YEAR(created_at) AS M, DAY_OF_MONTH(created_at) AS D,HOUR_OF_DAY(created_at) AS H
       FROM hydra_access_count_logs group by Y,M,D,H  """
}




GET _sql
{
  "query": """
        select 
         *
       FROM hydra_access_count_logs   """
}


GET _sql
{
  "query": """
       select 
         SUM(count) as total,
            SUM(case when status = 1 then count else 0 end) as success_count,
            SUM(case when status = 2 then count else 0 end) as error_count,
            YEAR(created_at) AS Y,MONTH_OF_YEAR(created_at) AS M, DAY_OF_MONTH(created_at) AS D
        FROM hydra_access_count_logs 
            where  create_stamp >= 1647705600  and  create_stamp <= 1647878399  group by Y,M,D """
}



GET _sql
{
  "query": """
        select SUM(count) as total, 
        SUM(case when status = 1 then count else 0 end) as success_count,
        SUM(case when status = 2 then count else 0 end) as error_count, 
        max(id) as id,
        account_id
        FROM hydra_access_count_logs where create_stamp >= 1647705600 and create_stamp <= 1647791999 group by account_id
       """
}




#手动创建索引
PUT  hydra_access_logs
{
    "mappings": {
    "properties": {
        "account_id": {
            "type": "long"
         },
         "headers": {
            "type": "text"
         },
         "hydra_path": {
            "type": "text"
         },
         "enterprise_id": {
            "type": "long"
         },
         "method": {
            "type": "text"
         },
         "route": {
            "type": "text"
         },
         "client_ip": {
            "type": "text"
         },
         "params": {
            "type": "text"
         },
         "response": {
            "type": "text"
         },
         "user_agent": {
            "type": "text"
         },
         "time": {
            "type": "text"
          },
          "created_at": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss"
          },
          "cookie_id": {
            "type": "long"
          },
          "updated_at": {
            "type": "date",
             "format": "yyyy-MM-dd HH:mm:ss"
          },
          "log_file_id": {
            "type": "text"
          },
          "status": {
            "type": "long"
          },
          "create_stamp": {
            "type": "long"
          }
          
}
    }

}



#修改表字段
PUT hydra_access_count_logs
{
  "mappings": {
      "properties": {
        "created_at": {
          "type":   "date",
          "format": "yyyy-MM-dd HH:mm:ss"
        }
    
    }
  }
}


#数据同步
POST _reindex                   
{
  "source": {
    "index": "hydra_access_count_logs1"
  },
  "dest": {
    "index": "hydra_access_count_logs"
  }
}

#删除源索引
DELETE hydra_access_count_logs1


#设置别名
POST /_aliases
{
        "actions": [
            {"add": {"index": "hydra_access_count_logs1", "alias": "hydra_access_count_logs7"}}
        ]
}

#删除别名
POST /_aliases
{
    "actions": [
        {"remove": {"index": "hydra_access_count_logs1", "alias": "hydra_access_count_logs"}}
    ]
}

```



















