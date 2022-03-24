<?php
declare(strict_types = 1);
namespace Elasticsearch;
use Elasticsearch\ClientBuilder;
/**
 * elasticsearch封装类
 * @author caoxu
 * @date 2022-03-14
 */
class ElasticSearch
{
    public $config;
    public $api;
    public $index_name;
    public $index_type;
    public $hosts = ['http://elastic:F39AC7e2d0@code1:9200']; // 端口号设置
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

    /**
     * 初始化索引参数
     * @author joniding
     * @return array
     */
    public function initParams()
    {
        return [
            'index' => $this->index_name,
            'type'  => $this->index_type,
        ];
    }

    /**
     * 但条件查询
     * @param $key 参数名称
     * @param $value 搜索的内容
     * @author joniding
     */
    public function where($key, $value)
    {
        $this->query["condition"]['bool']['must']["$key"] = $value;
    }

    /**
     * 多条件查询
     * @param $key 参数名称
     * @param $value 搜索的内容
     * @author joniding
     */
    public function whereArr($whereArr)
    {
        if (!empty($where) && is_array($where)) {
            foreach ($whereArr as $key => $value) {
                $this->query["condition"]['bool']['must']["$key"] = $value;
            }
        }
    }

    /**
     * 范围查询
     * @param  $param 参数名称
     * @param  $between [1,2]
     * @author joniding
     */
    public function whereBetween($param, $between)
    {
        if (!empty($param) && is_array($between)) {
            $this->query["condition"]['bool']['must']["$param"]["between"] = $between;
        }
    }

    /**
     * wherein
     * @param  $param 参数名称
     * @param  $in [1,2]
     * @author joniding
     */
    public function whereIn($param, $in){
        if (!empty($param) && is_array($in)) {
            $this->query["condition"]['bool']['must']["$param"]["in"] = $in;
        }
    }

    /**
     * notin
     * @param  $param 参数名称
     * @param  $in [1,2]
     * @author joniding
     */
    public function whereNotIN($param, $notIn){
        if (!empty($param) && is_array($notIn)) {
            $this->query["condition"]['bool']['must']["$param"]["notIn"] = $notIn;
        }
    }

    /**
     * 分组
     * @param  $param 参数名称
     * @author joniding
     */
    public function groupBy($param)
    {
        if (!empty($param) && isset($param)) {
            $this->query["agg"]["terms"] = $param;
        }

    }

    /**
     * 聚合
     * @param  $field 聚合参数
     * @param  $alias 别名
     * @param  $polymerization 聚合条件  count,sum,max
     * @author joniding
     */
    public  function sum($field, $alias, $polymerization){
        if (!empty($field) && !empty($alias) && !empty($polymerization)) {
            $this->query["agg"]['field']["$alias"]["field"] = $field;
            $this->query['agg']["alias"]["$alias"]["alias"] = $alias;
            $this->query["agg"]["polymerization"]["$alias"]["polymerization"]  = $polymerization;
        }
    }


    /**
     * 排序
     * @param  $field 排序参数
     * @param  $sort_rule 排序方式   // 排序方式 asc(升序)  desc(降序)/默认
     * @author joniding
     */
    public function orderBy($sort_field, $sort_rule)
    {
        if (!empty($sort_field) && !empty($sort_rule)) {
            $this->query["sort_field"] = $sort_field;
            $this->query["sort_rule"]  = $sort_rule; // 排序方式 asc(升序)  desc(降序)/默认
        }
    }

    /**
     * 分组排序
     * @param  $field 排序参数
     * @param  $sort_rule 排序方式   // 排序方式 asc(升序)  desc(降序)/默认
     * @author joniding
     */
    public function groupOrder($sort_field, $sort_rule)
    {
        if (!empty($sort_field) && !empty($sort_field)) {
            $this->query['agg']['order'][$sort_field] = $sort_rule;
        }

    }

    /**
     * 分页
     * @param  $page 从第几页开始
     * @param  $page_size 每页显示
     * @author joniding
     */
    public function limit($page, $page_size)
    {
        if (!empty($page_size)) {
            $this->query["page"] = $page;
            $this->query["page_size"] = $page_size;
        }
    }

    /**
     * 要查出来的信息
     * @param
     * @author joniding
     */
    public function select($param){
        if (!empty($param)) {
            $this->query["_source"]["include"] = $param;
        }
    }

    // 查询总数量
    public  function count($sql){

        if (!empty($sql)) {
            $res = [];
            try {
                $res = $this->api->msearch(["query" => $sql],$this->hosts);
            }catch (\Throwable $e){
                $e->getMessage();
            }

            $log  = 0;
            if (!empty($res['rows']) && !empty($res['columns'])) {
                $log = $res["rows"][0][0];
            }
            return $log;
        }


//        $data = $this->query;
//        try{
//            if (!is_array($data)){
//                return [];
//            }
//            $query= [];
//            $params = $this->initParams();
//            $params['size'] = 0;
//            /**
//             * 条件组合过滤，筛选条件
//             */
//            if (array_key_exists('condition',$data)){
//                $condition = $data['condition'];
//                if (array_key_exists('bool',$condition)){
//                    //必须满足
//                    if (array_key_exists('must',$condition['bool'])){
//                        foreach ($condition['bool']['must'] as $key => $val){
//                            if (is_array($val)){
//                                if (!empty($val["between"])) {
//                                    $query['bool']['must'][]['range'] = [
//                                        $key => [
//                                            'gte'  => $val["between"][0],
//                                            'lte'  => $val["between"][1]
//                                        ]
//                                    ];
//                                } else if (!empty($val["in"])) {
//                                    $query['bool']['filter'][]['terms'] = [
//                                        $key => array_values($val["in"])
//                                    ];
//
//                                } else if (!empty($val["notIn"])) {
//                                    $query['bool']['filter'][]["bool"]["must_not"]['terms'] = [
//                                        $key => array_values($val["notIn"])
//                                    ];
//                                }
//
//                            }else{
//                                $query['bool']['must'][]['match'] = [
//                                    $key => $val
//                                ];
//                            }
//                        }
//                        $params['body']['query'] = $query;
//                    }
//                }
//            }
//
//            $return = $this->api->search($params);
//        }catch (\Exception $e){
//            throw $e;
//        }
//        $res = 0;
//        if(!empty($return) && !empty($return["hits"]["total"]["value"])) {
//            $res = $return["hits"]["total"]["value"];
//        }
//        return $res;
    }








    // sql 查询
    public function sql($sql){
        if (!empty($sql)) {
            $res = [];
            try {
                $res = $this->api->msearch(["query" => $sql],$this->hosts);
            }catch (\Throwable $e){
                return $e->getMessage();
            }

            $log  = [];
            if (!empty($res['rows']) && !empty($res['columns'])) {
                foreach ($res['rows'] as $k => $v){
                    foreach ($v as $key=> $value) {
                        $log[$k][$res['columns'][$key]["name"]] = $value;
                    }
                }
            }
            return $log;
        }
    }

    /**
     *  单表搜索， 不支持聚合
     * @author joniding
     * @param $data
     * $data['condition'] 条件组合
     * $data['page_size'] 每页显示数量
     * $data['page'] 从第几条开始
     * $data['es_sort_field'] 自定义排序字段
     * @return array|bool
     * @throws \Exception
     */
    public function searchMulti()
    {
        $data = $this->query;
        try{
            if (!is_array($data)){
                return [];
            }
            $params = $this->initParams();
            if (array_key_exists('fields',$data)){
                $params['_source'] = $data['fields'];
            }

            //分页
            if (array_key_exists('page_size',$data)){
                $params['size'] = !empty($data['page_size'])?$data['page_size']:1;

                //前端页码默认传1
                $params['from'] = !empty($data['page'])?($data['page']-1)*$params['size']:0;
                unset($data['page_size'],$data['page']);
            }
            //排序
            if (array_key_exists('sort_field',$data)){

                $sort_file = !empty($data['sort_field'])?$data['sort_field']:'total_favorited';
                $sort_rule = !empty($data['sort_rule'])?$data['sort_rule']:'desc';
                $params['body']['sort'][] = [
                    ''.$sort_file.'' => [
                        'order' => ''.$sort_rule.'',
                    ]
                ];
                unset($data['sort_field'],$data['sort_rule']);
            }else{
//                $params['body']['sort'][] = [
//                    'created_at' => [
//                        'order' => 'desc',
//                    ]
//                ];
            }
            /**
             * 深度（滚动）分页
             */
            if (array_key_exists('scroll',$data)){
                $params['scroll'] = $data['scroll'];
            }

            //条件组合
            if (array_key_exists('condition',$data)){
                $query     = [];
                $condition = $data['condition'];

                /**
                 * 组合查询
                 */
                if (array_key_exists('bool',$condition)){
                    //必须满足
                    if (array_key_exists('must',$condition['bool'])){
                        foreach ($condition['bool']['must'] as $key => $val){
                            if (is_array($val)){
                                if (!empty($val["between"])) {
                                    $query['bool']['must'][]['range'] = [
                                        $key => [
                                            'gte'  => $val["between"][0],
                                            'lte'  => $val["between"][1]
                                        ]
                                    ];
                                } else if (!empty($val["in"])) {
                                    $query['bool']['filter'][]['terms'] = [
                                        $key => array_values($val["in"])
                                    ];

                                } else if (!empty($val["notIn"])) {
                                    $query['bool']['filter'][]["bool"]["must_not"]['terms'] = [
                                        $key => array_values($val["notIn"])
                                    ];
                                }
                            }else{
                                $query['bool']['must'][]['match'] = [
                                    $key => $val
                                ];
                            }
                        }
                    }
                }
                !empty($query) && $params['body']['query'] = $query;
            }

            // 要查询出来的数据
            if (!empty($this->query["_source"]["include"])) {
                $params['body']["_source"]["include"] = $this->query["_source"]["include"];
            }

            $return = $this->api->search($params);
        }catch (\Throwable $e){
            $e->getMessage();
        }

        //var_dump(json_encode($params));exit;

        $res = [];
        if(!empty($return) && !empty($return["hits"]["hits"])) {
            foreach ($return["hits"]["hits"] as $k => $v) {
                $res[$k] = $v["_source"];
            }
        }

        $this->query = [];
        return $res;
    }



    /**
     * 聚合统计,方差
     * @param $data
     * @return array
     * @throws \Exception
     * @author:caoxu
     * @date:2022-03-14
     */
    public function sumSelect()
    {
        $data = $this->query;
        try{
            if (!is_array($data)){
                return [];
            }
            $query= [];
            $params = $this->initParams();
            if (!empty($this->query["agg"])) {
                $params['size'] = 0;
            } else {
                $params['size'] = 500;
            }

            /**
             * 条件组合过滤，筛选条件
             */
            if (array_key_exists('condition',$data)){
                $condition = $data['condition'];
                if (array_key_exists('bool',$condition)){
                    //必须满足
                    if (array_key_exists('must',$condition['bool'])){
                        foreach ($condition['bool']['must'] as $key => $val){
                            if (is_array($val)){
                                if (!empty($val["between"])) {
                                    $query['bool']['must'][]['range'] = [
                                        $key => [
                                            'gte'  => $val["between"][0],
                                            'lte'  => $val["between"][1]
                                        ]
                                    ];
                                } else if (!empty($val["in"])) {
                                    $query['bool']['filter'][]['terms'] = [
                                        $key => array_values($val["in"])
                                    ];

                                } else if (!empty($val["notIn"])) {
                                    $query['bool']['filter'][]["bool"]["must_not"]['terms'] = [
                                        $key => array_values($val["notIn"])
                                    ];
                                }

                            }else{
                                $query['bool']['must'][]['match'] = [
                                    $key => $val
                                ];
                            }
                        }
                        $params['body']['query'] = $query;
                    }
                }
            }


            //分组、排序设置
            if (array_key_exists('agg',$data)){
                $agg = [];
                //字段值
                if (array_key_exists('terms',$data['agg'])){
                    $agg['_result']['terms'] = [
                        'field'   => $data['agg']['terms'],
                        'size'    => 500,
                    ];
                    if (array_key_exists('order',$data['agg'])){
                        foreach ($data['agg']['order'] as $key => $val){
                            $fields = $key;
                            $agg['_result']['terms']['order'] = [
                                $fields => $val
                            ];
                            unset($fields);
                        }
                    }
                }

                if (!empty($data["agg"]["terms"])) {

                    //聚合桶
                    if (array_key_exists('field',$data['agg'])){
                        foreach ($data['agg']['alias'] as $key => $val) {
                            $agg['_result']['aggs'][$key] = [
                                $data['agg']['polymerization']["$key"]['polymerization']=> [
                                    'field'  => $data['agg']['field']["$key"]['field']
                                ]
                            ];
                        }
                    }

                } else {

                    foreach ($data['agg']['alias'] as $key => $val) {
                        $agg[$key] = [
                            $data['agg']['polymerization']["$key"]['polymerization']=> [
                                'field'  => $data['agg']['field']["$key"]['field']
                            ]
                        ];
                    }

                }

                //日期聚合统计
                if (array_key_exists('date',$data['agg'])){
                    $date_agg = $data['agg']['date'];
                    //根据日期分组
                    if (array_key_exists('field',$date_agg)){
                        $agg['result'] = [
                            'date_histogram' => [
                                'field'     => $data['agg']['date']['field'],
                                'interval'  => '2h',
                                'format'    => 'yyyy-MM-dd  HH:mm:ss'
                            ]
                        ];
                    }

                    if (array_key_exists('agg',$date_agg)){
                        //分组
                        if (array_key_exists('terms',$date_agg['agg'])){
                            $agg['result']['aggs']['result']['terms'] = [
                                'field' => $date_agg['agg']['terms'],
                                'size'  => 100,
                            ];
                        }
                        //统计最大、最小值等
                        if (array_key_exists('stats',$date_agg['agg'])){
                            $agg['result']['aggs']['result']['aggs'] = [
                                'result_stats' => [
                                    'extended_stats' => [
                                        'field' => $date_agg['agg']['stats']
                                    ]
                                ]
                            ];
                        }
                    }

                }

                // 要查询出来的数据
                if (!empty($this->query["_source"]["include"])) {
                    $params['body']["_source"]["include"] = $this->query["_source"]["include"];
                }
                $params['body']['aggs'] = $agg;
            }
            unset($this->query);
            $return = $this->api->search($params);
        }catch (\Exception $e){
            echo $e;exit;
            throw $e;
        }

        $res = [];
        if(!empty($return) && !empty($return["aggregations"]["_result"])) {
            $res = $return["aggregations"]["_result"]["buckets"];
        } else if(!empty($return) && !empty($return["aggregations"])){
            $res = $return["aggregations"];
        }
        return $res;
    }








    /**
     * 创建一个索引
     * @author joniding
     * @param $settings
     * @return array
     * @throws \Throwable
     */
    public function createIndex($settings = [])
    {
        try{
            $initParams['index'] = $this->index_name;
            !empty($settings) && $initParams['body']['settings'] = $settings;
            $res = $this->api->indices()->create($initParams);
        }catch(Throwable $e){
            return $this->error($e->getMessage());
        }
        return $res;
    }

    /**
     * 更新索引的映射 mapping
     * @author joniding
     * @param $data
     * @return array
     * @throws \Exception
     */
    public function setMapping($data)
    {
        try{
            $initParams = $this->initParams();
            $initParams['body'] = $data;
            $res = $this->api->indices()->putMapping($initParams);
        }catch (\Exception $e){
            throw $e;
        }
        return $res;
    }

    /**
     * 获取索引映射 mapping
     * @author joniding
     * @return array
     * @throws \Exception
     */
    public function getMapping()
    {
        try{
            $initParams = $this->initParams();
            $res = $this->api->indices()->getMapping($initParams);
        }catch (Throwable $e){
            throw $e;
        }

        return $res;
    }



    /**
     * 向索引中插入数据
     * @author joniding
     * @param $data
     * @return bool
     * @throws \Exception
     */
    public function add($data)
    {
        try{
            $params = $this->initParams();
            isset($data['id']) && $params['id'] = $data['id'];
            $params['body'] = $data['body'];
            $res = $this->api->index($params);
        }catch (\Throwable $e){
            return $this->error($e->getMessage());
        }
        if (!isset($res['_shards']['successful']) || !$res['_shards']['successful']){
            return false;
        }
        return true;
    }


    /**
     * 单字段模糊查询
     * 满足单个字段查询（不带分页+排序）match 分词查询
     * @author joniding
     * @param $data
     * @return array
     * @throws \Exception
     */
    public function search($data = [])
    {
        try{
            $params = $this->initParams();

            if (!empty($data)){
                $field = key($data);
                $query = [
                    'match' => [
                        $field => [
                            'query' => $data[$field],
                            'minimum_should_match'  => '90%'  //相似度，匹配度
                        ]
                    ]
                ];
                $params['body']['query']        = $query;
            }
            $res = $this->api->search($params);

        }catch (\Exception $e){
            throw $e;
        }
        return $res;
    }


    /**
     * 批量插入数据
     * @author joniding
     * @param $data
     * @return array
     * @throws \Exception
     */
    public function bulk($data)
    {
        try{
            if (empty($data['body'])) return false;
            $params = $this->initParams();
            $params['body'] = $data['body'];

            $res = $this->api->bulk($params);

        }catch (Throwable $e){
            throw $e;
        }
        return $res;
    }


    /**
     * 检测文档是否存在
     * @param $id
     * @return array|bool
     * @throws \Exception
     */
    public function IndexExists($id = "")
    {
        try{
            $params = $this->initParams();
            $params['id'] = $id;
            $res = $this->api->exists($params);
        }catch (Throwable $e){
            throw $e;
        }
        return $res;
    }


    /**
     * 根据唯一id查询数据
     * @author joniding
     * @param $id
     * @return array
     * @throws \Exception
     */
    public function searchById($id)
    {
        try{
            $params = $this->initParams();
            $params['id'] = $id;

            $res = $this->api->get($params);
        }catch (\Exception $e){
            throw $e;
        }
        return $res;
    }

    /**
     * 查询索引是否存在
     * @return array|bool
     * @throws \Exception
     */
    public function exist()
    {
        try{
            $params['index'] = $this->index_name;

            $res = $this->api->indices()->exists($params);

        }catch (\Exception $e){
            throw $e;
        }
        return $res;
    }


    /**
     * 根据唯一id删除
     * @author joniding
     * @param $id
     * @return bool
     * @throws \Exception
     */
    public function delete($id = '')
    {
        try{
            $params       = $this->initParams();
            $params['id'] = $id;

            $res = $this->api->delete($params);
        }catch (\Exception $e){
            throw $e;
        }
        if (!isset($res['_shards']['successful'])){
            return false;
        }
        return true;
    }

    /**
     * 批量查询，只能根据id来查
     * @param $data
     * @return array
     * @throws \Exception
     * @author:caoxu
     * @date:2022-03-14
     */
    public function mGet($data)
    {
        try{
            if (!is_array($data)) return [];
            //初始化索引
            $params = $this->initParams();

            if (array_key_exists('fields',$data)){
                $query['ids'] = $data['fields'];
                $params['body'] = $query;
            }
            $res = $this->api->mget($params);
            return $res;

        }catch (Throwable $e){
            throw $e;
        }
    }

    /**
     * 深度分页
     * @param $data
     * @return array
     * @throws Throwable
     * @author:caoxu
     * @date:2022-03-14
     */
    public function scroll($data)
    {
        try{
            $params = [
                'scroll_id' => $data['scroll_id'],
                'scroll'    => '1m'
            ];

            $res = $this->api->scroll($params);
//            \Log::info(json_encode($params));

            if (isset($res['_scroll_id']) && $res['_scroll_id'] != $data['scroll_id']){
                $this->api->clearScroll(['scroll_id' => $data['scroll_id'] ]);
            }

            return $res;
        }catch(Throwable $e){
            throw $e;
        }
    }



}

