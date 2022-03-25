<?php
/*
 * Copyright © 2017-2020 Linkworld Performance Marketing or its subsidiaries. All rights reserved.
 */

namespace Controller\Moon;

use App\Model\Account\Session;
use App\Model\Customer\Shop;
use App\Model\Moon\CountLog;
use App\Model\Sakura\Advertiser;
use App\Models\Xiaohong\Account;
use Capsule;
use Controller\MoonController;
use Database;
use DB;
use Elasticsearch\ElasticSearch;
//use C\Push\ElasticSearch;

class TopCrawlerController extends MoonController
{
    public $index_name = "hydra"; // es存储的库名
    public $index_type = "access_logs"; // es存储的表名

    /**
     * 获取爬虫统计列表 （完）
     * @return mixed|string [type] [description]
     */
    public function getCrawlerList()
    {
        $start_date = $this->param('start_date');
        $end_date = $this->param('end_date');
        $route = $this->param('route');
        $account_id = $this->param('account_id');

        if (empty($start_date) || empty($end_date)) {
            return $this->error('参数异常');
        }

        $start_date = strtotime($start_date);
        $end_date  .= ' 23:59:59';
        $end_date   = strtotime($end_date);

        $where = " create_stamp >= $start_date  and  create_stamp <= $end_date ";
        if (!empty($account_id)) {
            $where .= " and account_id = ".$account_id;
        }

        if (!empty($route)) {
            $where .= " and route.keyword = '".$route."'";
        }

        $query = new ElasticSearch();
        $res = $query->sql("select 
                                     SUM(count) as total,
                                     SUM(case when status = 1 then count else 0 end) as success_count,
                                     SUM(case when status = 2 then count else 0 end) as error_count,
                                     route.keyword  as route
                                 FROM hydra_access_count_logs 
                                     where $where group by route.keyword "
        );

        if (!empty($res)) {
            foreach ($res as $k => $v) {
                $res[$k]["success_rate"] = round($v['success_count'] / $v['total'] * 100, 2);
            }
        }

        return $this->success($res ?? []);

    }

    /**
     * 获取爬虫统计账户列表 （完）
     * @return mixed|string [type] [description]
     */
    public function getCrawlerByAccountList()
    {
        $start_date = $this->param('start_date');
        $end_date = $this->param('end_date');
        $route = $this->param('route');
        $account_id = $this->param('account_id');

        if (empty($start_date) || empty($end_date)) {
            return $this->error('参数异常');
        }

        $start_date = strtotime($start_date);
        $end_date  .= ' 23:59:59';
        $end_date   = strtotime($end_date);

        $where = " create_stamp >= $start_date  and  create_stamp <= $end_date ";
        if (!empty($account_id)) {
            $where .= " and account_id = ".$account_id;
        }

        if (!empty($route)) {
            $where .= " and route.keyword = '".$route."'";
        }


        $query = new ElasticSearch();
        $res = $query->sql("select 
                                     SUM(count) as total,
                                     SUM(case when status = 1 then count else 0 end) as success_count,
                                     SUM(case when status = 2 then count else 0 end) as error_count,
                                     max(id) as id,
                                     account_id
                                 FROM hydra_access_count_logs 
                                     where $where group by account_id"
        );


        $id = [];
        if (!empty($res)) {
            foreach ($res as $k => $v) {
                $id[$v["id"]] = $v["id"];
                $res[$k]["success_rate"] = round($v['success_count'] / $v['total'] * 100, 2);
            }
        }

        if (!empty($id)) {
            // 获取账户信息
            $query = new ElasticSearch("hydra_access_count_logs", "_doc");
            $query->select(["id","platform","nick","account_id"]);
            $query->whereIn("id",$id);
            $query->limit(0, count($res));
            $account = $query->searchMulti();

            if (!empty($account)) {
                foreach ($res as $kk => $vv) {
                     foreach ($account as $value) {
                         if ($vv["id"] == $value["id"]) {
                             $res[$kk]["platform"] = $value["platform"];
                             $res[$kk]["nick"] = $value["nick"];
                         }
                         if (empty($res[$kk]["nick"])) {
                             $res[$kk]["platform"] = "未知";
                             $res[$kk]["nick"] = "未知";
                         }
                     }
                }
            }
        } else {
            $res = [];
        }

        return $this->success($res ?? []);

//        $end_date .= ' 23:59:59';
//        $fields = [
//            CountLog::raw('SUM(count) as total'),
//            CountLog::raw('SUM(case when status = 1 then count else 0 end) as success_count'),
//            CountLog::raw('SUM(case when status = 2 then count else 0 end) as error_count'),
//            'status', 'account_id'
//        ];
//
//
//        if (!empty($route)) {
//            $fields = array_merge($fields, ['route']);
//        }
//
//        $query =  CountLog::select($fields)
//            ->whereBetween('create_time', [$start_date, $end_date]);
//
//        if (!empty($route)) {
//            $query->where('route', 'like',  '%'.$route.'%');
//        }
//        if (!empty($account_id)) {
//            $query->where('account_id', $account_id);
//        }
//
//        $logs = $query->groupBy(['account_id'])->with('shop')->with('xiaohong')->with('sakura')
//            ->where('status', '>', 0)->get()->toArray();
//        if (!empty($logs)) {
//            $logs = collect($logs)->map(function($log) {
//                $log['success_rate'] = round($log['success_count'] / $log['total'] * 100, 2);
//                $log['nick'] = $log['shop']['nick'] ?? $log['xiaohong']['nickName'] ?? $log['sakura']['name'] ?? '未知';
//                if (!empty($log['shop'])) {
//                    $log['platform'] = '阿里妈妈';
//                } elseif (!empty($log['xiaohong'])) {
//                    $log['platform'] = '小红书';
//                } elseif (!empty($log['sakura'])) {
//                    $log['platform'] = 'unidesk';
//                } else {
//                    $log['platform'] = '未知';
//                }
//                $log['success_count'] += 0;
//                $log['error_count'] += 0;
//                $log['total'] += 0;
//                unset($log['shop']);
//                unset($log['xiaohong']);
//                unset($log['sakura']);
//                return $log;
//            })->toArray();
//        }

        return $this->success($logs ?? []);

    }

    /**
     * 获取爬虫统计详细 （完）
     * @return mixed|string [type] [description]
     */
    public function getCrawlerDetails()
    {
        $start_date = $this->param('start_date');
        $end_date = $this->param('end_date');
        $route = $this->param('route');
        $status = $this->param('status');
        $page = $this->param('page') ?? 1;
        $page_size = $this->param('page_size') ?? 20;
        $account_id = $this->param('account_id');


        if (empty($start_date) || empty($end_date)) {
            return $this->error('参数异常');
        }

        $start_date = strtotime($start_date);
        $end_date  .= ' 23:59:59';
        $end_date   = strtotime($end_date);

        $query = new ElasticSearch("hydra_access_count_logs","_doc");

        $where = " create_stamp >= $start_date  and  create_stamp <= $end_date ";
        if (!empty($route)){
            $where .= " and route.keyword = '".$route."'";
            $query->where("route.keyword", $route);
        }

        if (!empty($status)) {
            $where .= " and status = '".$status."'";
            $query->where("status", $status);
        }

        if (!empty($account_id)) {
            $where .= " and account_id = '".$account_id."'";
            $query->where("account_id", $account_id);
        }

        $query->whereBetween("create_stamp", [$start_date,$end_date]);
        $query->orderBy("create_stamp", "desc");
        $query->limit($page, $page_size);
        $logs = $query->searchMulti();


        $query = new ElasticSearch();
        $count= $query->count(" select coun(*) as count from hydra_access_count_logs where $where ");


//        if ($page > 1) {
//            $offset = ($page - 1) * $page_size;
//        } else {
//            $offset = 0;
//        }

//        $end_date .= ' 23:59:59';
//
//        $query =  CountLog::whereBetween('create_time', [$start_date, $end_date]);
//
//        if (!empty($route)) {
//            $query->where('route', $route);;
//        }
//        if (!empty($status)) {
//            $query->where('status', $status);
//        }
//        if (!empty($account_id)) {
//            $query->where('account_id', $account_id);
//        }
//
//        $count = $query->where('status', '>', 0)->count();
//
//        $logs = $query->orderBy('create_time', 'desc')->offset($offset)->limit($page_size)->with('shop')->with('xiaohong')->with('sakura')->where('status', '>', 0)->get()->toArray();
//
//        if (!empty($logs)) {
//            $logs = collect($logs)->map(function($log) {
//                $log['nick'] = $log['shop']['nick'] ?? $log['xiaohong']['nickName'] ?? $log['sakura']['name'] ?? '未知';
//                if (!empty($log['shop'])) {
//                    $log['platform'] = '阿里妈妈';
//                } elseif (!empty($log['xiaohong'])) {
//                    $log['platform'] = '小红书';
//                } elseif (!empty($log['sakura'])) {
//                    $log['platform'] = 'unidesk';
//                } else {
//                    $log['platform'] = '未知';
//                }
//                unset($log['shop']);
//                unset($log['xiaohong']);
//                unset($log['sakura']);
//                return $log;
//            })->toArray();
//        }

        $result = [
            'total' => $count,
            'page' => $page,
            'page_size' => $page_size,
            'list' => $logs
        ];
        return $this->success($result);
    }

    /**
     * 获取爬虫分时分日统计 完
     * @return mixed|string [type] [description]
     */
    public function getCrawlerCurveInfo()
    {
        $start_date = $this->param('start_date');
        $end_date = $this->param('end_date');
        $route = $this->param('route');
        $account_id = $this->param('account_id');

        if (empty($start_date) || empty($end_date)) {
            return $this->error('参数异常');
        }

        $start_time = strtotime($start_date);
        $end_time   = $end_date.' 23:59:59';
        $end_time   = strtotime($end_time);

        $where = " create_stamp >= $start_time  and  create_stamp <= $end_time ";
        if (!empty($account_id)) {
            $where .= " and account_id = ".$account_id;
        }

        if (!empty($route)) {
            $where .= " and route.keyword = '".$route."'";
        }


        $query = new ElasticSearch();

        if ($start_date == $end_date) {
            $res = $query->sql("select 
                                     SUM(count) as total,
                                     SUM(case when status = 1 then count else 0 end) as success_count,
                                     SUM(case when status = 2 then count else 0 end) as error_count,
                                     YEAR(created_at) AS Y,MONTH_OF_YEAR(created_at) AS M, DAY_OF_MONTH(created_at) AS D,HOUR_OF_DAY(created_at) AS H
                                 FROM hydra_access_count_logs 
                                     where $where group by Y,M,D,H"
            );
        } else {
            $res = $query->sql("select 
                                     SUM(count) as total,
                                     SUM(case when status = 1 then count else 0 end) as success_count,
                                     SUM(case when status = 2 then count else 0 end) as error_count,
                                     YEAR(created_at) AS Y,MONTH_OF_YEAR(created_at) AS M, DAY_OF_MONTH(created_at) AS D
                                 FROM hydra_access_count_logs 
                                     where $where group by Y,M,D"
            );
        }

        $logs = [];
        if (!empty($res)) {
            foreach ($res as $k=>$v) {
                if ($start_date == $end_date) {
                    $logs[$k]["hour"] = (string)$v["H"];
                } else {
                    $logs[$k]["day"]  = (string)$v["Y"].'-'.$v["M"]."-".$v["D"];
                }
                $logs[$k]["total"] = $v["total"];
                $logs[$k]["success_count"] = $v["success_count"];
                $logs[$k]["error_count"]   = $v["error_count"];
            }
        }

//        $end_time = $end_date . ' 23:59:59';
//        if ($start_date == $end_date) {
//            $query =  CountLog::select(
//                    CountLog::raw('DATE_FORMAT(create_time,\'%k\') as hour'),
//                    CountLog::raw('SUM(count) as total'),
//                    CountLog::raw('SUM(case when status = 1 then count else 0 end) as success_count'),
//                    CountLog::raw('SUM(case when status = 2 then count else 0 end) as error_count'),
//                    'status', 'account_id'
//                )
//                ->whereBetween('create_time', [$start_date, $end_time]);
//
//            $unit = 'hour';
//        } else {
//            $query =  CountLog::select(
//                    CountLog::raw('DATE_FORMAT(create_time,\'%Y-%m-%d\') as day'),
//                    CountLog::raw('SUM(count) as total'),
//                    CountLog::raw('SUM(case when status = 1 then count else 0 end) as success_count'),
//                    CountLog::raw('SUM(case when status = 2 then count else 0 end) as error_count'),
//                    'status', 'account_id'
//                )
//                ->whereBetween('create_time', [$start_date, $end_time]);
//            $unit = 'day';
//        }
//
//        if (!empty($route)) {
//            $query->where('route', 'like', '%' . $route . '%');
//        }
//
//        if (!empty($account_id)) {
//            $query->where('account_id', $account_id);
//        }
//
//        $logs = $query->groupBy($unit)->where('status', '>', 0)->get()->toArray();

        return $this->success($logs ?? []);
    }


    //获取一分钟内的爬虫详情列表 (完)
    public function getCrawlerDetailsInfo()
    {
        $start_date = $this->param('create_time');
        $route = $this->param('route');
        $status = $this->param('status');
        $account_id = $this->param('account_id');

        if (empty($start_date) || empty($route) || empty($status) || empty($account_id)) {
            return $this->error('参数异常');
        }
        $start_date = strtotime($start_date);
        $end_date   = $start_date + 60;

        // 查询es数据
        $query = new ElasticSearch("hydra_access_logs", "_doc");
        //$query->select(["account_id","client_ip","status","route"]);
        $query->where("account_id", $account_id);
        $query->where("route", $route);
        $query->where("status", $status);
        $query->whereBetween("create_stamp", [$start_date,$end_date]);
        $query->orderBy("create_stamp", "desc");
        $query->limit(0, 10000);
        $res   =  $query->searchMulti();

        return $this->success($res);
    }

    /**
     * 获取条件 店铺
     * @return mixed|string [type] [description]
     */
    public function getConditionAccount()
    {
        $app_key_range = ['24556529', '25051557', '23749352'];

        $nicks = Shop::whereIn('status', [0, 1, -2])->pluck('nick')->unique()->values()->toArray();

        $accounts = Session::select('account_id as value', 'nick as label')->distinct(true)
            ->whereIn('app_key', $app_key_range)
            ->whereIn('nick', $nicks)
            ->where('r1_expires_at', '>=', date('Y-m-d H:i:s'))->get()->toArray();

        $account_xiaohong = Account::select('id as value', 'nickName as label')->where('status', 1)->get()->toArray();
        $account_ud = Advertiser::select('id as value', 'name as label')->whereNotNull('name')->where('status', 1)->get()->toArray();

        $account = array_merge($accounts, $account_xiaohong, $account_ud);
        return $this->success($account);
    }
}
