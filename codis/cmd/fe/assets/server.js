'use strict';

var server = angular.module('serverInfo', ["ui.bootstrap", "ngRoute"]);

$(document).ready(function() {
    $("#all_response_cmd").select2();
});

var g_refresh_interval  = 0;

function getUrlParams () {
    var url = window.location.href;
    var codis_position = url.indexOf("codis=");
    var group_position = url.indexOf("group=");
    var server_position = url.indexOf("server=");
    var interval_position = url.indexOf("interval=");
    var codis = url.substring(codis_position+6, group_position-2);
    var group = url.substring(group_position+6, server_position-1);
    var server = url.substring(server_position+7, interval_position-1);
    var interval = url.substring(interval_position+9);
    g_refresh_interval = interval;

    return {codis:codis, group:group, server:server, Interval:interval};
}

function changeGlobalInterval(interval) {
    g_refresh_interval = interval;
}

function getGlobalInterval() {
    return g_refresh_interval;
}

function triggerCodisTree() {
    parent.triggerIframe();
}

function showStats(stats) {
    document.getElementById('stats_info').innerHTML = "<p>" + stats + "</p>";
}

function getServerInfo (stats_info, group_id, server_addr) {
    var group_info = stats_info.group; 
    var server_info;
    var group_index;
    
    //根据groupid锁定信息所在的groupinfo.models组下标
    for (var i=0; i<group_info.models.length; i++) {
        if (group_info.models[i].id == group_id) {
            group_index = i;
            break ;
        }    
    }

    var server_arr = [];
    server_arr = group_info.models[group_index].servers;

    for (var i=0; i<server_arr.length; i++) {
        if (server_addr == server_arr[i].server) {
            server_info = server_arr[i];
            break ; 
        }
    }
 
    var master = "NA";
    var memory = "NA";
    var cache_mem = "NA";
    var cache_num = "NA";
    var readcmd_num = "NA";
    var cache_hitrate = "NA";
    var maxmem = "NA"; 
    var type = "NA";
    var version = "";
    var keys = "NA"; 
    var have_cache = false;
    if (group_info.stats[server_addr] && !group_info.stats[server_addr].timeout && !group_info.stats[server_addr].error) {
        if (group_info.stats[server_addr].stats["master_addr"]) {
            master = group_info.stats[server_addr].stats["master_addr"] + ":" + group_info.stats[server_addr].stats["master_link_status"];
        } else {
            master = "NO:ONE";
        }

        var v = parseInt(group_info.stats[server_addr].stats["used_memory"], 10);
        memory = humanSize(v);

        v = parseInt(group_info.stats[server_addr].stats["cache_memory"], 10);
        cache_mem = humanSize(v);

        v = parseInt(group_info.stats[server_addr].stats["cache_db_num"], 10);
        cache_num = v;

        v = parseInt(group_info.stats[server_addr].stats["all_cmds"], 10);
        readcmd_num = v;

        v = group_info.stats[server_addr].stats["hitratio_all"]
        cache_hitrate = v;


        var vm = parseInt(group_info.stats[server_addr].stats["maxmemory"], 10);
        if (vm == 0) {
            maxmem = "INF.";
        } else {
            maxmem = humanSize(vm);
        }

        if (group_info.stats[server_addr].stats["redis_version"]) {
            type = "redis";
            version = group_info.stats[server_addr].stats["redis_version"];
            have_cache = false;
        } else if (group_info.stats[server_addr].stats["pika_version"]){
            type = "pika";
            version = group_info.stats[server_addr].stats["pika_version"];
            var pika_version = group_info.stats[server_addr].stats["pika_version"];
            if (pika_version != "3.5-2.2.6" && parseInt(pika_version.slice(0,1)) >= 3) {
                if (group_info.stats[server_addr].stats["cache_keys"]) {
                    have_cache = true;
                } else {
                    have_cache = false;
                }
            } else {
                have_cache = false;
            }
        }

        keys = group_info.stats[server_addr].stats.db0;
    }

    var synced = "";
    if (server_info.action) {
        synced = server_info.action.state;
    }

    return {id:group_id, 
            addr:server_addr, 
            master:master, 
            synced:synced, 
            memory:memory, 
            maxmem:maxmem, 
            keys:keys, 
            type:type,
            version:version, 
            have_cache:have_cache,
            cache_mem:cache_mem, 
            cache_num:cache_num, 
            readcmd_num:readcmd_num,
            cache_hitrate:cache_hitrate};
}

server.factory('redirectionUrl', [function() {
    var redirectionUrl = {
        response: function(response) {
            if (response.headers("content-type").indexOf("html") >= 0){
                goHomePage();
            }
            return response;
        }
    };
    return redirectionUrl;
}]);


server.config(['$interpolateProvider',
    function ($interpolateProvider) {
        $interpolateProvider.startSymbol('[[');
        $interpolateProvider.endSymbol(']]');
    }
]);

server.config(['$httpProvider', function ($httpProvider) {
    $httpProvider.defaults.useXDomain = true;
    delete $httpProvider.defaults.headers.common['X-Requested-With'];
    $httpProvider.interceptors.push('redirectionUrl');
}]);

server.config(['$routeProvider', function ($routeProvider) {  
    ;  
}]);

server.run(['$rootScope', '$window', '$location', '$log', function ($rootScope, $window, $location, $log) {  
    var locationChangeSuccessOff = $rootScope.$on('$locationChangeSuccess', locationChangeSuccess); 
  
    function locationChangeSuccess(event) {  
        var $scope = window.scope;  
        var param = getUrlParams();
        $scope.selectServerInstance(param.server, param.group);
        scrollTo(0,0);
    } 
}]);


server.controller('MainServerCtrl', ['$scope', '$http', '$uibModal', '$timeout',
    function ($scope, $http, $uibModal, $timeout) {
        window.scope = $scope;

        /*Highcharts.setOptions({
            global: {
                useUTC: false,
            },
            exporting: {
                enabled: false,
            },
        });*/
        $scope.dashboard_version = "";
        $scope.have_more_cmd_info = false;
        $scope.chart_ops = newChatsOpsConfig();
        var qps_chart = echarts.init(document.getElementById('chart_ops'));
        qps_chart.setOption(newChatsOpsConfigEcharts("", 1));
        $scope.qps_arr = [];
        $scope.refresh_interval = 3;
        $scope.influxdb_period_s = 1;
        $scope.serverHistoryChart = "";
        $scope.readCmdQPSChart = "";
        $scope.have_cache = false;
        $scope.server_type = "pika";
        $scope.select_response_cmd = "";
        $scope.more_tp_cmd = "tp999";
        $scope.server_cmd_response_time_list = [];
        $scope.server_cmd_response_chart_list = [];
        $scope.legend_record_map = {};
        $scope.history_chart_map = {};
        //延时精度相关变量
        $scope.cmd_info_interval_mark = [
            {interval:1, level:1*3600, table_suffix:"1s"},
            {interval:10, level:5*3600, table_suffix:"10s"},
            {interval:60, level:2*24*3600, table_suffix:"60s"},
            {interval:600, level:15*24*3600, table_suffix:"600s"},
            {interval:3600, level:30*24*3600, table_suffix:"3600s"}
        ];

        window.onresize = function(){
            qps_chart.resize();
            /*if ($scope.serverHistoryChart != "") {
                $scope.serverHistoryChart.resize();
            }
            if ($scope.readCmdQPSChart != "") {
                $scope.readCmdQPSChart.resize();
            }*/
            for (var key in $scope.history_chart_map) {
                $scope.history_chart_map[key].resize();
            }
        }

        $scope.triggerCodisTree = function () {
            triggerCodisTree();
            window.onresize();
        }
        
        $scope.resetGroup = function () {
            $scope.server_info = {
                id:"NA", 
                addr:"NA", 
                master:"NA", 
                maxmem:"NA", 
                synced:"NA", 
                memory:"NA", 
                keys:"NA", 
                type:"NA", 
                cache_mem:"NA",
                cache_num:"NA",
                readcmd_num:"NA", 
                cahce_hitrate:"NA"};
            $scope.server_stats = "NA";
            $scope.server_stats_arr = [];

            $scope.chart_ops.series[0].data = [];
            $scope.qps_arr = [];
            $scope.dashboard_version = "";
            $scope.have_more_cmd_info = false;
        }
        $scope.resetGroup();
        
        $scope.param = getUrlParams();
        $scope.codis_name = $scope.param.codis;
        $scope.selected = $scope.param.server;
        $scope.refresh_interval = $scope.param.Interval;

        $scope.selectServerInstance = function (selected, groupid) {
            if(selected == $scope.server_info.addr){
                return ;
            }
            
            $scope.group_id = groupid;
            $scope.selected = selected;
            setColor($scope.search_id, "");
            $scope.search_type = "instantaneous_ops_per_sec";
            $scope.search_id = "server_qps";
                
            $scope.resetGroup();

            var codis_name =  $scope.codis_name;

            var url = concatUrl("/topom", codis_name);
            $http.get(url).then(function (resp) {
                var overview = resp.data;
                $scope.dashboard_version = overview.version;
                $scope.have_more_cmd_info = $scope.haveMoreCmdInfo($scope.dashboard_version);
                var influxdb_period = overview.config.metrics_report_influxdb_period;
                var influxdb_period_unit = influxdb_period.charAt(influxdb_period.length - 1);
                $scope.influxdb_period_s = parseInt(influxdb_period.substring(0, influxdb_period.length - 1));
                if (influxdb_period_unit == "m") {
                    $scope.influxdb_period_s *= 60;
                }
                $scope.stats_type = "info";
                $scope.getServerStatsByType();
                $scope.updateServerstats(overview.stats, groupid, selected);
                $scope.fillHistory();
                $scope.show_more_info = false;
                $('#more_info').hide();
            },function (failedResp) {
                // 请求失败执行代码
                if (selected == getUrlParams().server) {
                    alert("get " + codis_name + " info failed!");
                    clearHistorychart("historyCharts");   
                }
            });
        }
    
        $scope.parseJson = function(Json){
            for (var key in Json) {
                $scope.server_stats_arr.push(key + " : " + Json[key]);
            }
        }

        $scope.updateServerstats = function (codis_stats, groupid, selected) {
            var overview = codis_stats;
            var groupinfo = overview.group;

            if (!groupinfo.stats[selected] || groupinfo.stats[selected].timeout || groupinfo.stats[selected].error) {
                //historyHide();
                return ;
            }

            $scope.server_info = getServerInfo(overview, groupid, selected);
            $scope.have_cache = $scope.server_info.have_cache;
            $scope.server_stats = groupinfo.stats[selected].stats;
            $scope.server_stats_arr = [];
            $scope.parseJson($scope.server_stats);           
            
            var ops_array = $scope.chart_ops.series[0].data;
            if (ops_array.length >= 20) {
                ops_array.shift();
            }

            $scope.server_type = "Redis";
            if (!($scope.server_stats.redis_version)) {
                $scope.server_type = "Pika";
            }

            var server_data = Number($scope.server_stats.instantaneous_ops_per_sec);
            ops_array.push({x: new Date(), y: server_data});
            $scope.chart_ops.series[0].data = ops_array;

            if ($scope.qps_arr.length >= 20) {
                $scope.qps_arr.shift();
            }
            $scope.qps_arr.push([new Date(),server_data]);
            qps_chart.setOption({
                series: [{
                    data: $scope.qps_arr
                }]
            });

            if (!($("#chart_container_div", window.parent.document).is(":hidden"))) {
                if (parent.full_screen_chart_owner == "server_qps_charts") {
                    parent.full_screen_chart_for_iframe.setOption({
                        series: [{
                            data: $scope.qps_arr
                        }]
                    });
                }
            }

            $scope.group_id = groupid;
            $scope.selected = selected;
            //historyShow();
        }

        $scope.changeInterval = function(interval) {
            changeGlobalInterval(interval);
        }

        $scope.showServerQpsChartFullScreen = function() {
            $("#chart_container_div", window.parent.document).show();
            parent.full_screen_chart_owner = "server_qps_charts";
            parent.full_screen_chart_for_iframe.clear();
            parent.full_screen_chart_for_iframe.resize();
            parent.full_screen_chart_for_iframe.setOption(qps_chart.getOption());
            $(window).resize(function(){
                parent.full_screen_chart_for_iframe.resize();
            });
        }

        
        $scope.showServerHistoryChartContainer = function(chart_id_prefix, chart_id_cmd) {
            var type = chart_id_prefix + chart_id_cmd;
            $scope.showServerHistoryChartFullScreen(type);
        }

        $scope.showServerHistoryChartFullScreen = function(type) {
            $("#chart_container_div", window.parent.document).show();
            parent.full_screen_chart_owner = "server_history_charts";
            parent.full_screen_chart_for_iframe.clear();
            parent.full_screen_chart_for_iframe.resize();
            var option = "";
            if ($scope.history_chart_map[type] == undefined) {
                return;
            }
            //if (type == "historyCharts") {
            option = $scope.history_chart_map[type].getOption();
            option.dataZoom = [
                {
                    type: 'inside',
                    show: false,
                    xAxisIndex: [0],
                }
            ];
            /*} else if (type == "read_cmd_qps") {
                option = $scope.readCmdQPSChart.getOption();
                option.dataZoom = [
                    {
                        type: 'inside',
                        show: false,
                        xAxisIndex: [0],
                    }
                ];
            }*/
            parent.full_screen_chart_for_iframe.setOption(option);
            $(window).resize(function(){
                parent.full_screen_chart_for_iframe.resize();
            });
        }

        $scope.refresh_server_stats = function () {
            var codis_name = $scope.codis_name;
            if (isValidInput(codis_name)) {
                var xauth = genXAuth(codis_name);
                var url = concatUrl("/api/topom/stats/" + xauth, codis_name);
                $http.get(url).then(function (resp) {
                    var overview = resp.data;
                    $scope.updateServerstats(overview,$scope.group_id,$scope.selected);
                });
            }
        }
        //operate of history charts 
        //$scope.search_type = "";
        //$scope.search_id = "";
        $scope.search_type = "instantaneous_ops_per_sec";
        $scope.search_id = "server_qps";
        $scope.data_unit = "count(次)/s";
        $scope.time_id = "";

        $scope.fillHistory = function () {   
            setColor("", $scope.search_id);
            initTimer(6, 0);
            setColor($scope.time_id, "");
            $scope.time_id = "SixHours";
            setColor("", $scope.time_id);
            
            if ($scope.search_id == "server_qps") {
                $scope.getHistoryCmdResponseList();
            } else {
                $scope.getHistory("");
            }
        }

        $scope.haveMoreCmdInfo = function (dashboard_version) {
            //默认线上的集群都是3.x版本
            if (dashboard_version < "3.2.2-1.1") {
                return false;
            } 
            return true;
        }

        $scope.getHistoryCmdResponseList = function () {
            var codis_name = $scope.codis_name;
            var xauth = genXAuth(codis_name);
            var interval_mark = $scope.getIntervalMark();
            var table_suffix = interval_mark.table_suffix;
            var url_param = "";
            if (table_suffix == "" || !($scope.haveMoreCmdInfo($scope.dashboard_version))) {
                $scope.getHistory("");
                return;
            } else {
                url_param = interval_mark.interval.toString() + "/";
                table_suffix = "_cmd_info_" + table_suffix;
            }
            var table_name = getTableName("server_", $scope.server_info.addr);
            table_name = table_name + table_suffix;
            var sql_command = getShowSqlCommand(table_name);
            var url = concatUrl("/api/influxdb/query/" + xauth + "/" + url_param + sql_command, codis_name);
            $scope.select_response_cmd = "";
            if ($scope.server_cmd_response_chart_list.length > 0) {
                $scope.select_response_cmd = $scope.server_cmd_response_chart_list[0];
            }
            $scope.server_cmd_response_time_list = [];
            $scope.server_cmd_response_chart_list = [];
            $http.get(url).then(function (resp) {
                if (resp.data.Results && resp.data.Results[0].Series) {
                    $scope.server_cmd_response_time_list = [];
                    var cmd_list = resp.data.Results[0].Series[0].values;
                    var all_cmd_index = -1;
                    for (var i=0; i<cmd_list.length; i++) {
                        $scope.server_cmd_response_time_list.push(cmd_list[i][1]);
                        if (cmd_list[i][1] == $scope.select_response_cmd) {
                            $scope.server_cmd_response_chart_list.push(cmd_list[i][1]);
                        }
                        if (cmd_list[i][1] == "ALL") {
                            all_cmd_index = i;
                        }
                    }
                    if ($scope.server_cmd_response_chart_list.length <= 0 && all_cmd_index >= 0) {
                        $scope.server_cmd_response_chart_list.push(cmd_list[all_cmd_index][1]);
                    }
                }
                if ($scope.server_cmd_response_chart_list.length > 0) {
                    if ($scope.search_type == "delaystats") {
                        $scope.getHistory("delaystats");
                    } else {
                        $scope.getHistory("response");
                    }
                } else {
                    $scope.getHistory("");
                }
            },function (failedResp) {
                // 请求失败执行代码 
                $scope.getHistory("");   
            });
        }

        $scope.getIntervalMark = function () {
            var start_seconds = getTimeById('start_time');
            var end_seconds = getTimeById('end_time');
            var diff_seconds = (end_seconds - start_seconds)/1000;
            for (var i=0; i<$scope.cmd_info_interval_mark.length; i++) {
                if (diff_seconds > $scope.cmd_info_interval_mark[i].level) {
                    continue;
                } else {
                    return $scope.cmd_info_interval_mark[i];
                }
            }
            return {interval:-1, level:-1, table_suffix:""};
        }

        $scope.setType = function (id, type, count) {
            if ($scope.search_type == type) {
                return ;
            }
            setColor($scope.search_id, id);
            $scope.search_type = type;
            $scope.search_id = id;
            $scope.data_unit = count;
            //$scope.getHistory("");
            $scope.defaultLegendRecordMap(id);
            $scope.refreshHistory();
        }

        $scope.defaultLegendRecordMap = function(id) {
            if (id == "server_qps") {
                $scope.legend_record_map = {
                    "historyCharts": {"qps":"qps"},
                    "server_cmd_reponse_time_ALL":{"tp999":"tp999", "tp9999":"tp9999", "avg":"avg"},
                    "server_cmd_count_ALL":{"50ms":"50ms", "100ms":"100ms"},
                    "server_cmd_fails_ALL":{"redis_errtype":"redis_errtype"}
                };
            }  else if (id == "delay_stats") {
                $scope.legend_record_map = {
                    "historyCharts": {"qps":"qps"},
                    "server_cmd_reponse_time_ALL":{"50ms":"50ms", "100ms":"100ms"},
                };
            } else if (id == "Qps_all") {
                $scope.legend_record_map = {
                    "historyCharts": {"ALL":"ALL"},
                    "server_all_cmd_tp999": {"ALL":"ALL"}
                };
            } else {
                ;
            }
        }
        $scope.defaultLegendRecordMap("server_qps");

        $scope.quickSearch = function (start, end, timeId) {
            setColor($scope.time_id, timeId);
            $scope.time_id = timeId;
            initTimer(start,end);
            //$scope.getHistory();
            $scope.refreshHistory();
        }

        $scope.selectHistoryCmd = function (cmd) {
            $scope.server_cmd_response_chart_list = [];
            $scope.server_cmd_response_chart_list.push(cmd);
            if ($scope.search_type == "delaystats") {
                $scope.getHistory("delaystats");
                return;
            }
            $scope.getHistory("response");
        }

        $scope.refreshHistory = function () {
            if ($scope.search_type == "instantaneous_ops_per_sec" || $scope.search_type == "delaystats") {
                $scope.getHistoryCmdResponseList();
            } else if ($scope.search_type == "more") {
                $scope.getHistory("more");
            } else {
                $scope.getHistory("");
            }
        }

        $scope.getHistory = function (type) {
            var codis_name = $scope.codis_name;
            var xauth = genXAuth(codis_name);
            var start_seconds = getTimeById('start_time');
            var end_seconds = getTimeById('end_time');
            var table_name = getTableName("server_", $scope.selected);
            var search_type = $scope.search_type;
            if (search_type == "cache_hit_rate") {
                search_type = "cache_hit_rate,cache_read_qps"
            }

            if (start_seconds >= end_seconds) {
                alert("开始时间必须小于结束时间，请重新设置！");
                return ;
            }

            if (type == "more") {
                $scope.showAllCmdInfo(codis_name, xauth, start_seconds, end_seconds);
                return;
            }

            if (type == "change_tp_cmd") {
                $scope.getAllCmdInfoChart("server_all_cmd_tp999", codis_name, xauth, start_seconds, end_seconds, $scope.more_tp_cmd);
                return;
            }

            if (type == "delaystats") {
                var interval_mark = $scope.getIntervalMark();
                var table_suffix = interval_mark.table_suffix;
                var interval = interval_mark.interval;
                var url_param = "";
                if (table_suffix == "" || !($scope.haveMoreCmdInfo($scope.dashboard_version))) {
                    url_param = "";
                    table_suffix = "";
                    interval = 0;
                } else {
                    url_param = interval.toString() + "/";
                    table_suffix = "_cmd_info_" + table_suffix;
                }
                for (var i = 0; i < $scope.server_cmd_response_chart_list.length; i++) {
                    var table_name = getTableName("server_", $scope.server_info.addr);
                    table_name = table_name + table_suffix;
                    var search_type = "qps,delay50ms,delay100ms,delay200ms,delay300ms,delay500ms,delay1s,delay2s,delay3s";
                    var sql_command = getSelectSqlCommand(start_seconds, end_seconds, interval, table_name, search_type, "cmd_name='" + $scope.server_cmd_response_chart_list[i] + "'", $scope.influxdb_period_s);
                    var url = concatUrl("/api/influxdb/query/" + xauth + "/" + url_param + sql_command, codis_name);
                    
                    $scope.getCmdCountHistoryChart(search_type, url, $scope.server_cmd_response_chart_list[i]);

                }
                return;
            }

            if (type == "response") {
                var interval_mark = $scope.getIntervalMark();
                var table_suffix = interval_mark.table_suffix;
                var interval = interval_mark.interval;
                var url_param = "";
                if (table_suffix == "" || !($scope.haveMoreCmdInfo($scope.dashboard_version))) {
                    url_param = "";
                    table_suffix = "";
                    interval = 0;
                } else {
                    url_param = interval.toString() + "/";
                    table_suffix = "_cmd_info_" + table_suffix;
                }
                for (var i = 0; i < $scope.server_cmd_response_chart_list.length; i++) {
                    var table_name = getTableName("server_", $scope.server_info.addr);
                    table_name = table_name  + table_suffix;
                    var search_type = "qps,tp90,tp99,tp999,tp9999,tp100,delay50ms,delay100ms,delay200ms,delay300ms,delay500ms,delay1s,delay2s,delay3s,avg,redis_errtype";
                    var sql_command = getSelectSqlCommand(start_seconds, end_seconds, interval, table_name, search_type, "cmd_name='" + $scope.server_cmd_response_chart_list[i] + "'", $scope.influxdb_period_s);
                    var url = concatUrl("/api/influxdb/query/" + xauth + "/" + url_param + sql_command, codis_name);
                    
                    $scope.getServerCmdHistoryChart(search_type, url, $scope.server_cmd_response_chart_list[i]);

                }
                return;
            }

            var sql_command = getSelectSqlCommand(start_seconds, end_seconds, 0, table_name, search_type, "", $scope.influxdb_period_s); 
            $scope.sql = sql_command;
            var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, codis_name);
            $http.get(url).then(function (resp) {
                var show_lenged = "";
                if (resp.data.Results && resp.data.Results[0].Series) {
                    show_lenged = search_type;
                    if ($scope.search_type == "cache_hit_rate") {
                        show_lenged = "cache_hit_rate";
                    }
                    var chart = showHistory("historyCharts",resp.data.Results[0].Series[0], search_type, show_lenged);
                    chart.on('datazoom', function (params) {
                        var startValue = chart.getModel().option.dataZoom[0].startValue;
                        var endValue = chart.getModel().option.dataZoom[0].endValue;
                        
                        var start_time = "";
                        var end_time = "";
                        var chart_option = chart.getOption();
                        
                        start_time = chart_option.xAxis[0].data[startValue].slice(0,-3);
                        end_time = chart_option.xAxis[0].data[endValue].slice(0,-3);
                        if (start_time != "" && end_time != "") {
                            setDateValue(start_time, end_time);
                            $scope.refreshHistory();
                        }
                    });
                    //$scope.serverHistoryChart = chart;
                    $scope.history_chart_map["historyCharts"] = chart;

                    if ($scope.search_type == "cache_hit_rate") {
                        var chart_qps = showHistory("read_cmd_qps_history",resp.data.Results[0].Series[0], search_type, "cache_read_qps", search_type);
                        chart_qps.on('datazoom', function (params) {
                            var startValue = chart_qps.getModel().option.dataZoom[0].startValue;
                            var endValue = chart_qps.getModel().option.dataZoom[0].endValue;
                            
                            var start_time = "";
                            var end_time = "";
                            var chart_option = chart_qps.getOption();
                            
                            start_time = chart_option.xAxis[0].data[startValue].slice(0,-3);
                            end_time = chart_option.xAxis[0].data[endValue].slice(0,-3);
                            if (start_time != "" && end_time != "") {
                                setDateValue(start_time, end_time);
                                $scope.refreshHistory();
                            }
                        });
                        
                        //$scope.readCmdQPSChart = chart_qps;
                        $scope.history_chart_map["read_cmd_qps_history"] = chart;
                    }
                } else {
                    alert(search_type + "数据获取失败！");
                    clearHistorychart("historyCharts");
                }
            },function (failedResp) {
                // 请求失败执行代码
                alert(search_type + "数据获取失败！");
                clearHistorychart("historyCharts");
            });
        }

        $scope.getServerCmdHistoryChart = function (search_type, url, cmd_name) {
            var chart_id = "";
            $http.get(url).then(function (resp) {
                if (resp.data.Results && resp.data.Results[0].Series) {
                    
                    chart_id = "historyCharts";
                    var selected_legend = getSelectedLegend($scope.legend_record_map, chart_id);
                    if(selected_legend == "") {
                        selected_legend = "qps";
                        $scope.legend_record_map[chart_id] = {"qps": "qps"};
                    }
                    $scope.history_chart_map[chart_id] = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, "qps", search_type, selected_legend);
                    $scope.addDataZoomEvent($scope.history_chart_map[chart_id]);
                    $scope.addLegendSelectChangeEvent($scope.history_chart_map[chart_id], chart_id);

                    chart_id = "server_cmd_reponse_time_" + cmd_name;
                    var selected_legend = getSelectedLegend($scope.legend_record_map, chart_id);
                    if(selected_legend == "") {
                        selected_legend = "tp999,tp9999,avg";
                        $scope.legend_record_map[chart_id] = {"tp999": "tp999", "tp9999": "tp9999", "avg": "avg"};
                    }
                    $scope.history_chart_map[chart_id] = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, "tp90,tp99,tp999,tp9999,tp100,avg",  search_type, selected_legend);
                    $scope.addDataZoomEvent($scope.history_chart_map[chart_id]);
                    $scope.addLegendSelectChangeEvent($scope.history_chart_map[chart_id], chart_id);

                    chart_id = "server_cmd_count_" + cmd_name;
                    var selected_legend = getSelectedLegend($scope.legend_record_map, chart_id);
                    if(selected_legend == "") {
                        selected_legend = "50ms,100ms";
                        $scope.legend_record_map[chart_id] = {"50ms":"50ms", "100ms":"100ms"};
                    }
                    var legend = "qps,tp90,tp99,tp999,tp9999,tp100,50ms,100ms,200ms,300ms,500ms,1s,2s,3s,avg,redis_errtype";
                    $scope.history_chart_map[chart_id] = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, "50ms,100ms,200ms,300ms,500ms,1s,2s,3s",  legend, selected_legend);
                    $scope.addDataZoomEvent($scope.history_chart_map[chart_id]);
                    $scope.addLegendSelectChangeEvent($scope.history_chart_map[chart_id], chart_id);


                    chart_id = "server_cmd_fails_" + cmd_name;
                    var selected_legend = getSelectedLegend($scope.legend_record_map, chart_id);
                    if(selected_legend == "") {
                        selected_legend = "redis_errtype";
                        $scope.legend_record_map[chart_id] = {"redis_errtype":"redis_errtype"};
                    } 
                    $scope.history_chart_map[chart_id] = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, "redis_errtype", search_type, selected_legend);   
                    $scope.addDataZoomEvent($scope.history_chart_map[chart_id]);
                    $scope.addLegendSelectChangeEvent($scope.history_chart_map[chart_id], chart_id);
                } else {
                    alert(search_type + "数据获取失败！");
                    clearHistorychart("historyCharts");
                    clearHistorychart("server_cmd_reponse_time_" + cmd_name);
                    clearHistorychart("server_cmd_fails_" + cmd_name);
                    return;
                }
            },function (failedResp) {
                // 请求失败执行代码
                alert(search_type + "数据获取失败！");
                clearHistorychart("historyCharts");
                clearHistorychart("server_cmd_reponse_time_" + cmd_name);
                clearHistorychart("server_cmd_fails_" + cmd_name);
                return;
            });
        }

        $scope.getCmdCountHistoryChart = function (search_type, url, cmd_name) {
            var chart_id = "";
            $http.get(url).then(function (resp) {
                if (resp.data.Results && resp.data.Results[0].Series) {
                    
                    chart_id = "historyCharts";
                    var selected_legend = getSelectedLegend($scope.legend_record_map, chart_id);
                    if(selected_legend == "") {
                        selected_legend = "qps";
                        $scope.legend_record_map[chart_id] = {"qps":"qps"};
                    }
                    $scope.history_chart_map[chart_id] = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, "qps", search_type, selected_legend);
                    $scope.addDataZoomEvent($scope.history_chart_map[chart_id]);
                    $scope.addLegendSelectChangeEvent($scope.history_chart_map[chart_id], chart_id);

                    chart_id = "server_cmd_reponse_time_" + cmd_name;
                    var selected_legend = getSelectedLegend($scope.legend_record_map, chart_id);
                    if(selected_legend == "") {
                        selected_legend = "50ms,100ms";
                        $scope.legend_record_map[chart_id] = {"50ms":"50ms", "100ms":"100ms"};
                    }
                    var legend = "qps,50ms,100ms,200ms,300ms,500ms,1s,2s,3s";
                    $scope.history_chart_map[chart_id] = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, "50ms,100ms,200ms,300ms,500ms,1s,2s,3s",  legend, selected_legend);
                    $scope.addDataZoomEvent($scope.history_chart_map[chart_id]);
                    $scope.addLegendSelectChangeEvent($scope.history_chart_map[chart_id], chart_id);
                } else {
                    alert(search_type + "数据获取失败！");
                    clearHistorychart("historyCharts");
                    clearHistorychart("server_cmd_reponse_time_" + cmd_name);
                    return;
                }
            },function (failedResp) {
                // 请求失败执行代码
                alert(search_type + "数据获取失败！");
                clearHistorychart("historyCharts");
                clearHistorychart("server_cmd_reponse_time_" + cmd_name);
                return;
            });
        }

        $scope.addDataZoomEvent = function(chart) {
            chart.on('datazoom', function (params) {
                var startValue = chart.getModel().option.dataZoom[0].startValue;
                var endValue = chart.getModel().option.dataZoom[0].endValue;
                
                var start_time = "";
                var end_time = "";
                var chart_option = chart.getOption();
                
                start_time = chart_option.xAxis[0].data[startValue].slice(0,-3);
                end_time = chart_option.xAxis[0].data[endValue].slice(0,-3);
                
                if (start_time != "" && end_time != "") {
                    setDateValue(start_time, end_time);
                    $scope.refreshHistory();
                }
            });
        }

        $scope.addLegendSelectChangeEvent = function(chart, chart_id) {
            chart.on('legendselectchanged', function (params) {
                //alert(params.name + " : " + chart_id );
                if ($scope.legend_record_map[chart_id] == undefined) {
                    $scope.legend_record_map[chart_id] = {};
                }

                if ($scope.legend_record_map[chart_id][params.name] == undefined) {
                    $scope.legend_record_map[chart_id][params.name] = params.name;
                } else {
                    delete $scope.legend_record_map[chart_id][params.name];
                }
            });
        }

        $scope.showAllCmdInfo = function (codis_name, xauth, start_seconds, end_seconds) {
            $scope.getAllCmdInfoChart("historyCharts", codis_name, xauth, start_seconds, end_seconds, "qps", "more");
            $scope.getAllCmdInfoChart("server_all_cmd_tp999", codis_name, xauth, start_seconds, end_seconds, $scope.more_tp_cmd, "more");
        }

        $scope.getAllCmdInfoChart = function (chart_id, codis_name, xauth, start_seconds, end_seconds, type, btn_id) {
            var interval_mark = $scope.getIntervalMark();
            var table_suffix = interval_mark.table_suffix;
            var interval = interval_mark.interval;
            var url_param = "";
            if (table_suffix == "" || !($scope.haveMoreCmdInfo($scope.dashboard_version))) {
                url_param = "";
                table_suffix = "";
                interval = 0;
            } else {
                url_param = interval.toString() + "/";
                table_suffix = "_cmd_info_" + table_suffix;
            }
            var table_name = getTableName("server_", $scope.server_info.addr);
            table_name = table_name + table_suffix;
            var search_type = "";
            var sql_command = getSelectSqlCommand(start_seconds, end_seconds, interval, table_name, type, "", $scope.influxdb_period_s);
            sql_command = sql_command + ",cmd_name";
            var url = concatUrl("/api/influxdb/query/" + xauth + "/" + url_param + sql_command, codis_name);
            $http.get(url).then(function (resp) {
                if (resp.data.Results && resp.data.Results[0].Series) {
                    for (var i=0; i<resp.data.Results[0].Series.length; i++) {
                        if (0 == i) {
                            search_type = resp.data.Results[0].Series[i].tags.cmd_name;
                            continue;
                        } else if (resp.data.Results[0].Series[i].tags && resp.data.Results[0].Series[i].tags.cmd_name) {
                            search_type = search_type + "," + resp.data.Results[0].Series[i].tags.cmd_name;
                        }

                        for (var j=0; j<resp.data.Results[0].Series[0].values.length; j++) {
                            resp.data.Results[0].Series[0].values[j].push(resp.data.Results[0].Series[i].values[j][1]);
                        }
                    }

                    var legend_select_list = "ALL";
                    if ($scope.legendSelectedAll) {
                        legend_select_list = search_type;
                    }
                    var selected_legend = getSelectedLegend($scope.legend_record_map, chart_id);
                    var chart = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, search_type, "", selected_legend);   
                    chart.setOption({
                        legend:{
                            width: '85%',
                        }
                    });
                    chart.on('datazoom', function (params) {
                        var startValue = chart.getModel().option.dataZoom[0].startValue;
                        var endValue = chart.getModel().option.dataZoom[0].endValue;
                        //获得起止位置百分比
                        var start_time = "";
                        var end_time = "";
                        var chart_option = chart.getOption();

                        start_time = chart_option.xAxis[0].data[startValue].slice(0,-3);
                        end_time = chart_option.xAxis[0].data[endValue].slice(0,-3);
                        if (start_time != "" && end_time != "") {
                            setDateValue(start_time, end_time);
                            $scope.getHistory(btn_id);
                        }
                    });
                    $scope.addLegendSelectChangeEvent(chart, chart_id);
                    $scope.history_chart_map[chart_id] = chart;
                } else {
                    alert("数据获取失败！");
                    clearHistorychart(chart_id);
                    return;
                }
            },function (failedResp) {
                // 请求失败执行代码
                alert("数据获取失败！");
                clearHistorychart(chart_id);
                return;
            });
        }

        $scope.legendSelectedAll = false;
        $scope.selectAllCmd = function() {
            $scope.selectAllLegend("historyCharts");
            $scope.selectAllLegend("server_all_cmd_tp999");
            $scope.legendSelectedAll = !$scope.legendSelectedAll;
        }

        $scope.selectAllLegend = function(chart_id) {
            var option = $scope.history_chart_map[chart_id].getOption();
            var legend = option.legend[0].data;
            var selected_list = {};
            if ($scope.legendSelectedAll) {
                for (var i=0; i<legend.length; i++) {
                    selected_list[legend[i]] = false;
                }
                if (legend.length > 0) {
                    selected_list["ALL"] = true;
                }
                $scope.legend_record_map[chart_id] = {"ALL":"ALL"};
            } else {
                for (var i=0; i<legend.length; i++) {
                    selected_list[legend[i]] = true;
                }
                $scope.legend_record_map[chart_id] = {};
            }
            
            $scope.history_chart_map[chart_id].setOption({
                legend:{
                    data: legend,
                    selected: selected_list,
                    width: '85%',
                }
            });
        }



        $scope.show_more_info = false;
        $scope.showMoreInfo = function () {
            if (!$scope.show_more_info) {
                $('#more_info').show();
                $scope.show_more_info = true;
            } else {
                $('#more_info').hide();
                $scope.show_more_info = false;
            }
            $scope.getServerStatsByType();
        }

        $scope.stats_type = "info";
        $scope.Server_Info_suffix = "</br>";
        $scope.getServerStatsByType = function () {
            var codis_name = $scope.codis_name;
            if (isValidInput(codis_name)) {
                var xauth = genXAuth(codis_name);
                var cmd = "info";
                if ($scope.stats_type == "info") {
                    cmd = "info";
                } else if ($scope.stats_type == "client_list") {
                    cmd = "client list";
                } else if ($scope.stats_type == "config") {
                    cmd = "config get *";
                } else if ($scope.stats_type == "slowlog") {
                    cmd = "slowlog get 1000";
                } else if ($scope.stats_type == "cmdstats") {
                    cmd = "info delay";
                }  
                var url = concatUrl("/api/topom/docmd/" + xauth + "/" + $scope.selected + "/" + cmd, codis_name);
                $http.get(url).then(function (resp) {
                    $scope.slow_log_resp = resp.data;
                    if ((typeof $scope.slow_log_resp == 'string') && $scope.slow_log_resp.constructor == String) {
                        $scope.Server_Info = resp.data;
                    } else if ((typeof $scope.slow_log_resp == 'object') && $scope.slow_log_resp.constructor == Array) {
                        $scope.Server_Info = "";
                        for (var i=0; i<resp.data.length; i++) {
                            if (i%2 == 0) {
                                $scope.Server_Info += resp.data[i] + ": ";
                            } else {
                                $scope.Server_Info += resp.data[i] + "\r\n";
                            }
                        }
                        if ($scope.Server_Info == "") {
                            $scope.Server_Info = "no more response...";
                        }
                    }
                    if ($scope.Server_Info_suffix == "") {
                        $scope.Server_Info_suffix = "</br>";
                    } else {
                        $scope.Server_Info_suffix = "";
                    }
                    $scope.Server_Info = $scope.Server_Info_suffix + $scope.Server_Info .replace(/ /g, '&nbsp;').replace(/\r\n/g, '<br>').replace(/\n/g, '<br>').replace(/\t/g, '&nbsp;&nbsp;&nbsp;&nbsp;');
                    $scope.Server_Info = "<pre>" + $scope.Server_Info + "</pre>";
                    showStats($scope.Server_Info);
                });
            }
        }
        $scope.getServerStatsByType();

        $scope.downloadSlowLog = function () {
            var suffix = "";
            if ($scope.stats_type == "info") {
                suffix = "_info_";
            } else if ($scope.stats_type == "client_list") {
                suffix = "_client_";
            } else if ($scope.stats_type == "config") {
                suffix = "_config_";
            } else {
                suffix = "_slowlog_";
            }
            var blob = new Blob([$scope.slow_log_resp], {type: "text/plain;charset=utf-8"});
            var myDate = new Date();
            var time = myDate.getFullYear().toString();
            time += (myDate.getMonth()+1).toString() ;
            time += myDate.getDate().toString() ;
            time += myDate.getHours().toString() ;
            time += myDate.getMinutes().toString() ;
            time += myDate.getSeconds().toString() ;
            saveAs(blob, "server_" + $scope.param.server + suffix + time + ".txt");
        }

        var ticker = 0;
        (function autoRefreshStats() {
            if (ticker >= $scope.refresh_interval) {
                ticker = 0;
                $scope.refresh_server_stats();
            }
            ticker++;
            $timeout(autoRefreshStats, 1000);
        }());
    }
])
;
