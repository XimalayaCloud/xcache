'use strict';

var server = angular.module('serverInfo', ["ui.bootstrap", "ngRoute"]);

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

        window.onresize = function(){
            qps_chart.resize();
            if ($scope.serverHistoryChart != "") {
                $scope.serverHistoryChart.resize();
            }
            if ($scope.readCmdQPSChart != "") {
                $scope.readCmdQPSChart.resize();
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

        $scope.showServerHistoryChartFullScreen = function(type) {
            $("#chart_container_div", window.parent.document).show();
            parent.full_screen_chart_owner = "server_history_charts";
            parent.full_screen_chart_for_iframe.clear();
            parent.full_screen_chart_for_iframe.resize();
            var option = "";
            if (type == "historyCharts") {
                option = $scope.serverHistoryChart.getOption();
                option.dataZoom = [
                    {
                        type: 'inside',
                        show: false,
                        xAxisIndex: [0],
                    }
                ];
            } else if (type == "read_cmd_qps") {
                option = $scope.readCmdQPSChart.getOption();
                option.dataZoom = [
                    {
                        type: 'inside',
                        show: false,
                        xAxisIndex: [0],
                    }
                ];
            }
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
        $scope.search_type = "";
        $scope.search_id = "";
        $scope.search_type = "server_qps";
        $scope.search_id = "server_qps";
        $scope.data_unit = "count(次)/s";
        $scope.time_id = "";

        $scope.fillHistory = function () {   
            setColor("", $scope.search_id);
            setColor("", $scope.time_id);
            initTimer(6, 0);
            setColor($scope.time_id, "");
            $scope.getHistory();
        }

        $scope.setType = function (id, type, count) {
            if ($scope.search_type == type) {
                return ;
            }
            setColor($scope.search_id, id);
            $scope.search_type = type;
            $scope.search_id = id;
            $scope.data_unit = count;
            $scope.getHistory();
        }

        $scope.quickSearch = function (start, end, timeId) {
            setColor($scope.time_id, timeId);
            $scope.time_id = timeId;
            initTimer(start,end);
            $scope.getHistory();
        }

        $scope.getHistory = function () {
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

            var sql_command = getSelectSqlCommand(start_seconds, end_seconds, table_name, search_type, "", $scope.influxdb_period_s); 
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
                            $scope.getHistory();
                        }
                    });
                    $scope.serverHistoryChart = chart;

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
                                $scope.getHistory();
                            }
                        });
                        
                        $scope.readCmdQPSChart = chart_qps;
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
