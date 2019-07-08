'use strict';

var proxy = angular.module('proxyInfo', ["ui.bootstrap", "ngRoute"]);

$(document).ready(function() {
	$("#all_response_cmd").select2();
});

var g_refresh_interval  = 0;

function getUrlParams() {
	var url = window.location.href;
	var codis_position = url.indexOf("codis=");
	var proxy_position = url.indexOf("token=");
	var interval_position = url.indexOf("interval=");
	var codis = url.substring(codis_position+6, proxy_position-2);
	var proxy = url.substring(proxy_position+6, interval_position-1);
	var interval = url.substring(interval_position+9);
	g_refresh_interval  = interval;

	return {Codis:codis, Proxy:proxy, Interval:interval};
}

function changeGlobalInterval(interval) {
	g_refresh_interval = interval;
}

function getGlobalInterval() {
	return g_refresh_interval;
}

function showStats(stats) {
	document.getElementById('stats_info').innerHTML = "<p>" + stats + "</p>";
}

function triggerCodisTree() {
	parent.triggerIframe();
}

proxy.factory('redirectionUrl', [function() {
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

proxy.config(['$interpolateProvider',
	function ($interpolateProvider) {
		$interpolateProvider.startSymbol('[[');
		$interpolateProvider.endSymbol(']]');
	}
]);

proxy.config(['$httpProvider', function ($httpProvider) {
	$httpProvider.defaults.useXDomain = true;
	delete $httpProvider.defaults.headers.common['X-Requested-With'];
	$httpProvider.interceptors.push('redirectionUrl');
}]);

proxy.config(['$routeProvider', function ($routeProvider) {  
	;  
}]);

proxy.run(['$rootScope', '$window', '$location', '$log', function ($rootScope, $window, $location, $log) {  
	var locationChangeSuccessOff = $rootScope.$on('$locationChangeSuccess', locationChangeSuccess); 
  
	function locationChangeSuccess(event) {  
		var $scope = window.scope;  
		var param = getUrlParams();
		$scope.selectProxyInstance(param.Proxy);
		scrollTo(0,0);
	} 
}]); 

proxy.controller('MainProxyCtrl', ['$scope', '$http', '$uibModal', '$timeout',
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
		$scope.param = getUrlParams();
		$scope.codis_name = $scope.param.Codis;
		$scope.chart_ops = newChatsOpsConfig();
		var qps_chart = echarts.init(document.getElementById('chart_ops'));
		qps_chart.setOption(newChatsOpsConfigEcharts("", 1));
		$scope.qps_arr = [];
		$scope.refresh_interval = 3;
		$scope.influxdb_period_s = 1;
		$scope.proxyHistoryChart = "";
		$scope.proxy_cmd_response_time_list = [];
		$scope.proxy_cmd_response_chart_list = [];
		$scope.history_chart_map = {};
		$scope.Memory = "-";
		$scope.more_tp_cmd = "tp999";
		$scope.select_response_cmd = "";
		window.onresize = function(){
			qps_chart.resize();
			if ($scope.proxyHistoryChart != "") {
				$scope.proxyHistoryChart.resize();
			}
			for (var key in $scope.history_chart_map) {
				$scope.history_chart_map[key].resize();
			}
		}

		$scope.triggerCodisTree = function () {
			triggerCodisTree();
			window.onresize();
		}
		
		$scope.resetProxyInfo = function () {
			$scope.proxy_info = "NA";
			$scope.chart_ops.series[0].data = [];
			$scope.qps_arr = [];
			$scope.proxy_start_time = "NA";
			$scope.proxy_stats = "NA";

			$scope.stats_info = "NA";
			$scope.models_info = "NA"
			showStats($scope.stats_info);
		}

		$scope.getHistoryCmdResponseList = function () {
			var codis_name = $scope.codis_name;
			var xauth = genXAuth(codis_name);
			var table_name = getTableName("proxy_", $scope.proxy_info.proxy_addr);
			table_name = table_name + "_cmd_info";
			var sql_command = getShowSqlCommand(table_name);
			var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, codis_name);
			$scope.select_response_cmd = "";
			if ($scope.proxy_cmd_response_chart_list.length > 0) {
				$scope.select_response_cmd = $scope.proxy_cmd_response_chart_list[0];
			}
			$scope.proxy_cmd_response_time_list = [];
			$scope.proxy_cmd_response_chart_list = [];
			$http.get(url).then(function (resp) {
				if (resp.data.Results && resp.data.Results[0].Series) {
					$scope.proxy_cmd_response_time_list = [];
					var cmd_list = resp.data.Results[0].Series[0].values;
					var all_cmd_index = -1;
					for (var i=0; i<cmd_list.length; i++) {
						$scope.proxy_cmd_response_time_list.push(cmd_list[i][1]);
						if (cmd_list[i][1] == $scope.select_response_cmd) {
							$scope.proxy_cmd_response_chart_list.push(cmd_list[i][1]);
						}
						if (cmd_list[i][1] == "ALL") {
							all_cmd_index = i;
						}
					}
					if ($scope.proxy_cmd_response_chart_list.length <= 0 && all_cmd_index >= 0) {
						$scope.proxy_cmd_response_chart_list.push(cmd_list[all_cmd_index][1]);
					}
				}
				if ($scope.proxy_cmd_response_chart_list.length > 0) {
					$scope.getHistory("response");
				} else {
					$scope.getHistory("");
				}
			},function (failedResp) {
				// 请求失败执行代码 
				$scope.getHistory("");   
			});
		}

		$scope.selectProxyInstance = function (selected) {
			if ($scope.proxy_info && selected==$scope.proxy_info.token) {
				return ;
			}
			$scope.resetProxyInfo(); 
			var param = getUrlParams();
			var codis_name = param.Codis;
			$scope.refresh_interval = param.Interval;
			var url = concatUrl("/topom", codis_name);

			$http.get(url).then(function (resp) {
				var overview = resp.data;
				$scope.proxy_info = "NA";
				$scope.proxy_stats = "NA";
				var influxdb_period = overview.config.metrics_report_influxdb_period;
				var influxdb_period_unit = influxdb_period.charAt(influxdb_period.length - 1);
				$scope.influxdb_period_s = parseInt(influxdb_period.substring(0, influxdb_period.length - 1));
				if (influxdb_period_unit == "m") {
					$scope.influxdb_period_s *= 60;
				}

				$scope.updateProxyStats(overview.stats);
				$scope.getProxyInfo();
				$scope.fillHistory();
				$scope.show_more_info = false;
				$('#more_info').hide();
			},function (failedResp) {
				// 请求失败执行代码
				if (selected == getUrlParams().Proxy) {
					alert("get " + codis_name + " info failed!");
					clearHistorychart("historyCharts");   
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
			$scope.getProxyStatsByType();
		}

		$scope.stats_type = "info";
        $scope.getProxyStatsByType = function () {
        	if ($scope.stats_type == "info") {
        		$scope.getProxyInfo();
        	} else if ($scope.stats_type == "slowlog") {
        		$scope.getProxySlowLog();
        	}
        }

		$scope.getProxyInfo = function() {
			$("#slowlog").removeClass("active");
            $("#proxyinfo").addClass("active");
            $("#downloadslowlog").hide()
			$scope.models_info = JSON.stringify($scope.proxy_info, null, '\t').replace(/\n/g, '<br>').replace(/\t/g, '&nbsp;&nbsp;&nbsp;&nbsp;');
			$scope.stats_info = JSON.stringify($scope.proxy_stats, null, '\t').replace(/\n/g, '<br>').replace(/\t/g, '&nbsp;&nbsp;&nbsp;&nbsp;');
			var content = "<h4 style='padding-left:0px; padding-bottom:6px;'><b><span>Models </span></b></h4>" + $scope.models_info;
			content += "<h4 style='padding-left:0px; padding-bottom:6px;'><b><span>Stats </span></b></h4>" + $scope.stats_info;
			content = "<pre>" + content + "</pre>";
			showStats(content);
		}

		$scope.updateProxyStats = function (codis_stats) {
			$scope.param = getUrlParams();
			$scope.proxy_list = codis_stats.proxy.models;

			var overview = codis_stats;
			var all_proxy_models = overview.proxy.models;
			var all_proxy_stats  = overview.proxy.stats;
			$scope.proxy_stats = "NA";
			$scope.proxy_info = "NA";
			$scope.proxy_token = $scope.param.Proxy;

			for (var i=0; i<all_proxy_models.length; i++) {
				if (($scope.proxy_token == all_proxy_models[i].token)) {
					$scope.proxy_info = all_proxy_models[i];
					if (all_proxy_stats[$scope.proxy_info.token]) {
						$scope.proxy_stats = all_proxy_stats[$scope.proxy_info.token];
					}
					break ;
				}
			}

			if ($scope.proxy_info=="NA" || $scope.proxy_stats=="NA" || $scope.proxy_stats.timeout || $scope.proxy_stats.error) {
				//historyHide();
				return ;
			}

			$scope.proxy_start_time = $scope.proxy_info.start_time.substring(0,19);
			var new_date = new Date(parseInt($scope.proxy_stats.unixtime) * 1000);
			$scope.proxy_stats.unixtime = new_date.Format("yyyy-MM-dd hh:mm:ss");

			if ($scope.proxy_stats.stats && $scope.proxy_stats.stats.rusage && $scope.proxy_stats.stats.rusage.mem) {
				$scope.Memory = humanSize($scope.proxy_stats.stats.rusage.mem);
			} else {
				$scope.Memory = "-";
			}

			var ops_array = $scope.chart_ops.series[0].data;
			if (ops_array.length >= 20) {
				ops_array.shift();
			}
			var proxy_qps = $scope.proxy_stats.stats.ops.qps;              
			ops_array.push({x: new Date(), y: proxy_qps});
			$scope.chart_ops.series[0].data = ops_array;

			if ($scope.qps_arr.length >= 20) {
				$scope.qps_arr.shift();
			}
			$scope.qps_arr.push([new Date(),proxy_qps]);
			qps_chart.setOption({
				series: [{
					data: $scope.qps_arr
				}]
			});

			if (!($("#chart_container_div", window.parent.document).is(":hidden"))) {
				if (parent.full_screen_chart_owner == "proxy_qps_charts") {
					parent.full_screen_chart_for_iframe.setOption({
						series: [{
							data: $scope.qps_arr
						}]
					});
				}
			}
		}

		$scope.changeInterval = function(interval) {
			changeGlobalInterval(interval);
		}

		$scope.showProxyQpsChartContainer = function() {
			$("#chart_container_div", window.parent.document).show();
			parent.full_screen_chart_owner = "proxy_qps_charts";
			parent.full_screen_chart_for_iframe.clear();
			parent.full_screen_chart_for_iframe.resize();
			parent.full_screen_chart_for_iframe.setOption(qps_chart.getOption());
			$(window).resize(function(){
				parent.full_screen_chart_for_iframe.resize();
			});
		}

		$scope.refreshProxyStats = function () {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/stats/" + xauth, codis_name);
				$http.get(url).then(function (resp) {
					if ($scope.codis_name != codis_name) {
						return ;
					}
					$scope.updateProxyStats(resp.data);
				});
			}
		}

		//operate of history charts
		$scope.search_type = "ops_qps";
		$scope.search_id = "Qps";
		$scope.data_unit = "count(次)/s";
		$scope.time_id = "";

		$scope.fillHistory = function () {   
			setColor("", $scope.search_id);
			setColor("", $scope.time_id);
			initTimer(6, 0);
			setColor($scope.time_id, "");
			if ($scope.search_type == "ops_qps") {
				$scope.getHistoryCmdResponseList();
			} else if ($scope.search_type != "more") {
				$scope.getHistory("");
			} else {
				$scope.getHistory("more");
			}
		}

		$scope.setType = function (id, type, count) {
			if ($scope.search_type == type) {
				return ;
			}
			$scope.legendSelectedAll = false;
			setColor($scope.search_id, id);
			$scope.search_type = type;
			$scope.search_id = id;
			$scope.data_unit = count;
			$scope.getHistoryByType();
		}

		$scope.quickSearch = function (start, end, timeId) {
			setColor($scope.time_id, timeId);
			$scope.time_id = timeId;
			initTimer(start, end);
			//$scope.getHistory("all");
			$scope.getHistoryByType();
		}

		$scope.getHistoryByType = function () {
			if ($scope.search_type == "ops_qps") {
				$scope.getHistoryCmdResponseList();
			} else if ($scope.search_type == "more") {
				$scope.getHistory("more");
			} else {
				$scope.getHistory("");
			}
		}

		$scope.showProxyHistoryTimeout = function () {
			$scope.timeout_show = !$scope.timeout_show;
			
			$("#all_response_cmd").select2();
			$scope.getHistory("response");
		}

		$scope.selectHistoryCmd = function (cmd) {
			$scope.proxy_cmd_response_chart_list = [];
			$scope.proxy_cmd_response_chart_list.push(cmd);
			$scope.getHistory("response");
		}

		$scope.refreshHistory = function () {
			if ($scope.search_type == "ops_qps") {
				$scope.getHistoryCmdResponseList();
			} else if ($scope.search_type != "more") {
				$scope.getHistory("");
			} else {
				$scope.getHistory("more");
			}
		}

		$scope.getHistory = function (type) {
			var codis_name = $scope.codis_name;
			var xauth = genXAuth(codis_name);
			var start_seconds = getTimeById('start_time');
			var end_seconds = getTimeById('end_time');
			if (start_seconds >= end_seconds) {
				alert("开始时间必须小于结束时间，请重新设置！");
				return ;
			}
			
			if (type == "all" || type == "") {
				var table_name = getTableName("proxy_", $scope.proxy_info.proxy_addr);
				var chart_id = "historyCharts";
				var search_type = $scope.search_type;
				var sql_command = getSelectSqlCommand(start_seconds, end_seconds, table_name, search_type, "", $scope.influxdb_period_s);
				var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, codis_name);
				$scope.getProxyHistoryChart(chart_id, search_type, url);
			}

			if (type == "more") {
				$scope.showAllCmdInfo(codis_name, xauth, start_seconds, end_seconds);
				return;
			}

			if (type == "change_tp_cmd") {
				$scope.getAllCmdInfoChart("proxy_all_cmd_tp999", codis_name, xauth, start_seconds, end_seconds, $scope.more_tp_cmd);
				return;
			}

			if ((type == "all" || type == "response") && $scope.search_type == "ops_qps") {
				for (var i = 0; i < $scope.proxy_cmd_response_chart_list.length; i++) {
					var table_name = getTableName("proxy_", $scope.proxy_info.proxy_addr);
					table_name = table_name + "_cmd_info";
					var search_type = "qps,tp90,tp99,tp999,tp9999,tp100,avg,fails,redis_errtype";
					var sql_command = getSelectSqlCommand(start_seconds, end_seconds, table_name, search_type, "cmd_name='" + $scope.proxy_cmd_response_chart_list[i] + "'", $scope.influxdb_period_s);
					var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, codis_name);
					
					$scope.getProxyCmdHistoryChart(search_type, url, $scope.proxy_cmd_response_chart_list[i]);

				}
			}
		}

		$scope.getProxyHistoryChart = function (chart_id, search_type, url) {
			$http.get(url).then(function (resp) {
				if (resp.data.Results && resp.data.Results[0].Series) {
					var chart = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, search_type);
					if (chart_id == "historyCharts") {
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
								$scope.getHistoryByType();
							}
						});
					}
					$scope.history_chart_map[chart_id] = chart;
				} else {
					alert(search_type + "数据获取失败！");
					clearHistorychart(chart_id);
					return;
				}
			},function (failedResp) {
				// 请求失败执行代码
				alert(search_type + "数据获取失败！");
				clearHistorychart(chart_id);
				return;
			});
		}

		$scope.showAllCmdInfo = function (codis_name, xauth, start_seconds, end_seconds) {
			$scope.getAllCmdInfoChart("historyCharts", codis_name, xauth, start_seconds, end_seconds, "qps");
			$scope.getAllCmdInfoChart("proxy_all_cmd_tp999", codis_name, xauth, start_seconds, end_seconds, $scope.more_tp_cmd);
		}

		$scope.more_qps_select_legend = "ALL";
		$scope.more_tp_select_legend = "ALL";
		$scope.getAllCmdInfoChart = function (chart_id, codis_name, xauth, start_seconds, end_seconds, type) {
			var table_name = getTableName("proxy_", $scope.proxy_info.proxy_addr);
			table_name = table_name + "_cmd_info";
			var search_type = "";
			var sql_command = getSelectSqlCommand(start_seconds, end_seconds, table_name, type, "", $scope.influxdb_period_s);
			sql_command = sql_command + ",cmd_name";
			var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, codis_name);
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
					var chart = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, search_type, "", legend_select_list);	
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
							$scope.getHistory("more");
						}
					});
					$scope.history_chart_map[chart_id] = chart;
				} else {
					alert("All Cmd TP999: 数据获取失败！");
					clearHistorychart(chart_id);
					return;
				}
			},function (failedResp) {
				// 请求失败执行代码
				alert("All Cmd TP999: 数据获取失败！");
				clearHistorychart(chart_id);
				return;
			});
		}

		$scope.getProxyCmdHistoryChart = function (search_type, url, cmd_name) {
			var chart_id = "";
			$http.get(url).then(function (resp) {
				if (resp.data.Results && resp.data.Results[0].Series) {
					
					chart_id = "historyCharts";
					$scope.history_chart_map[chart_id] = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, "qps", search_type);
					$scope.addDataZoomEvent($scope.history_chart_map[chart_id]);

					chart_id = "proxy_cmd_reponse_time_" + cmd_name;
					$scope.history_chart_map[chart_id] = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, "tp90,tp99,tp999,tp9999,tp100,avg",  search_type, "tp999,avg");
					$scope.addDataZoomEvent($scope.history_chart_map[chart_id]);

					chart_id = "proxy_cmd_fails_" + cmd_name;
					$scope.history_chart_map[chart_id] = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, "fails,redis_errtype", search_type);	
					$scope.addDataZoomEvent($scope.history_chart_map[chart_id]);
				} else {
					alert(search_type + "数据获取失败！");
					clearHistorychart("historyCharts");
					clearHistorychart("proxy_cmd_reponse_time_" + cmd_name);
					clearHistorychart("proxy_cmd_fails_" + cmd_name);
					return;
				}
			},function (failedResp) {
				// 请求失败执行代码
				alert(search_type + "数据获取失败！");
				clearHistorychart("historyCharts");
				clearHistorychart("proxy_cmd_reponse_time_" + cmd_name);
				clearHistorychart("proxy_cmd_fails_" + cmd_name);
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
					$scope.getHistoryByType();
				}
			});
		}

		/*$scope.get_proxy_or_history_timeout = function(id, url, search_type, chart_type){
			$http.get(url).then(function (resp) {
				var table_name = "";
				if (resp.data.Results && resp.data.Results[0].Series) {
					table_name = resp.data.Results[0].Series[0].name;
					for (var i=1; i<resp.data.Results[0].Series.length; i++) {
						table_name += "," + resp.data.Results[0].Series[i].name;
						for (var j=0; j<resp.data.Results[0].Series[0].values.length; j++) {
							resp.data.Results[0].Series[0].values[j].push(resp.data.Results[0].Series[i].values[j][1]);
						}
					}
					var chart = showHistory("_history_chart" + id, resp.data.Results[0].Series[0], $scope.search_type, table_name);
					if (chart_type == "proxy"){
						$scope.history_proxy_chart_map[id] = chart;
					} else if (chart_type == "server") {
						$scope.history_server_chart_map[id] = chart;
					} else {
						$scope.history_cache_chart_map[id] = chart;
					}
				} else {
					alert(search_type + "数据获取失败！");
					clearHistorychart(chart_type + "_history_chart" + id);   
				}
			},function (failedResp) {
				// 请求失败执行代码
				clearHistorychart(chart_type + "_history_chart" + id);    
			});
		}*/
		$scope.legendSelectedAll = false;
		$scope.selectAllCmd = function() {
			$scope.selectAllLegend("historyCharts");
			$scope.selectAllLegend("proxy_all_cmd_tp999");
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
			} else {
				for (var i=0; i<legend.length; i++) {
					selected_list[legend[i]] = true;
				}
			}
			
			$scope.history_chart_map[chart_id].setOption({
				legend:{
					data: legend,
					selected: selected_list,
					width: '85%',
				}
			});
		}

		$scope.showProxyHistoryChartContainer = function (chart_id_prefix, chart_id_cmd) {
			var chart_id = chart_id_prefix + chart_id_cmd;
			$("#chart_container_div", window.parent.document).show();
			parent.full_screen_chart_owner = "proxy_hitory_charts";
			parent.full_screen_chart_for_iframe.clear();
			parent.full_screen_chart_for_iframe.resize();
			var option = $scope.history_chart_map[chart_id].getOption();
			option.dataZoom = [
				{
					type: 'inside',
					show: false,
					xAxisIndex: [0],
				}
			];
			parent.full_screen_chart_for_iframe.setOption(option);
			$(window).resize(function(){
				parent.full_screen_chart_for_iframe.resize();
			});
		}

		$scope.getProxySlowLog = function () {
            var codis_name = $scope.codis_name;
            $("#proxyinfo").removeClass("active");
            $("#slowlog").addClass("active");
            $("#downloadslowlog").show();
            if (isValidInput(codis_name)) {
                var xauth = genXAuth(codis_name);
                var cmd = "xslowlog get 1000";
                var url = concatUrl("/api/topom/docmd/" + xauth + "/" + $scope.proxy_info.proxy_addr + "/" + cmd, codis_name);
                $http.get(url).then(function (resp) {
                    $scope.slow_log_resp = resp.data;
                    $scope.slow_log = resp.data;
                    if ($scope.slow_log == "") {
                    	$scope.slow_log = "no more response...";
                    }
                    $scope.slow_log = $scope.slow_log .replace(/ /g, '&nbsp;').replace(/\r\n/g, '<br>').replace(/\t/g, '&nbsp;&nbsp;&nbsp;&nbsp;');
                    $scope.slow_log = "<pre>" + $scope.slow_log + "</pre>";
                    showStats($scope.slow_log);
                });
            }
        }

        $scope.downloadInfo = function () {
        	var text = "";
        	var suffix = "";
        	if ($scope.stats_type == "info") {
        		text = JSON.stringify($scope.proxy_info, null, '\t') + "\r\n" + JSON.stringify($scope.proxy_stats, null, '\t');
        		suffix = "_info_";
        	} else {
        		text = $scope.slow_log_resp;
        		suffix = "_slowlog_";
        	}
        	var blob = new Blob([text], {type: "text/plain;charset=utf-8"});
        	var myDate = new Date();
        	var time = myDate.getFullYear().toString();
        	time += (myDate.getMonth()+1).toString() ;
        	time += myDate.getDate().toString() ;
        	time += myDate.getHours().toString() ;
        	time += myDate.getMinutes().toString() ;
        	time += myDate.getSeconds().toString() ;
			saveAs(blob, "proxy_" + $scope.proxy_info.proxy_addr + suffix + time + ".txt");
        }

		var ticker = 0;
		(function autoRefreshStats() {
			if (ticker >= $scope.refresh_interval) {
				ticker = 0;
				$scope.refreshProxyStats();
			}
			ticker++;
			$timeout(autoRefreshStats, 1000);
		}());
	}
])
;
