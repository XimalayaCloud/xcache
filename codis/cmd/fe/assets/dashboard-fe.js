'use strict';

var dashboard = angular.module('dashboard-fe', ["highcharts-ng", "ui.bootstrap", "ngRoute"]);
var full_screen_chart_for_iframe = "";
var full_screen_chart_owner = "";

$(document).ready(function() {
	$("#all_response_cmd").select2();
});

function openDialog(id) {
	$('.theme-popover-mask').fadeIn(10);
	$('#' + id).slideDown(20);
}
function closeDialog(id) {
	$('.theme-popover-mask').fadeOut(10);
	$('#' + id).slideUp(20);
}

var main_left = "200px";
var iframe_left = "230px";
function triggerCodisTree() {
	if (main_left == "200px") {
		$("#main").css("margin-left","0px");
		$("#everyCodisInfo").css("margin-left","15px");
		$("#indexInfo").css("margin-left","15px");
		$("#containerTable").css("left","0px");
		$("#sidebar").animate({width:"0px"}, 250);
		main_left = "0px";
	} else if (main_left == "0px") {
		$("#main").css("margin-left","200px");
		$("#everyCodisInfo").css("margin-left","45px");
		$("#indexInfo").css("margin-left","45px");
		$("#containerTable").css("left","230px");
		$("#sidebar").animate({width:"228px"}, 250);
		main_left = "200px";
	} 
	if (!($('#iframeTable').css('display') === 'none')) {
		if (iframe_left == "230px") {
			$("#iframeTable").animate({left:"0px"}, 50);
			iframe_left = "0px";
		} else if (iframe_left == "0px") {
			$("#iframeTable").animate({left:"230px"}, 50);
			iframe_left = "230px";
		} 
	}
}

function triggerCodisTreeSlow() {
	if (main_left == "200px") {
		$("#main").animate({marginLeft:"0px"}, 200);
		$("#everyCodisInfo").animate({marginLeft:"15px"}, 200);
		$("#indexInfo").animate({marginLeft:"15px"}, 200);
		$("#containerTable").animate({left:"0px"}, 200);
		$("#sidebar").animate({width:"0px"}, 200);
		main_left = "0px";
	} else if (main_left == "0px") {
		$("#main").animate({marginLeft:"200px"}, 200);
		$("#everyCodisInfo").animate({marginLeft:"45px"}, 200);
		$("#indexInfo").animate({marginLeft:"45px"}, 200);
		$("#containerTable").animate({left:"230px"}, 200);
		$("#sidebar").animate({width:"228px"}, 200);
		main_left = "200px";
	} 
}


function triggerIframe() {
	
	triggerCodisTree();
}

function getGlobalInterval() {
	return $("#infoContainer")[0].contentWindow.getGlobalInterval();
}

function getUrlParams() {
	var url = window.location.href;
	var codis_index = url.indexOf("#");
	var codis_name = url.substring(codis_index + 1);
	return codis_name;
}

function newTreeviewNode(codisname) {
	var node = {
		text: codisname,
		type: "codis",
		addr: "",
		href: "#" + codisname,
		state: {
			expanded: false,
		},
		nodes: [
			{
				text: 'Proxy',
				type: 'proxy',
				href: "#" + codisname,
				codisname: codisname,
				state: {
					expanded: false,
				},
				nodes: [],
			},
			{
				text: 'Group',
				type: 'group',
				href: "#" + codisname,
				codisname: codisname,
				state: {
					expanded: false,
				},
				nodes: [],
			}
		]
	};
	return node;
}

function qpsEchartLine(id) {
	this.id = id;
	this.data = [];
	this.source = "";
}

function renderSlotsCharts(slots_array) {
	var groups = {};
	var counts = {};
	var n = slots_array.length;
	for (var i = 0; i < n; i++) {
		var slot = slots_array[i];
		groups[slot.group_id] = true;
		if (slot.action.target_id) {
			groups[slot.action.target_id] = true;
		}
		if (counts[slot.group_id]) {
			counts[slot.group_id]++;
		} else {
			counts[slot.group_id] = 1;
		}
	}
	var series = [];
	for (var g in groups) {
		var xaxis = 2;
		if (g == 0) {
			xaxis = 0;
		}
		var s = {name: 'Group-' + g + ':' + (counts[g] == undefined ? 0 : counts[g]), data: [], group_id: g};
		for (var beg = 0, end = 0; end <= n; end++) {
			if (end == n || slots_array[end].group_id != g) {
				if (beg < end) {
					s.data.push({x: xaxis, low: beg, high: end - 1, group_id: g});
				}
				beg = end + 1;
			}
		}
		xaxis = 1;
		for (var beg = 0, end = 0; end <= n; end++) {
			if (end == n || !(slots_array[end].action.target_id && slots_array[end].action.target_id == g)) {
				if (beg < end) {
					s.data.push({x: xaxis, low: beg, high: end - 1, group_id: g});
				}
				beg = end + 1;
			}
		}
		s.data.sort(function (a, b) {
			return a.x - b.x;
		});
		series.push(s);
	}
	series.sort(function (a, b) {
		return a.group_id - b.group_id;
	});
	
	new Highcharts.Chart({
		chart: {
			renderTo: 'slots_charts',
			type: 'columnrange',
			inverted: true,
		},
		title: {
			style: {
				display: 'none',
			}
		},
		xAxis: {
			categories: ['Offline', 'Migrating', 'Default'],
			min: 0,
			max: 2,
		},
		yAxis: {
			min: 0,
			max: 1024,
			tickInterval: 64,
			title: {
				style: {
					display: 'none',
				}
			},
		},
		legend: {
			enabled: true
		},
		plotOptions: {
			columnrange: {
				grouping: false
			},
			series: {
				animation: false,
				events: {
					legendItemClick: function () {
						return false;
					},
				}
			},
		},
		credits: {
			enabled: false
		},
		tooltip: {
			formatter: function () {
				switch (this.point.x) {
				case 0:
					return '<b>Slot-[' + this.point.low + "," + this.point.high + "]</b> are <b>Offline</b>";
				case 1:
					return '<b>Slot-[' + this.point.low + "," + this.point.high + "]</b> will be moved to <b>Group-[" + this.point.group_id + "]</b>";
				case 2:
					return '<b>Slot-[' + this.point.low + "," + this.point.high + "]</b> --> <b>Group-[" + this.point.group_id + "]</b>";
				}
			}
		},
		series: series,
	});
	//判断有几个正在使用的group组
	var used_group = [];
	for (var i=0; i<series.length; i++) {
		if (i==0 && series[i].group_id==0) {
			continue;
		}
		used_group.push(series[i].group_id);
	}
	return used_group;
}

function processProxyStats(codis_stats) {
	var proxy_array = codis_stats.proxy.models;
	var proxy_stats = codis_stats.proxy.stats;
	var qps = 0, sessions = 0;
	for (var i = 0; i < proxy_array.length; i++) {
		var p = proxy_array[i];
		var s = proxy_stats[p.token];
		p.sessions = "NA";
		p.commands = "NA";
		p.switched = false;
		p.primary_only = false;
		p.qps = 0;
		if (!(p.jodis_path)) {
			p.jodis_path = "";
		} else {
			var lastindex = p.jodis_path.lastIndexOf('/');
			p.jodis_path = p.jodis_path.substring(0, lastindex);
		}
		if (!s) {
			p.status = "PENDING";
		} else if (s.timeout) {
			p.status = "TIMEOUT";
		} else if (s.error) {
			p.status = "ERROR";
		} else {
			if (s.stats.online) {
				p.sessions = "total=" + s.stats.sessions.total + ",alive=" + s.stats.sessions.alive;
				p.commands = "total=" + s.stats.ops.total + ",fails=" + s.stats.ops.fails;
				if (s.stats.ops.redis != undefined) {
					p.commands += ",rsp.errs=" + s.stats.ops.redis.errors;
				}
				p.commands += ",qps=" + s.stats.ops.qps;
				p.status = "HEALTHY";
			} else {
				p.status = "PENDING";
			}
			if (s.stats.sentinels != undefined) {
				if (s.stats.sentinels.switched != undefined) {
					p.switched = s.stats.sentinels.switched;
				}
			}
			if (s.stats.backend != undefined) {
				if (s.stats.backend.primary_only != undefined) {
					p.primary_only = s.stats.backend.primary_only;
				}
			}
			p.qps = s.stats.ops.qps;
			qps += s.stats.ops.qps;
			sessions += s.stats.sessions.alive;
		}
	}
	return {proxy_array: proxy_array, qps: qps, sessions: sessions};
}

function processGroupStats(codis_stats) {
	var group_array = codis_stats.group.models;
	var group_stats = codis_stats.group.stats;
	var keys = 0, cache_memory = 0, cache_hitrate = 0, pika_memory = 0, dbsize = 0;
	var codis_memory = 0, codis_ssd = 0;
	var all_read_cmd = 0, hits = 0;
	var have_cache = false;
	var is_redis_group = false;
	var dbkeyRegexp = /db\d+/;
	var cachekeyRegexp = /cache\d+/;
	var cache_stats_map = {};
	for (var i = 0; i < group_array.length; i++) {
		var g = group_array[i];
		g.server_type = "null";
		if (g.promoting.state) {
			g.ispromoting = g.promoting.state != "";
			if (g.promoting.index) {
				g.ispromoting_index = g.promoting.index;
			} else {
				g.ispromoting_index = 0;
			}
		} else {
			g.ispromoting = false;
			g.ispromoting_index = -1;
		}
		g.runids = {}
		g.canremove = (g.servers.length == 0);
		for (var j = 0; j < g.servers.length; j++) {
			var x = g.servers[j];
			var s = group_stats[x.server];
			x.keys = "";
			x.memory = "NA";
			x.cache_hitrate = 0;
			x.cache_memory = "NA";
			x.maxmem = "NA";
			x.master = "NA";
			x.type = "NA";
			if (j == 0) {
				x.master_expect = "NO:ONE";
			} else {
				x.master_expect = g.servers[0].server;
			}
			if (!s) {
				x.status = "PENDING";
			} else if (s.timeout) {
				x.status = "TIMEOUT";
			} else if (s.error) {
				x.status = "ERROR";
			} else {
				for (var field in s.stats) {
					if (dbkeyRegexp.test(field)) {
						var v = parseInt(s.stats[field].split(",")[0].split("=")[1], 10);
						if (j == 0) {
							keys += v;
						}
						x.keys = x.keys  + field + ": " + s.stats[field] + "\n";
					}
				}
				for (var field in s.stats) {
					if (cachekeyRegexp.test(field)) {
						var cache_stats_arr = s.stats[field].split(",");
						var cache_ip = cache_stats_arr[0].split("=")[1];
						var cache_port = cache_stats_arr[1].split("=")[1];
						var cache_addr = cache_ip + ":" + cache_port;
						var cache_stats = cache_stats_arr[2].split("=")[1];
						cache_stats_map[cache_addr] = cache_stats;	
					}
				}
				if (s.stats["redis_version"]) {
					if (j == 0) {
						g.server_type = "redis";
						if (i == 0) {
							have_cache = false;
							is_redis_group = true;
						}
					}
					x.type = "redis";
					x.version = s.stats["redis_version"];
				} else if (s.stats["pika_version"]) {
					if (j == 0) {
						g.server_type = "pika";
						var pika_version = s.stats["pika_version"];
						if (i == 0 && pika_version != "") {
							is_redis_group = false;
							if(pika_version != "3.5-2.2.6" && parseInt(pika_version.slice(0,1)) >= 3) {
								if (s.stats["cache_keys"]) {
									have_cache = true;
								} else {
									have_cache = false;
								}
							} else {
								have_cache = false;
							}
						}
					}
					x.type = "pika";
					x.version = s.stats["pika_version"];
				}

				if (s.stats["used_memory"]) {
					var v = parseInt(s.stats["used_memory"], 10);
					x.memory = humanSize(v);
					if (j == 0) {
						pika_memory += v;
					}

					codis_memory += v;
				}
				if (s.stats["cache_memory"]) {
					var v = parseInt(s.stats["cache_memory"], 10);
					if (j == 0) {
						cache_memory += v;
					}
					x.cache_memory = humanSize(v);
					codis_memory += v;
				}
				if (s.stats["hitratio_all"]) {
					var v = s.stats["hitratio_all"];
					if (j == 0) {
						if (s.stats["all_cmds"]) {
							all_read_cmd += parseInt(s.stats["all_cmds"], 10);
						}
						if (s.stats["hits"]) {
							hits += parseInt(s.stats["hits"], 10);
						}
					}
					x.cache_hitrate = v;
				}
				if (s.stats["maxmemory"]) {
					var v = parseInt(s.stats["maxmemory"], 10);
					if (j == 0) {
						dbsize += v;
					}
					if (v == 0 && s.stats["redis_version"]) {
						x.maxmem = "INF."
					} else {
						x.maxmem = humanSize(v);
					}

					codis_ssd += v;
				}
				if (s.stats["master_addr"]) {
					x.master = s.stats["master_addr"] + ":" + s.stats["master_link_status"];
				} else {
					x.master = "NO:ONE";
				}

				if (j == 0) {
					x.master_status = (x.master == "NO:ONE");
				} else {
					x.master_status = (x.master == g.servers[0].server + ":up");
				}

				if (s.stats["binlog_offset"]) {
					x.binlog_offset = "binlog_offset: " + s.stats["binlog_offset"];
				} else {
					x.binlog_offset = "";
				}

				if (s.stats["is_slots_reloading"]) {
					x.is_slots_reloading = s.stats["is_slots_reloading"];
				} else {
					x.is_slots_reloading = "";
				}

				if (s.stats["is_slots_deleting"]) {
					x.is_slots_deleting = s.stats["is_slots_deleting"];
				} else {
					x.is_slots_deleting = "";
				}

				if (s.stats["is_compact"]) {
					x.is_compact = s.stats["is_compact"];
				} else {
					x.is_compact = "";
				}

				if (s.stats["cache_memory"]) {
					x.cache_info = "cache: mem=" + humanSize(s.stats["cache_memory"]) + ",";
				} else {
					x.cache_info = "cache: mem=-,";
				}

				if (s.stats["cache_keys"]) {
					x.cache_info += "keys=" + s.stats["cache_keys"] + ",";
				} else {
					x.cache_keys += "keys=-,";
				}

				if (s.stats["hitratio_all"]) {
					x.cache_info += "hitrate=" + s.stats["hitratio_all"];
				} else {
					x.cache_info += "hitrate=-";
				}

				g.runids[s.stats["run_id"]] = x.server;

				if (j == 0 && s.stats["instantaneous_ops_per_sec"]) {
					x.qps = s.stats["instantaneous_ops_per_sec"];
				} else {
					x.qps = '';
				}
			}
			if (g.ispromoting) {
				x.canremove = false;
				x.canpromote = false;
				x.canslaveof = "";
				x.actionstate = "";
				x.ispromoting = (j == g.ispromoting_index);
			} else {
				x.canremove = (j != 0 || g.servers.length <= 1);
				x.canpromote = j != 0;
				x.ispromoting = false;
			}
			if (x.action.state) {
				if (x.action.state != "pending") {
					x.canslaveof = "create";
					x.actionstate = x.action.state;
				} else {
					x.canslaveof = "remove";
					x.actionstate = x.action.state + ":" + x.action.index;
				}
			} else {
				x.canslaveof = "create";
				x.actionstate = "";
			}
			x.server_text = x.server;
		}
		/*for (var j = 0; j < g.caches.length; j++) {
			var x = g.caches[j];
			var s = group_stats[x.server];
			x.keys = "";
			x.memory = "NA";
			x.maxmem = "NA";
			x.master = "NA";
			x.type = "NA";
			if (!s) {
				x.status = "PENDING";
			} else if (s.timeout) {
				x.status = "TIMEOUT";
			} else if (s.error) {
				x.status = "ERROR";
			} else {
				for (var field in s.stats) {
					if (dbkeyRegexp.test(field)) {
						var v = parseInt(s.stats[field].split(",")[0].split("=")[1], 10);
						x.keys = x.keys  + field + ": " + s.stats[field] + "\n";
					}
				}
				x.action_stats = "offline";
				if (cache_stats_map[x.server]) {
					x.action_stats = cache_stats_map[x.server];
				}

				if (s.stats["redis_version"]) {
					x.type="redis";
				} else if (s.stats["pika_version"]) {
					x.type="pika";
				}
				if (s.stats["used_memory"]) {
					var v = parseInt(s.stats["used_memory"], 10);
					redis_memory += v;
					x.memory = humanSize(v);
				}
				if (s.stats["maxmemory"]) {
					var v = parseInt(s.stats["maxmemory"], 10);
					if (v == 0) {
						x.maxmem = "INF."
					} else {
						x.maxmem = humanSize(v);
					}
				}

				g.runids[s.stats["run_id"]] = x.server;

				if (s.stats["instantaneous_ops_per_sec"]) {
					x.qps = s.stats["instantaneous_ops_per_sec"];
				} else {
					x.qps = 0;
				}
			}
			x.canremove = true;
			x.server_text = x.server;
		}*/
	}
	if (all_read_cmd > 0) {
		cache_hitrate = (Math.round((hits / all_read_cmd) * 100 * 100)/100).toString() + "%";
	} else {
		cache_hitrate = "0%";
	}
	return {group_array: group_array, 
			keys: keys, cache_memory: 
			cache_memory,cache_hitrate: 
			cache_hitrate, pika_memory: 
			pika_memory, 
			dbsize: dbsize,
			codis_memory:codis_memory,
			codis_ssd:codis_ssd,
			have_cache:have_cache,
			is_redis_group:is_redis_group};
}

function processSentinels(codis_stats, group_stats, codis_name) {
	var ha = codis_stats.sentinels;
	var out_of_sync = false;
	var servers = [];
	if (ha && ha.model != undefined) {
		if (ha.model.servers == undefined) {
			ha.model.servers = []
		}
		for (var i = 0; i < ha.model.servers.length; i ++) {
			var x = {server: ha.model.servers[i]};
			var s = ha.stats[x.server];
			x.runid_error = "";
			if (!s) {
				x.status = "PENDING";
			} else if (s.timeout) {
				x.status = "TIMEOUT";
			} else if (s.error) {
				x.status = "ERROR";
			} else {
				x.masters = 0;
				x.masters_down = 0;
				x.slaves = 0;
				x.sentinels = 0;
				var masters = s.stats["sentinel_masters"];
				if (masters != undefined) {
					for (var j = 0; j < masters; j ++) {
						var record = s.stats["master" + j];
						if (record != undefined) {
							var pairs = record.split(",");
							var dict = {};
							for (var t = 0; t < pairs.length; t ++) {
								var ss = pairs[t].split("=");
								if (ss.length == 2) {
									dict[ss[0]] = ss[1];
								}
							}
							var name = dict["name"];
							if (name == undefined) {
								continue;
							}
							if (name.lastIndexOf(codis_name) != 0) {
								continue;
							}
							if (name.lastIndexOf("-") != codis_name.length) {
								continue;
							}
							x.masters ++;
							if (dict["status"] != "ok") {
								x.masters_down ++;
							}
							x.slaves += parseInt(dict["slaves"]);
							x.sentinels += parseInt(dict["sentinels"]);
						}
					}
				}
				x.status_text = "masters=" + x.masters;
				x.status_text += ",down=" + x.masters_down;
				var avg = 0;
				if (x.slaves == 0) {
					avg = 0;
				} else {
					avg = Number(x.slaves) / x.masters;
				}
				x.status_text += ",slaves=" + avg.toFixed(2);
				if (x.sentinels == 0) {
					avg = 0;
				} else {
					avg = Number(x.sentinels) / x.masters;
				}
				x.status_text += ",sentinels=" + avg.toFixed(2);

				if (s.sentinel != undefined) {
					var group_array = group_stats.group_array;
					for (var t in group_array) {
						var g = group_array[t];
						var d = s.sentinel[codis_name + "-" + g.id];
						var runids = {};
						if (d != undefined) {
							if (d.master != undefined) {
								var o = d.master;
								runids[o["runid"]] = o["ip"] + ":" + o["port"];
							}
							if (d.slaves != undefined) {
								for (var j = 0; j < d.slaves.length; j ++) {
									var o = d.slaves[j];
									runids[o["runid"]] = o["ip"] + ":" + o["port"];
								}
							}
						}
						for (var runid in runids) {
							if (g.runids[runid] === undefined) {
								x.runid_error = "[+]group=" + g.id + ",server=" + runids[runid] + ",runid="
									+ ((runid != "") ? runid : "NA");
							}
						}
						for (var runid in g.runids) {
							if (runids[runid] === undefined) {
								x.runid_error = "[-]group=" + g.id + ",server=" + g.runids[runid] + ",runid=" + runid;
							}
						}
					}
				}
			}
			servers.push(x);
		}
		out_of_sync = ha.model.out_of_sync;
	}
	var masters = "NA";
	if (ha && ha.masters) {
		masters = ha.masters;
	} else {
		masters = {};
	}
	return {servers:servers, masters:masters, out_of_sync: out_of_sync}
}

function getInstanceInfo(codis_stats) {
	var server_num = 0;
	var machine = [];
	for (var i = 0; i < codis_stats.group.models.length; i++) {
		server_num = server_num + codis_stats.group.models[i].servers.length;
		for (var j = 0; j < codis_stats.group.models[i].servers.length; j++) {
			var server = codis_stats.group.models[i].servers[j].server;
			var ip = server.slice(0, server.indexOf(":"));
			var exist = false;
			for (var k=0; k<machine.length; k++) {
				if (machine[k] == ip) {
					exist = true;
					break;
				}
			}
			if (!exist) {
				machine.push(ip);
			}
		}
	}
	return {server_num:server_num, machine_num:machine.length};
}

/*function getslotsCacheMap(codis_stats, group_array) {
	for (var i = 0; i < group_array.length; i++) {
		for (var j = 0; j < group_array[i].caches.length; j++) {
			group_array[i].caches[j].num_slots = 0;
		}
	}

	for (var i = 0; i < codis_stats.slots.length; i++) {
		var gid = codis_stats.slots[i].group_id;
		var cid = codis_stats.slots[i].cache_id;
		for (var j = 0; j < group_array.length; j++) {
			if (group_array[j].id == gid) {
				for (var k = 0; k < group_array[j].caches.length; k++) {
					if (group_array[j].caches[k].id == cid) {
						group_array[j].caches[k].num_slots++;	
					}
				}
			}
		}	
	}
	return group_array;
}*/

function getProxyNum(codis_stats) {
	return codis_stats.proxy.models.length;
}

function getClientConn(codis_stats) {
	var p_array = codis_stats.proxy.models;
	var p_stats = codis_stats.proxy.stats;
	var total_count = 0;
	var alive_count = 0;
	for (var i = 0; i < p_array.length; i++) {
		//参考原生代码做错误处理
		var s = p_stats[p_array[i].token];
		if (s && !s.timeout && !s.error) {
			total_count += s.stats.sessions.total;
			alive_count += s.stats.sessions.alive;
		}
	}
	return {total:total_count, alive:alive_count};
}

function showEveryInfo() {
	$("#indexInfo").hide();
	$("#everyCodisInfo").show();
	$(".row").show();
	$("#navigationDashboardBtnGroup").show();
	$("#refreshParam").show();
	$("#groupIndex").hide();
	$("#proxyIndex").hide();
	$("#slotsIndex").hide();
	$("#sentinelsIndex").hide();
	$("#expansionIndex").hide();
	scrollTo(0,0);
}

function getUrlHost() {
	return window.location.host;
}

function compareStr(property, rev){
	return function(a,b){
		var value1 = a[property];
		var value2 = b[property];
		if (value1 == "-" && value2 != "-") {
			return rev * -1;
		}
		if (value1 != "-" && value2 == "-") {
			return rev * 1;
		} 
		if (value1 == "-" && value2 == "-") {
			return 0;
		}
		if (value1 > value2){
				return rev * 1;
		} else if (value1 < value2) {
				return rev * -1;
		} else {
				return 0;
		}
	}
}

function compareSize(property, rev){
	return function(a,b){
		var num1 = numSize(a[property]);
		var num2 = numSize(b[property]);
		if (num1 > num2){
				return rev * 1;
		} else if (num1 < num2) {
				return rev * -1;
		} else {
				return 0;
		};
	}
}

dashboard.factory('redirectionUrl', [function() {
	var redirectionUrl = {
		response: function(response) {
			if (response.headers("content-type").indexOf("html") >= 0){
				jumpToLink("/index");
			}
			return response;
		}
	};
	return redirectionUrl;
}]);

dashboard.config(['$interpolateProvider',
	function ($interpolateProvider) {
		$interpolateProvider.startSymbol('[[');
		$interpolateProvider.endSymbol(']]');
	}
]);

dashboard.config(function($httpProvider){ 
	$httpProvider.defaults.transformRequest = function(obj){  
		var str = [];  
		for(var p in obj){  
			str.push(encodeURIComponent(p) + "=" + encodeURIComponent(obj[p]));  
		}
		return str.join("&");
	}

	$httpProvider.defaults.headers.post = {
		'Content-Type': 'application/x-www-form-urlencoded'  
	}
});

dashboard.directive('repeatFinish',function($timeout){
	return {
		link: function(scope, element, attr){
			if(scope.$last == true){
				$timeout(function () {
					scope.$eval(attr.repeatFinish);
				});
			}
		}
	}
});

dashboard.config(['$httpProvider', function ($httpProvider) {
	$httpProvider.defaults.useXDomain = true;
	delete $httpProvider.defaults.headers.common['X-Requested-With'];
	$httpProvider.interceptors.push('redirectionUrl');
}]);  

dashboard.controller('MainCodisCtrl', ['$scope', '$http', '$uibModal', '$timeout',
	function ($scope, $http, $uibModal, $timeout) {
		Highcharts.setOptions({
			global: {
				useUTC: false,
			},
			exporting: {
				enabled: false,
			},
		});

		if (document.body.clientWidth > 1280) {
			;
		}
				 
		//获取所有单个集群信息
		$scope.getCodisInfo = function (selected, index) {
			var overview;
			var url = concatUrl("/topom", selected);
			$http.get(url).then(function (resp) {
				overview = resp.data;
				var codis_addr = overview.model.admin_addr;
				var product_name = overview.model.product_name;
				var coordinator = "[" + overview.config.coordinator_name + "]" + overview.config.coordinator_addr;
				var start_time = overview.model.start_time.substring(0,19);
				var group_stats = processGroupStats(overview.stats);
				var instance_info = getInstanceInfo(overview.stats);
				var server_num = instance_info.server_num;
				var machine_num = instance_info.machine_num;
				var used_memory = formatGB(group_stats.codis_memory);
				var server_ssd = formatGB(group_stats.codis_ssd);
				var total_keys = group_stats.keys;
				var redis_key = group_stats.keys.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
				var client_conn = getClientConn(overview.stats);
				var conn_state = client_conn.alive;
				var proxy_arr = overview.stats.proxy.models;
				var group_arr = overview.stats.group.models;

				if (group_stats.is_redis_group) {
					$scope.codis_redis_arr.push({
						product_name:product_name, 
						codis_addr:codis_addr, 
						coordinator:coordinator, 
						start_time:start_time,
						used_memory:used_memory,
						server_ssd:server_ssd,
						qps:0,
						tp99:0,
						tp999:0,
						tpavg:0,
						server_num:server_num,
						machine_num:machine_num,
						conn_state:conn_state
					});
					$scope.codis_redis_count_info.Codis_Num += 1;
					$scope.codis_redis_count_info.Memorys += group_stats.codis_memory;
					$scope.codis_redis_count_info.Servers += server_num;
					$scope.codis_redis_count_info.Machines += machine_num;
					$scope.codis_redis_count_info.Memorys_Human = humanSize($scope.codis_redis_count_info.Memorys);
					$scope.codis_redis_arr.sort(compareStr("product_name"));
				} else {
					$scope.codis_pika_arr.push({
						product_name:product_name, 
						codis_addr:codis_addr, 
						coordinator:coordinator, 
						start_time:start_time,
						used_memory:used_memory,
						server_ssd:server_ssd,
						qps:0,
						tp99:0,
						tp999:0,
						tpavg:0,
						server_num:server_num,
						machine_num:machine_num,
						conn_state:conn_state
					});
					$scope.codis_pika_count_info.Codis_Num += 1;
					$scope.codis_pika_count_info.SSD += group_stats.codis_ssd;
					$scope.codis_pika_count_info.Servers += server_num;
					$scope.codis_pika_count_info.Machines += machine_num;
					$scope.codis_pika_count_info.SSD_Human = humanSize($scope.codis_pika_count_info.SSD);
					$scope.codis_pika_arr.sort(compareStr("product_name"));
				}
				

				$scope.getAverageQps(product_name, codis_addr, group_stats.is_redis_group);
				$scope.getAverageTP(product_name, codis_addr, group_stats.is_redis_group);
				codis_treeview[index].addr = codis_addr;
				for (var i=0; i<proxy_arr.length; i++) {
					codis_treeview[index].nodes[0].nodes.push({
						text: proxy_arr[i].proxy_addr,
						type: 'proxynode',
						href: '#' + selected,
						codisname: selected,
						proxytoken: proxy_arr[i].token,
					});
				}

				for (var i=0; i<group_arr.length; i++) {
					for (var j=0; j<group_arr[i].servers.length; j++){
						codis_treeview[index].nodes[1].nodes.push({
							text: group_arr[i].servers[j].server + " [G" + group_arr[i].id + "]",
							serveraddr: group_arr[i].servers[j].server,
							type: 'servernode',
							href: '#' + selected,
							codisname: selected,
							groupid: group_arr[i].id,
						});
					}
				}
				$scope.getTreeview();
				$scope.recoverTreevireState();
			},function (failedResp) {
				// 请求失败
				$scope.adjustTreeviewSelected(index);
				codis_treeview[index] = {
					text: selected + " [down]",
					type: "codis",
					href: "#" + selected,
					color: "gray",
					state: {
						expanded: false,
					},
					nodes: [],
				};
				$scope.getTreeview();

				$scope.adjustTreeviewExpend(index);
				$scope.recoverTreevireState();
			});
		}

		$scope.codis_type = "redis";
		$scope.sort_type = "product_name";
		$scope.sort_style = 1;
		$scope.sortByType = function(codis_type, sort_type, order) {
			$scope.sort_style = 1;
			$scope.sort_type = sort_type;
			if (codis_type == "redis") {
				if (sort_type == "used_memory" || sort_type == "server_ssd") {
					$scope.codis_redis_arr.sort(compareSize(sort_type, order));
				} else {
					$scope.codis_redis_arr.sort(compareStr(sort_type, order));
				}
			} else if (codis_type == "pika") {
				if (sort_type == "used_memory" || sort_type == "server_ssd") {
					$scope.codis_pika_arr.sort(compareSize(sort_type, order));
				} else {
					$scope.codis_pika_arr.sort(compareStr(sort_type, order));
				}
			}
			$scope.codis_type = codis_type;
			$scope.sort_style = order;
		}

		$scope.getAverageQps = function(product_name, codis_addr, is_redis_group) {
			var xauth = genXAuth(product_name);
			var date = new Date();
			var end_seconds = date.getTime();
			var start_seconds = end_seconds - 1 * 24 * 60 * 60 * 1000;
			var table_name = getTableName("dashboard_", codis_addr);
			var search_type = "ops_qps";
			var sql_command = getSelectSqlCommand(start_seconds, end_seconds, table_name, search_type, "", 1);
			sql_command = sql_command.substring(0, sql_command.indexOf("group by time"));
			var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, product_name);
			
			$scope.remoteGetQps(product_name, url, is_redis_group);
		}

		$scope.getAverageTP = function(product_name, codis_addr, is_redis_group) {
			var xauth = genXAuth(product_name);
			var date = new Date();
			var end_seconds = date.getTime();
			var start_seconds = end_seconds - 1 * 24 * 60 * 60 * 1000;
			var table_name = getTableName("dashboard_", codis_addr);
			table_name += "_cmd_info";
			var search_type = "tp99,tp999,avg";
			var sql_command = getSelectSqlCommand(start_seconds, end_seconds, table_name, search_type, "cmd_name='ALL'", 1);
			sql_command = sql_command.substring(0, sql_command.indexOf("group by time"));
			var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, product_name);

			$scope.remoteGetTP(product_name, url, is_redis_group);
		}


		$scope.remoteGetQps = function(product_name, url, is_redis_group) {
			$http.get(url).then(function (resp) {
				var index = -1;
				if (is_redis_group) {
					for (var i=0; i<$scope.codis_redis_arr.length; i++) {
						if ($scope.codis_redis_arr[i].product_name == product_name) {
							index = i;
							break;
						}
					}
				} else {
					for (var i=0; i<$scope.codis_pika_arr.length; i++) {
						if ($scope.codis_pika_arr[i].product_name == product_name) {
							index = i;
							break;
						}
					}
				}
				
				if (index == -1) {
					return;
				}

				if (resp.data.Results && resp.data.Results.length > 0) {
					if (resp.data.Results[0].Series && resp.data.Results[0].Series.length > 0) {
						if (resp.data.Results[0].Series[0].values && resp.data.Results[0].Series[0].values.length > 0) {
							if (is_redis_group) {
								$scope.codis_redis_arr[index].qps = parseInt(resp.data.Results[0].Series[0].values[0][1]);
							} else {
								$scope.codis_pika_arr[index].qps = parseInt(resp.data.Results[0].Series[0].values[0][1]);
							}
							return;
						}
					}
				}
				if (is_redis_group) {
					$scope.codis_redis_arr[index].qps = "-";
				} else {
					$scope.codis_pika_arr[index].qps = "-";
				}
			},function (failedResp) {
				//$scope.codisinfo[index].qps = "error"; 
			});
		}

		$scope.remoteGetTP = function(product_name, url, is_redis_group) {
			$http.get(url).then(function (resp) {
				var index = -1;
				if (is_redis_group) {
					for (var i=0; i<$scope.codis_redis_arr.length; i++) {
						if ($scope.codis_redis_arr[i].product_name == product_name) {
							index = i;
							break;
						}
					}
				} else {
					for (var i=0; i<$scope.codis_pika_arr.length; i++) {
						if ($scope.codis_pika_arr[i].product_name == product_name) {
							index = i;
							break;
						}
					}
				}
				if (index == -1) {
					return;
				}
				if (resp.data.Results && resp.data.Results.length > 0) {
					if (resp.data.Results[0].Series && resp.data.Results[0].Series.length > 0) {
						if (resp.data.Results[0].Series[0].values && resp.data.Results[0].Series[0].values.length > 0) {
							if (is_redis_group) {
								$scope.codis_redis_arr[index].tp99 = parseInt(resp.data.Results[0].Series[0].values[0][1]);
								$scope.codis_redis_arr[index].tp999 = parseInt(resp.data.Results[0].Series[0].values[0][2]);
								$scope.codis_redis_arr[index].tpavg = parseInt(resp.data.Results[0].Series[0].values[0][3]);
							} else {
								$scope.codis_pika_arr[index].tp99 = parseInt(resp.data.Results[0].Series[0].values[0][1]);
								$scope.codis_pika_arr[index].tp999 = parseInt(resp.data.Results[0].Series[0].values[0][2]);
								$scope.codis_pika_arr[index].tpavg = parseInt(resp.data.Results[0].Series[0].values[0][3]);
							}
							return;
						}
					}
				}
				if (is_redis_group) {
					$scope.codis_redis_arr[index].tp99 = "-";
					$scope.codis_redis_arr[index].tp999 = "-";
					$scope.codis_redis_arr[index].tpavg = "-";
				} else {
					$scope.codis_pika_arr[index].tp99 = "-";
					$scope.codis_pika_arr[index].tp999 = "-";
					$scope.codis_pika_arr[index].tpavg = "-";
				}
			},function (failedResp) {
				//$scope.codisinfo[index].qps = "error"; 
			});
		}
		

		$scope.adjustTreeviewSelected = function (index) {
			var selected_node = "";
			if ($scope.selected_treeview_nodeid >= 0) {
				selected_node = $('#treeview_codis_list').treeview('getNode', $scope.selected_treeview_nodeid);
			}
			var selected_codis_name = "";
			var is_parent_node = false;
			if (selected_node != "") {
				if (selected_node.type == "codis") {
					is_parent_node = true;
					selected_codis_name = selected_node.text;
				} else {
					selected_codis_name = selected_node.codisname;
				}
			}
			var selected_parent_index = -1;
			for (var i=0; i<codis_treeview.length; i++) {
				if((codis_treeview[i].text) == selected_codis_name){
					selected_parent_index = i;
					break ;
				}
			}
			if (selected_parent_index > index) {
				$scope.selected_treeview_nodeid -= 2;
			} else if (selected_parent_index == index) {
				if (!is_parent_node) {
					$scope.selected_treeview_nodeid = -1;
				}
			}
		}

		$scope.adjustTreeviewExpend = function (index) {
			var expanded_parent_name = "";
			if ($scope.expanded_nodeid.parentid >= 0) {
				var node_type = $('#treeview_codis_list').treeview('getNode', $scope.expanded_nodeid.parentid).type;
				if (node_type != "codis") {
					expanded_parent_name = $('#treeview_codis_list').treeview('getNode', $scope.expanded_nodeid.parentid - 2).text;
				} else {
					expanded_parent_name = $('#treeview_codis_list').treeview('getNode', $scope.expanded_nodeid.parentid).text;
				}
			}
			var expanded_parent_index = -1;
			for (var i=0; i<codis_treeview.length; i++) {
				if((codis_treeview[i].text) == expanded_parent_name){
					expanded_parent_index = i;
					break ;
				}
			}
			var selected_parent_index = -1;
			for (var i=0; i<codis_treeview.length; i++) {
				if(codis_treeview[i].text == expanded_parent_name){
					expanded_parent_index = i;
					break ;
				}
			}
			if (expanded_parent_index > index) {
				$scope.expanded_nodeid.parentid -= 2;
				$scope.expanded_nodeid.proxyid -= 2;
				$scope.expanded_nodeid.serverid -= 2;
			} else if (expanded_parent_index == index) {
				$scope.expanded_nodeid.proxyid = -1;
				$scope.expanded_nodeid.serverid = -1;
			}
		}

		$scope.recoverTreevireState = function () {
			if ($scope.expanded_nodeid.parentid >= 0) {
				$('#treeview_codis_list').treeview('expandNode',[$scope.expanded_nodeid.parentid, {silent: true }]);
			}
			if ($scope.expanded_nodeid.proxyid >= 0) {
				$('#treeview_codis_list').treeview('expandNode',[$scope.expanded_nodeid.proxyid, {silent: true }]);
			}
			if ($scope.expanded_nodeid.serverid >= 0) {
				$('#treeview_codis_list').treeview('expandNode',[$scope.expanded_nodeid.serverid, {silent: true }]);
			}
			if ($scope.selected_treeview_nodeid >= 0) {
				$('#treeview_codis_list').treeview('selectNode', [ $scope.selected_treeview_nodeid, { silent: true } ]);
			}
		}

		//获取所有集群简要信息并存储到codisinfo数组中
		$scope.getCodisListInfo = function () {
			codis_treeview = [];
			$scope.codis_redis_arr = [];
			$scope.codis_pika_arr = [];
			$http.get('/list').then(function (resp) {
				$scope.codis_list = [];
				$scope.getTreeview();
				if (Object.prototype.toString.call(resp.data) == '[object Array]') {
					$scope.codis_list = resp.data;
					for(var i=0; i<$scope.codis_list.length; i++){
						codis_treeview.push(newTreeviewNode($scope.codis_list[i]));
						$scope.getCodisInfo($scope.codis_list[i], i);
						$scope.getTreeview();
					}
				}
				//package list struct
				if ($scope.codis_list.length <= 0) {
					alert("当前用户没有可操作集群，请联系管理员！");
				} 
			});
		}

		$scope.triggerCodisTree = function () {
			triggerCodisTree();
			$scope.resizeAllCharts();
		}

		$scope.codis_name = "";
		$scope.codis_redis_arr = [];
		$scope.codis_pika_arr = [];
		$scope.refresh_interval = 3;
		$scope.ip_port = getUrlHost();
		$scope.codis_redis_count_info = {
			Codis_Num: 0,
			Memorys: 0,
			Memorys_Human:"0 KB",
			Servers: 0,
			Machines:0,
		};
		$scope.codis_pika_count_info = {
			Codis_Num: 0,
			SSD: 0,
			SSD_Human:"0 KB",
			Servers: 0,
			Machines:0,
		};;
		var codis_treeview = [];
		$scope.getCodisListInfo();
		$scope.expanded_nodeid = {parentid:-1, proxyid:-1, serverid:-1};
		$scope.selected_treeview_nodeid = -1;
		$scope.can_operat = false;
		var qps_chart = echarts.init(document.getElementById('chart_ops'));
		var full_screen_chart_orgin;
		qps_chart.setOption(newChatsOpsConfigEcharts("", 1));
		$scope.qps_arr = [];
		$scope.proxy_div_sum = 0;
		$scope.proxy_qps_data_list = [];   //qps数据列表
		$scope.proxy_qps_chart_list = [];
		$scope.server_div_sum = 0;
		$scope.server_qps_data_list = [];   //qps数据列表
		$scope.server_qps_chart_list = [];
		$scope.qps_bunch_size = 4;
		var history_chart;
		$scope.history_main_chart = "";
		$scope.history_proxy_chart_map = [];
		$scope.history_server_chart_map = [];
		$scope.history_bunch_size = 4;
		$scope.treeview_proxy_ischange = false;
		$scope.treeview_proxy_add = false;
		$scope.treeview_server_ischange = false;
		$scope.treeview_server_add = false;
		$scope.influxdb_period_s = 1;
		$scope.create_by_param = true;
		$scope.is_admin = false;
		$scope.need_history_chart = true;
		$scope.proxy_num_ischange = false;
		$scope.server_num_ischange = false;
		$scope.cmd_response_chart_list = ["ALL"];
		$scope.history_chart_map = {};
		$scope.history_chart_tp999_all = {};
		$scope.history_chart_qps_all = {};
		$scope.showReadCmdInfo = false;
		$scope.have_cache = false;
		$scope.is_redis_group = false;
		$scope.showReadCmdInfo = false;
		$scope.more_tp_cmd = "tp999";
		$scope.select_response_cmd = "";

		//成倍扩容相关变量
		$scope.expansion_plan_list = [];
		$scope.expansion_refresh_status = false;

		full_screen_chart_for_iframe = echarts.init(document.getElementById('chart_container'));
		$("#chart_container_div").hide();
		$("#everyCodisInfo").hide();

		window.onresize = function(){
			$scope.resizeAllCharts();
		}

		$scope.resizeAllCharts = function() {
			if (!($("#QpsTop").is(":hidden"))) {
				if (qps_chart != "") {
					qps_chart.resize();
				}
				if ($scope.history_main_chart != "") {
					$scope.history_main_chart.resize();
				}
				for (var i=0; i<$scope.proxy_qps_chart_list.length; i++) {
					if ($scope.proxy_qps_chart_list[i]) {
						$scope.proxy_qps_chart_list[i].resize();
					}
				}
				for (var i=0; i<$scope.server_qps_chart_list.length; i++) {
					if ($scope.server_qps_chart_list[i]) {
						$scope.server_qps_chart_list[i].resize();
					}
				}
				for (var i=0; i<$scope.history_proxy_chart_map.length; i++) {
					if ($scope.history_proxy_chart_map[i]) {
						$scope.history_proxy_chart_map[i].resize();
					}
				}
				for (var i=0; i<$scope.history_server_chart_map.length; i++) {
					if ($scope.history_server_chart_map[i]) {
						$scope.history_server_chart_map[i].resize();
					}
				}
				for (var key in $scope.history_chart_map) {
					if ($scope.history_chart_map[key]) {
						$scope.history_chart_map[key].resize();
					}
				}
				for (var key in $scope.history_chart_tp999_all) {
					if ($scope.history_chart_tp999_all[key]) {
						$scope.history_chart_tp999_all[key].resize();
					}
				}
				for (var key in $scope.history_chart_qps_all) {
					if ($scope.history_chart_qps_all[key]) {
						$scope.history_chart_qps_all[key].resize();
					}
				}
			}
		}
		
		$http.get('/isadmin').then(function (resp) {
			$scope.is_admin = resp.data;
		},function (failedResp) {
			alert("get authority info failed!");  
		});

		$scope.resetOverview = function () {
			//$scope.codis_name = "NA";
			$scope.codis_addr = "NA";
			$scope.codis_coord = "NA";
			$scope.codis_coord_name = "Coordinator";
			$scope.codis_coord_addr = "NA";
			$scope.codis_start = "NA"
			$scope.codis_qps = "NA";
			$scope.codis_sessions = "NA";
			$scope.cache_mem = "NA";
			$scope.cache_hitrate = "NA";
			$scope.db_size = "NA";
			$scope.redis_keys = "NA";
			$scope.slots_array = [];
			$scope.proxy_array = [];
			$scope.group_array = [];
			$scope.slots_actions = [];
			$scope.qps_arr = [];
			$scope.slots_action_interval = "NA";
			$scope.slots_action_disabled = "NA";
			$scope.slots_action_failed = false;
			$scope.slots_action_remain = 0;
			$scope.server_num = 0;
			$scope.machine_num = 0;
			$scope.proxy_num = 0;
			$scope.total_conn = 0;
			$scope.alive_conn = 0;
			$scope.can_operat = false;
			$scope.sentinel_servers = [];
			$scope.sentinel_out_of_sync = false;
			setColor($scope.search_id, "");
			setColor($scope.time_id, "");
			$scope.search_id = "";
			$scope.show_type = "";
			$scope.time_id = "";
			$scope.legendSelectedAll = false;
			$scope.select_response_cmd = "";

			//清空proxy和server的单个QPS曲线数据
			$scope.proxy_qps_data_list = [];
			$scope.proxy_chart_div_info = [];
			$scope.proxy_qps_chart_list = [];
			$scope.proxy_div_sum=0;
			$scope.server_qps_data_list = [];
			$scope.server_chart_div_info = [];
			$scope.server_qps_chart_list = [];
			$scope.server_div_sum = 0;
			$("#newProxyQpsBtn").removeClass("active");
			$("#newServerQpsBtn").removeClass("active");
			//清空proxy和server的单个history曲线数据
			$scope.proxy_history_chart = [];
			$scope.server_history_chart = [];
			$("#newHistoryBtn").removeClass("active");

			$("#proxy_qps_list").empty();
			$("#server_qps_list").empty();
			//historyHide();

			$scope.expansion_plan_list = [];
			$scope.expansion_refresh_status = false;
			$('#expansionTask').hide();
			$scope.show_expansion = false;
			$scope.expansion_err_resp = "";
			$scope.expansion_slots_list = "";
		}
		$scope.resetOverview();

		$scope.getTreeview = function () {
			$('#treeview_codis_list').treeview({
				data: codis_treeview, 
				showIcon: true,
				color: "#428bca",
				expandIcon: 'glyphicon glyphicon-chevron-right',
				collapseIcon: 'glyphicon glyphicon-chevron-down',
				emptyIcon: '',
				enableLinks: false,
				onNodeSelected: function (event, node) {
					window.location.href = node.href;
					if ($scope.selected_treeview_nodeid != -1 && $scope.selected_treeview_nodeid != node.nodeId) {
						$('#treeview_codis_list').treeview('unselectNode', [ $scope.selected_treeview_nodeid, { silent: true } ]);
					}
					$scope.selected_treeview_nodeid = node.nodeId;
					$scope.onNodeClick(event, node);
				},
				onNodeUnselected: function (event, node) {
					if ($scope.selected_treeview_nodeid == node.nodeId) {
						$('#treeview_codis_list').treeview('selectNode', [ node.nodeId, { silent: true } ]);
					}
				},
				onNodeExpanded: function (event, node) {
					if (node.type == "codis"){
						if ($scope.expanded_nodeid.parentid >= 0) {
							$('#treeview_codis_list').treeview('collapseNode',[$scope.expanded_nodeid.parentid, { silent:true, ignoreChildren:false}]);
						}
						$scope.expanded_nodeid.parentid = node.nodeId;
						$scope.expanded_nodeid.proxyid = -1;
						$scope.expanded_nodeid.serverid = -1;
					} else if (node.type == "proxy") {
						if ($scope.expanded_nodeid.proxyid >= 0) {
							$('#treeview_codis_list').treeview('collapseNode',[$scope.expanded_nodeid.proxyid, { silent:true, ignoreChildren:false}]);
						}
						$scope.expanded_nodeid.proxyid = node.nodeId;
					} else if (node.type == "group") {
						if ($scope.expanded_nodeid.serverid >= 0) {
							$('#treeview_codis_list').treeview('collapseNode',[$scope.expanded_nodeid.serverid, { silent:true, ignoreChildren:false}]);
						}
						$scope.expanded_nodeid.serverid = node.nodeId;
					}
				},
				onNodeCollapsed: function (event, node) {
					if (node.type == "codis"){
						$scope.expanded_nodeid.parentid = -1;
					} else if (node.type == "proxy"){
						$scope.expanded_nodeid.proxyid = -1;
					} else if (node.type == "group"){
						$scope.expanded_nodeid.serverid = -1;
					}
				},
			});
		}
		$scope.onNodeClick = function (event, node) {
			if (!($('#iframeTable').css('display') === 'none')) {
				$scope.refresh_interval = getGlobalInterval();
			}

			if (node.type == "codis") {
				$scope.selected_treeview_nodeid = node.nodeId;
				$scope.need_history_chart = true;
				$scope.selectCodisInstance(node.text);
				$("#infoContainer").prop("src", "about:blank");
				$("#iframeTable").hide();
				$("#navigationProxyBtnGroup").hide();
			} else if (node.type == "proxy") {
				$scope.selected_treeview_nodeid = node.nodeId;
				var parent = $('#treeview_codis_list').treeview('getParent', node);
				if ($scope.codis_name != parent.text) {
					$scope.need_history_chart = false;
					$scope.selectCodisInstance(parent.text);
				}
				$("#infoContainer").prop("src", "about:blank");
				$("#iframeTable").hide();
				$("#navigationDashboardBtnGroup").hide();
				$("#navigationProxyBtnGroup").hide();
				$scope.refreshStats();
				showRoleTable('proxyIndex', '');
			} else if (node.type == "group") {
				$scope.selected_treeview_nodeid = node.nodeId;
				var parent = $('#treeview_codis_list').treeview('getParent', node);
				if ($scope.codis_name != parent.text) {
					$scope.need_history_chart = false;
					$scope.selectCodisInstance(parent.text);
				}
				$("#infoContainer").prop("src", "about:blank");
				$("#iframeTable").hide();
				$("#navigationDashboardBtnGroup").hide();
				$("#navigationProxyBtnGroup").show();
				showRoleTable('groupIndex', 'slotsIndex');
				$("#sentinelsIndex").show();
				$("#expansionIndex").show();
				$scope.refreshStats();
			} else if (node.type == "proxynode") {
				$scope.showProxyDetailedInfo(node.proxytoken, node.codisname);
			} else if (node.type == "servernode") {
				$scope.showServerDetailedInfo(node.groupid, node.serveraddr, node.codisname);
			}
		}

		$scope.showProxyDetailedInfo = function (token, codisname) {
			$("#iframeTable").show();
			var url = "/proxyInfo?codis=" + codisname + "#token=" + token + "&interval=" + $scope.refresh_interval;
			$("#infoContainer").prop("src", url);
		}

		$scope.showServerDetailedInfo = function (groupid, serveraddr, codisname) {
			$("#iframeTable").show();
			var url = "/serverInfo?codis="  + codisname 
					+ "#group=" + groupid + "&server=" + serveraddr + "&interval=" + $scope.refresh_interval;
			$("#infoContainer").prop("src", url);
		}

		$scope.selectTreeViewNode = function (type, text) {
			var node_list;
			var node = "";

			if (!($('#iframeTable').css('display') === 'none')) {
				$scope.refresh_interval = getGlobalInterval();
			}

			if ($scope.selected_treeview_nodeid >= 0) {
				node_list = $('#treeview_codis_list').treeview('getNode', $scope.selected_treeview_nodeid).nodes;
				for (var i=0; i<node_list.length; i++) {
					if (type == "proxy" && node_list[i].text == text) {
						node = node_list[i];
						break ;
					} else if (type == "server" && node_list[i].serveraddr == text) {
						node = node_list[i];
						break ;
					}
				}
			} else if (type == 'codis') {
				for (var i=0; ; i++) {
					node = $('#treeview_codis_list').treeview('getNode', i);
					if (node == undefined || node.text == text) {
						break;
					}
				}
			}

			if (node == undefined) {
				return;
			}
			
			if (node.nodeId != $scope.selected_treeview_nodeid) {
				if (type == "proxy" && $scope.selected_treeview_nodeid != $scope.expanded_nodeid.proxyid) {
					$('#treeview_codis_list').treeview('expandNode',[$scope.selected_treeview_nodeid, {silent: true }]);
					$scope.expanded_nodeid.proxyid = $scope.selected_treeview_nodeid;
				} else if (type == "server" && $scope.selected_treeview_nodeid != $scope.expanded_nodeid.serverid) {
					$('#treeview_codis_list').treeview('expandNode',[$scope.selected_treeview_nodeid, {silent: true }]);
					$scope.expanded_nodeid.serverid = $scope.selected_treeview_nodeid;
				}

				$('#treeview_codis_list').treeview('selectNode', [ node.nodeId, { silent: true } ]);
				if (type == "proxy") {
					$scope.showProxyDetailedInfo(node.proxytoken, node.codisname);
				} else if (type == "server") {
					$scope.showServerDetailedInfo(node.groupid, node.serveraddr, node.codisname);
				} else if (type == "codis") {
					$scope.selectCodisInstance(text);
				}
				$scope.selected_treeview_nodeid = node.nodeId;
			}
		}

		$scope.getRemoteServiceConf = function (type, serveraddr) {
			var url = "/deploy/" + type + "/remote/" + serveraddr;
			serveraddr = serveraddr.replace(/:/g, "_");
			var postfix = "";
			if (type == "redis" || type == "pika") {
				postfix = ".conf";
			} else {
				postfix = ".toml";
			}
			$http.get(url).then(function (resp) {
				var blob = new Blob([resp.data], {type: "text/plain;charset=utf-8"});
				saveAs(blob, type + "_" + serveraddr + postfix);
			},function (failedResp) {
				alertErrorResp(failedResp);
			});
		}

		$scope.openNewServiceDialog = function(id, type) {
			$scope.getGroupListByServerType(type);
			openDialog(id);
			$("#manual_conf_btn").addClass("active");
		}

		$scope.getGroupListByServerType = function(type) {
			$scope.deployServiceType = type;
			if (type == "server") {
				$scope.pika_group_arr = [];
				$scope.redis_group_arr = [];
				for (var i=0; i<$scope.group_array.length; i++) {
					var server_type = $scope.group_array[i].server_type;
					if (server_type == "null" || server_type == "redis") {
						$scope.redis_group_arr.push($scope.group_array[i].id);
					}
					if (server_type == "null" || server_type == "pika") {
						$scope.pika_group_arr.push($scope.group_array[i].id);
					}
				}
			}
		}

		$scope.closeNewServiceDialog = function(id) {
			$scope.create_by_param = true;
			$("#conf_way_hand .btn").removeClass("active");
			closeDialog(id);
		}

		$scope.newService_server_type = "Redis";
		$scope.newServiceByParam = function (type) {
			if(type == "param") {
				$scope.create_by_param = true;
			} else if (type == "file") {
				$scope.create_by_param = false;
			}
		}

		$scope.postDashboardInfo = function () {
			var admin_addr = strTrim($("input[name='admin_addr']").val());
			var product_name = strTrim($("input[name='product_name']").val());
			var ncpu = strTrim($("input[name='ncpu']").val());
			var logLevel = strTrim($("#logLevel").val());
			if (admin_addr == "") {
				alert("\"admin_addr\"不能为空！");
				return;
			}
			if (product_name == "") {
				alert("\"product_name\"不能为空！");
				return;
			}
			if ($scope.create_by_param == true) {
				var coordinator_addr = strTrim($("input[name='coordinator_addr']").val());
				var product_auth = strTrim($("input[name='product_auth']").val());
				var influxdb_server = strTrim($("input[name='influxdb_server']").val());
				var influxdb_period = strTrim($("input[name='influxdb_period']").val());
				var influxdb_username = strTrim($("input[name='influxdb_username']").val());
				var influxdb_password = strTrim($("input[name='influxdb_password']").val());
				var influxdb_database = strTrim($("input[name='influxdb_database']").val());
				if (coordinator_addr == "") {
					alert("\"coordinator_addr\"不能为空！");
					return;
				}
				if (influxdb_server == "") {
					alert("\"influxdb_server\"不能为空！");
					return;
				}
				if (influxdb_username == "") {
					alert("\"influxdb_username\"不能为空！");
					return;
				}
				if (influxdb_password == "") {
					alert("\"influxdb_password\"不能为空！");
					return;
				}
				if (influxdb_database == "") {
					alert("\"influxdb_database\"不能为空！");
					return;
				}
				$("#loadingChart").show();
				$http({  
					method:'post',  
					url:'/deploy/dashboard/create',  
					data:{
						admin_addr:admin_addr, 
						coordinator_addr:coordinator_addr, 
						influxdb_database:influxdb_database, 
						product_name:product_name, 
						product_auth:product_auth, 
						ncpu:ncpu, 
						logLevel:logLevel, 
						influxdb_server:influxdb_server, 
						influxdb_period:influxdb_period, 
						influxdb_username:influxdb_username, 
						influxdb_password:influxdb_password
					},  
					headers:{'Content-Type': 'application/x-www-form-urlencoded'},  
					transformRequest: function(obj) {  
						var str = [];  
						for(var p in obj){  
							str.push(encodeURIComponent(p) + "=" + encodeURIComponent(obj[p]));  
						}  
						return str.join("&");  
					}  
				}).then(function success(resp){ 
					$("#loadingChart").hide();
					if (resp.data == "\"Success\""){
						$scope.addDashboardToDatabase(product_name, admin_addr);
					} else {
						alertErrorResp(resp);
					}
				}, function error(failedresp) {
					$("#loadingChart").hide();
					alertErrorResp(failedresp);  
				});
			} else {
				var file = document.querySelector("input[type=file]").files[0];
				if (file == undefined) {
					alert("请上传配置文件！");
					return;
				}
				$("#loadingChart").show();
				$http({  
					method:'post',  
					url:'/deploy/dashboard/create',  
					data:{
						confFile:file, 
						admin_addr:admin_addr,
						product_name:product_name, 
						ncpu:ncpu, 
						logLevel:logLevel
					},  
					headers:{'Content-Type': undefined},  
					transformRequest: (data, headersGetter) => {
						let formData = new FormData();
						angular.forEach(data, function (value, key) {

						  formData.append(key, value);

						});
						return formData;
					} 
				}).then(function success(resp){ 
					$("#loadingChart").hide();
					if (resp.data == "\"Success\""){
						$scope.addDashboardToDatabase(product_name, admin_addr);
					} else {
						alertErrorResp(resp);
					}
				}, function error(failedResp) {
					$("#loadingChart").hide();  
					alertErrorResp(failedResp);  
				}); 
			}
		}

		$scope.addDashboardToDatabase = function(product_name, admin_addr) {
			var sql = "insert into codis(codisname,dashboard) values('" 
					+ product_name + "','" 
					+ admin_addr + "')";
			var url = "/sql/"+sql;

			$http.get(url).then(function (resp) {
				var result = resp.data;
				if (!isNaN(result)) {
					$scope.getCodisListInfo();
					//alert("codis信息添加成功");
					resp.data = "Dashboard: " + admin_addr + " 创建并添加成功 !";
					alertSuccessResp(resp);
				} else {
					//alert("codis信息添加失败");
					resp.data = "Dashboard: " + admin_addr + " 创建成功，但添加失败 !";
					alertErrorResp(resp);
				}
			},function (failedResp) {
				alertErrorResp(failedResp);
			});
		}

		$scope.postProxyInfo = function () {
			var admin_addr = strTrim($("input[name='admin_addr']").val());
			var proxy_addr = strTrim($("input[name='proxy_addr']").val());
			var ncpu = strTrim($("input[name='ncpu']").val());
			var logLevel = strTrim($("#logLevel").val());
			var product_name = $scope.codis_name;
			var proxy_is_exist = false;
			for (var i=0; i<$scope.proxy_array.length; i++) {
				if (admin_addr == $scope.proxy_array[i].admin_addr) {
					proxy_is_exist = true;
					break;
				}
			}
			if (admin_addr == "") {
				alert("\"admin_addr\"不能为空！");
				return;
			}
			if (proxy_addr == "") {
				alert("\"proxy_addr\"不能为空！");
				return;
			}

			if ($scope.create_by_param == true) {
				var jodis_addr = strTrim($("input[name='jodis_addr']").val());
				var product_auth = strTrim($("input[name='product_auth']").val());
				var max_clients = strTrim($("input[name='max_clients']").val());
				var backend_parallel = strTrim($("input[name='backend_parallel']").val());
				var backend_max_pipeline = strTrim($("input[name='backend_max_pipeline']").val());
				var session_max_pipeline = strTrim($("input[name='session_max_pipeline']").val());
				if (jodis_addr == "") {
					alert("\"jodis_addr\"不能为空！");
					return;
				}
				$("#loadingChart").show();
				$http({  
					method:'post',  
					url:'/deploy/proxy/create',  
					data:{
						admin_addr:admin_addr, 
						proxy_addr:proxy_addr, 
						jodis_addr:jodis_addr, 
						product_name:product_name, 
						product_auth:product_auth, 
						ncpu:ncpu, 
						logLevel:logLevel,
						max_clients:max_clients,
						backend_parallel:backend_parallel,
						backend_max_pipeline:backend_max_pipeline,
						session_max_pipeline:session_max_pipeline
					},  
					headers:{'Content-Type': 'application/x-www-form-urlencoded'},  
					transformRequest: function(obj) {  
						var str = [];  
						for(var p in obj){  
							str.push(encodeURIComponent(p) + "=" + encodeURIComponent(obj[p]));  
						}  
						return str.join("&");  
					}  
				}).then(function success(resp){
					$("#loadingChart").hide();
					if (resp.data == "\"Success\"") {
						if (!proxy_is_exist) {
							$scope.createProxy(admin_addr);
						}
						resp.data = "Create Prxoy " + admin_addr + " Success !";
						alertSuccessResp(resp);
					} else {
						alertErrorResp(resp);
					}
					
				}, function error(failedResp) {
					$("#loadingChart").hide();
					alertErrorResp(failedResp);  
				});
			} else {
				var file = document.querySelector("input[type=file]").files[0];
				if (file == undefined) {
					alert("请上传配置文件");
					return;
				}
				$("#loadingChart").show();
				$http({  
					method:'post',  
					url:'/deploy/proxy/create',  
					data:{
						confFile:file, 
						admin_addr:admin_addr,
						proxy_addr:proxy_addr, 
						ncpu:ncpu, 
						logLevel:logLevel,
						product_name:product_name
					},  
					headers:{'Content-Type': undefined},  
					transformRequest: (data, headersGetter) => {
						let formData = new FormData();
						angular.forEach(data, function (value, key) {

						  formData.append(key, value);

						});
						return formData;
					} 
				}).then(function success(resp){
					$("#loadingChart").hide(); 
					if (resp.data == "\"Success\"") {
						if (!proxy_is_exist) {
							$scope.createProxy(admin_addr);
						}
						resp.data = "Create Prxoy " + admin_addr + " Success !";
						alertSuccessResp(resp);
					} else {
						alertErrorResp(resp);
					}
				}, function error(failedResp) { 
					$("#loadingChart").hide(); 
					alertErrorResp(failedResp);  
				}); 
			}
		}

		$scope.postServerInfo = function (type) {
			var remote_ip = strTrim($("input[name='remote_ip']").val());
			var port = strTrim($("input[name='port']").val());
			var groupid = strTrim($("#group_id").val());
			if (remote_ip == "") {
				alert("请输入\"remote_ip\"");
				return;
			}

			if (port == "") {
				alert("请输入\"port\"");
				return;
			}
			
			if (groupid == "" || groupid == null) {
				alert("请选择\"groupid\"");
				return;
			}
			var server_addr = remote_ip + ":" + port; 
			if (type == "Redis") {
				if ($scope.create_by_param == true) {
					var databases = strTrim($("input[name='databases']").val());
					var masterauth = strTrim($("input[name='masterauth']").val());
					var requirepass = strTrim($("input[name='requirepass']").val());
					var maxclients = strTrim($("input[name='maxclients']").val());
					var maxmemory = strTrim($("input[name='maxmemory']").val());
					var appendonly = strTrim($("#appendonly").val());
					var appendfsync = strTrim($("#appendfsync").val());
					$("#loadingChart").show();
					$http({  
						method:'post',  
						url:'/deploy/redis/create',  
						data:{
							remote_ip:remote_ip, 
							port:port, 
							databases:databases, 
							masterauth:masterauth, 
							requirepass:requirepass,  
							maxclients:maxclients, 
							maxmemory:maxmemory, 
							appendonly:appendonly, 
							appendfsync:appendfsync
						},  
						headers:{'Content-Type': 'application/x-www-form-urlencoded'},  
						transformRequest: function(obj) {  
							var str = [];  
							for(var p in obj){  
								str.push(encodeURIComponent(p) + "=" + encodeURIComponent(obj[p]));  
							}  
							return str.join("&");  
						}  
					}).then(function success(resp){ 
						$("#loadingChart").hide();
						if (resp.data == "\"Success\"") {
							resp.data = "Create " + type + ": " + server_addr + " Success !";
							alertSuccessResp(resp);
							$scope.addGroupServer(groupid, server_addr);
						} else {
							alertErrorResp(resp);
						}
					}, function error(failedResp) {
						$("#loadingChart").hide();
						alertErrorResp(failedResp);  
					});
				} else {
					var file = document.querySelector("input[type=file]").files[0];
					if (file == undefined) {
						alert("请上传配置文件");
						return;
					}
					$("#loadingChart").show();
					$http({  
						method:'post',  
						url:'/deploy/redis/create',  
						data:{
							confFile:file,  
							remote_ip:remote_ip, 
							port:port
						},  
						headers:{'Content-Type': undefined},  
						transformRequest: (data, headersGetter) => {
							let formData = new FormData();
							angular.forEach(data, function (value, key) {

							  formData.append(key, value);

							});
							return formData;
						} 
					}).then(function success(resp){ 
						$("#loadingChart").hide();
						if (resp.data == "\"Success\"") {
							resp.data = "Create " + type + ": " + server_addr + " Success !";
							alertSuccessResp(resp);
							$scope.addGroupServer(groupid, server_addr);
						} else {
							alertErrorResp(resp);
						}
					}, function error(failedResp) {  
						$("#loadingChart").hide();
						alertErrorResp(failedResp);  
					}); 
				}
			} else if (type == "Pika") {
				if ($scope.create_by_param == true) {
					var thread_num = strTrim($("input[name='thread_num']").val());
					var masterauth = strTrim($("input[name='masterauth']").val());
					var userpass = strTrim($("input[name='userpass']").val());
					var requirepass = strTrim($("input[name='requirepass']").val());
					var maxclients = strTrim($("input[name='maxclients']").val());
					$("#loadingChart").show();
					$http({  
						method:'post',  
						url:'/deploy/pika/create',  
						data:{
							remote_ip:remote_ip, 
							port:port, 
							thread_num:thread_num, 
							masterauth:masterauth, 
							userpass:userpass, 
							requirepass:requirepass,  
							maxclients:maxclients, 
						},  
						headers:{'Content-Type': 'application/x-www-form-urlencoded'},  
						transformRequest: function(obj) {  
							var str = [];  
							for(var p in obj){  
								str.push(encodeURIComponent(p) + "=" + encodeURIComponent(obj[p]));  
							}  
							return str.join("&");  
						}  
					}).then(function success(resp){
						$("#loadingChart").hide(); 
						if (resp.data == "\"Success\"") {
							resp.data = "Create " + type + ": " + server_addr + " Success !";
							alertSuccessResp(resp);
							$scope.addGroupServer(groupid, server_addr);
						} else {
							alertErrorResp(resp);
						}
					}, function error(failedResp) {
						$("#loadingChart").hide();
						alertErrorResp(failedResp);  
					});
				} else {
					var file = document.querySelector("input[type=file]").files[0];
					if (file == undefined) {
						alert("请上传配置文件");
						return;
					}
					$("#loadingChart").show();
					$http({  
						method:'post',  
						url:'/deploy/pika/create',  
						data:{
							confFile:file,  
							remote_ip:remote_ip, 
							port:port
						},  
						headers:{'Content-Type': undefined},  
						transformRequest: (data, headersGetter) => {
							let formData = new FormData();
							angular.forEach(data, function (value, key) {

							  formData.append(key, value);

							});
							return formData;
						} 
					}).then(function success(resp){ 
						$("#loadingChart").hide();
						if (resp.data == "\"Success\"") {
							resp.data = "Create " + type + ": " + server_addr + " Success !";
							alertSuccessResp(resp);
							$scope.addGroupServer(groupid, server_addr);
						} else {
							alertErrorResp(resp);
						}
					}, function error(failedResp) { 
						$("#loadingChart").hide(); 
						alertErrorResp(failedResp);  
					}); 
				}
			}
		}
		
		$scope.selectCodisInstance = function (selected) {
			showEveryInfo();
			qps_chart.resize();
			if ($scope.codis_name == selected) {
				if ($scope.need_history_chart == true && $("#demo").attr("class").indexOf("in") <= 0) {
					$scope.fillHistory();
					//historyShow();
				}
				return ;
			}

			$scope.resetOverview();
			$scope.codis_name = selected;
			$http.get('/auth/'+selected).then(function (resp) {
				$scope.can_operat = resp.data;
				$scope.queryCodisInfo(selected);
			},function (failedResp) {
				// 请求失败
				if (selected == getUrlParams()) {
					alert("get authority info failed!");
					clearHistorychart("historyCharts");   
				}
			});
		}

		$scope.queryCodisInfo = function (codisname) {
			var url = concatUrl("/topom", codisname);
			$http.get(url).then(function (resp) {
				if ($scope.codis_name != codisname) {
					return ;
				}

				var overview = resp.data;
				$scope.codis_addr = overview.model.admin_addr;
				$scope.codis_start = overview.model.start_time.substring(0,19);
				$scope.codis_coord = "[" + overview.config.coordinator_name + "] " + overview.config.coordinator_addr;
				$scope.codis_coord_name = "[" + overview.config.coordinator_name.charAt(0).toUpperCase() + overview.config.coordinator_name.slice(1) + "]";
				$scope.codis_coord_addr = overview.config.coordinator_addr;
				var influxdb_period = overview.config.metrics_report_influxdb_period;
				var influxdb_period_unit = influxdb_period.charAt(influxdb_period.length - 1);
				$scope.influxdb_period_s = parseInt(influxdb_period.substring(0, influxdb_period.length - 1));
				if (influxdb_period_unit == "m") {
					$scope.influxdb_period_s *= 60;
				}
				$scope.updateStats(overview.stats);
				if ($scope.need_history_chart){
					$scope.fillHistory();
					//historyShow();
				}
			},function (failedResp) {
				// 请求失败
				if (codisname == getUrlParams()) {
					alert("get " + codisname + " info failed!");
					$scope.resetOverview();
					clearHistorychart("historyCharts");   
				}
			});
		}

		$scope.updateStats = function (codis_stats) {
			var proxy_stats = processProxyStats(codis_stats);
			var group_stats = processGroupStats(codis_stats); 
			var sentinel = processSentinels(codis_stats, group_stats, $scope.codis_name);

			var merge = function(obj1, obj2) {
				if (obj1 === null || obj2 === null) {
					return obj2;
				}
				if (Array.isArray(obj1)) {
					if (obj1.length != obj2.length) {
						return obj2;
					}
					for (var i = 0; i < obj1.length; i ++) {
						obj1[i] = merge(obj1[i], obj2[i]);
					}
					return obj1;
				}
				if (typeof obj1 === "object") {
					for (var k in obj1) {
						if (obj2[k] === undefined) {
							delete obj1[k];
						}
					}
					for (var k in obj2) {
						obj1[k] = merge(obj1[k], obj2[k]);
					}
					return obj1;
				}
				return obj2;
			}
			
			$scope.GroupStats = codis_stats.group.stats;
			$scope.codis_qps = proxy_stats.qps;
			$scope.codis_sessions = proxy_stats.sessions;
			$scope.have_cache = group_stats.have_cache;
			$scope.is_redis_group = group_stats.is_redis_group;
			$scope.cache_mem = humanSize(group_stats.cache_memory);
			$scope.cache_hitrate = group_stats.cache_hitrate;
			$scope.server_mem = humanSize(group_stats.pika_memory + group_stats.cache_memory);
			$scope.db_size = humanSize(group_stats.dbsize);
			if ($scope.is_redis_group && group_stats.dbsize == 0) {
				$scope.db_size = "INF."
			}
			$scope.redis_keys = group_stats.keys.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
			$scope.slots_array = merge($scope.slots_array, codis_stats.slots);
			$scope.proxy_array = merge($scope.proxy_array, proxy_stats.proxy_array);
			$scope.group_array = merge($scope.group_array, group_stats.group_array);
			
			//$scope.group_array = getslotsCacheMap(codis_stats, $scope.group_array);
			var instance_info = getInstanceInfo(codis_stats);
			$scope.server_num = instance_info.server_num;
			$scope.machine_num = instance_info.machine_num;
			$scope.alive_conn = getClientConn(codis_stats).alive;
			$scope.proxy_num = getProxyNum(codis_stats);
			$scope.slots_actions = [];
			$scope.slots_action_interval = codis_stats.slot_action.interval;
			$scope.slots_action_disabled = codis_stats.slot_action.disabled;
			$scope.slots_action_failed = codis_stats.slot_action.progress.failed;
			$scope.slots_action_remain = codis_stats.slot_action.progress.remain;
			$scope.slots_action_progress = codis_stats.slot_action.progress.status;
			$scope.sentinel_servers = merge($scope.sentinel_servers, sentinel.servers);
			$scope.sentinel_out_of_sync = sentinel.out_of_sync;
			$scope.ip_port = getUrlHost(); 
	  
			for (var i = 0; i < $scope.slots_array.length; i++) {
				var slot = $scope.slots_array[i];

				if (slot.action.state) {
					$scope.slots_actions.push(slot);
				}
			}

			if ($scope.sentinel_servers.length != 0) {
				for (var i = 0; i < $scope.group_array.length; i ++) {
					var g = $scope.group_array[i];
					var ha_master = sentinel.masters[g.id];
					var ha_master_ingroup = false;
					for (var j = 0; j < g.servers.length; j ++) {
						var x = g.servers[j];
						if (ha_master == undefined) {
							x.ha_status = "ha_undefined";
							continue;
						}
						if (j == 0) {
							if (x.server == ha_master) {
								x.ha_status = "ha_master";
							} else {
								x.ha_status = "ha_not_master";
							}
						} else {
							if (x.server == ha_master) {
								x.ha_status = "ha_real_master";
							} else {
								x.ha_status = "ha_slave";
							}
						}
						if (x.server == ha_master) {
							x.server_text = x.server + " [HA]";
							ha_master_ingroup = true;
						}
					}
					if (ha_master == undefined || ha_master_ingroup) {
						g.ha_warning = "";
					} else {
						g.ha_warning = "[HA: " + ha_master + "]";
					}
				}
			}

			$scope.usingGroup = renderSlotsCharts($scope.slots_array);

			if ($scope.qps_arr.length >= 20) {
				$scope.qps_arr.shift();
			}

			for (var i=0; i<$scope.group_array.length; i++) {
				$scope.group_array[i].used = false;
				for (var j=0; j<$scope.usingGroup.length; j++) {
					if ($scope.group_array[i].id == $scope.usingGroup[j]) {
						$scope.group_array[i].used = true;
						break;
					}
				}
			}
			
			$scope.qps_arr.push([new Date(), proxy_stats.qps]);
			qps_chart.setOption({
				series: [{
					name: '',
					data: $scope.qps_arr,
				}],
				grid: {
					top: '10%',
					bottom: '0%',
				}
			});

			if ($scope.proxy_num_ischange && !($("#QpsTop").is(":hidden"))) {
				if ($scope.proxy_qps_chart_list.length > 0) {
					$scope.newQpsDiv("proxy");
					$scope.newQpsDiv("proxy");
				}
				if ($scope.proxy_history_chart.length > 0) {
					$scope.newHistoryDiv("proxy");
					$scope.newHistoryDiv("proxy");
				}
				$scope.proxy_num_ischange = false;
			} 

			if ($scope.server_num_ischange && !($("#QpsTop").is(":hidden"))) {
				if ($scope.server_qps_chart_list.length > 0) {
					$scope.newQpsDiv("server");
					$scope.newQpsDiv("server");
				}
				if ($scope.server_history_chart.length > 0) {
					$scope.newHistoryDiv("server");
					$scope.newHistoryDiv("server");
				}
				$scope.server_num_ischange = false;
			}  

			if ($scope.proxy_qps_data_list.length > 0 && !$scope.proxy_num_ischange && $scope.proxy_qps_chart_list.length > 0) {
				for (var i=0; i<$scope.proxy_qps_data_list.length; i++) {
					if ($scope.proxy_qps_data_list[i].data.length >=20) {
						$scope.proxy_qps_data_list[i].data.shift(); 
					}
					$scope.proxy_qps_data_list[i].data.push([new Date(), $scope.proxy_array[i].qps]);
				}
			
				for (var i=0; i<$scope.proxy_div_sum; i++) {
					var series = [];
					var legend = [];
					for (var j=0; j<$scope.qps_bunch_size; j++) {
						series.push({
							name: $scope.proxy_qps_data_list[i * $scope.qps_bunch_size + j].source,
							data: $scope.proxy_qps_data_list[i * $scope.qps_bunch_size + j].data,
						});
						legend.push($scope.proxy_qps_data_list[i * $scope.qps_bunch_size + j].source);
						if (i * $scope.qps_bunch_size + j >= $scope.proxy_array.length-1) {
							break ; 
						}
					}

					$scope.proxy_qps_chart_list[i].setOption({
						series: series,
						legend: {
							padding: 0,
							left: 100,
							data: legend,
						},
					});
				}
			}

			if ($scope.server_qps_data_list.length > 0 && !$scope.server_num_ischange && $scope.server_qps_chart_list.length > 0) {
				var j = 0;
				for (var i=0; i<$scope.group_array.length; i++) {
					if ($scope.group_array[i].servers.length <= 0) {
						continue ; 
					} else {
						if ($scope.server_qps_data_list[j].data.length >= 20) {
							$scope.server_qps_data_list[j].data.shift();
						}
						$scope.server_qps_data_list[j].data.push([new Date(), $scope.group_array[i].servers[0].qps]);
						j++;
					}
				}

				for (var i=0; i<$scope.server_div_sum; i++) {
					var series = [];
					var legend = [];
					for (var j=0; j<$scope.qps_bunch_size; j++) {
						series.push({
							name: $scope.server_qps_data_list[i * $scope.qps_bunch_size + j].source,
							data: $scope.server_qps_data_list[i * $scope.qps_bunch_size + j].data,
						});
						legend.push($scope.server_qps_data_list[i * $scope.qps_bunch_size + j].source);
						if (i * $scope.qps_bunch_size + j >= $scope.server_qps_data_list.length-1) {
							break ; 
						}
					}

					$scope.server_qps_chart_list[i].setOption({
						series: series,
						legend: {
							padding: 0,
							left: 100,
							data: legend,
						},
					});
				}
			}

			if ($scope.treeview_proxy_ischange) {
				$scope.refresh_proxy_tree();
				$scope.getTreeview();
				$scope.treeview_proxy_ischange = false;
				var expandedNodeText = "";
				if ($scope.expanded_nodeid.parentid >= 0) {
					$('#treeview_codis_list').treeview('expandNode',[$scope.expanded_nodeid.parentid, {silent: true }]);
					expandedNodeText = $('#treeview_codis_list').treeview('getNode',$scope.expanded_nodeid.parentid).text;
				}
				if ($scope.expanded_nodeid.proxyid >= 0) {
					$('#treeview_codis_list').treeview('expandNode',[$scope.expanded_nodeid.proxyid, {silent: true }]);
				}

				if ($scope.expanded_nodeid.serverid >= 0 && expandedNodeText == $scope.codis_name) {
					if ($scope.treeview_proxy_add){
						$scope.expanded_nodeid.serverid++;
					} else {
						$scope.expanded_nodeid.serverid--;
					}
					$('#treeview_codis_list').treeview('expandNode',[$scope.expanded_nodeid.serverid, {silent: true }]);
				} 
				if ($scope.expanded_nodeid.serverid >= 0) {
					$('#treeview_codis_list').treeview('expandNode',[$scope.expanded_nodeid.serverid, {silent: true }]);
				} 
				if ($scope.selected_treeview_nodeid >= 0) {
					$('#treeview_codis_list').treeview('selectNode', [ $scope.selected_treeview_nodeid, { silent: true } ]);
				}

			}
			
			if ($scope.treeview_server_ischange) {
				$scope.refresh_server_tree();
				$scope.getTreeview();
				$scope.treeview_server_ischange = false;

				if ($scope.expanded_nodeid.parentid >= 0) {
					$('#treeview_codis_list').treeview('expandNode',[$scope.expanded_nodeid.parentid, {silent: true }]);
				}
				if ($scope.expanded_nodeid.proxyid >= 0) {
					$('#treeview_codis_list').treeview('expandNode',[$scope.expanded_nodeid.proxyid, {silent: true }]);
				}
				if ($scope.expanded_nodeid.serverid >= 0) {
					$('#treeview_codis_list').treeview('expandNode',[$scope.expanded_nodeid.serverid, {silent: true }]);
				} 
				if ($scope.selected_treeview_nodeid >= 0) {
					$('#treeview_codis_list').treeview('selectNode', [ $scope.selected_treeview_nodeid, { silent: true } ]);
				}
			}
			if ($scope.expansion_refresh_status) {
				$scope.refreshExpansionList();
			}
		}

		$scope.refreshStats = function () {
			var codis_name = $scope.codis_name;
			var codis_addr = $scope.codis_addr;
			if (isValidInput(codis_name) && isValidInput(codis_addr)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/stats/" + xauth, codis_name);
				$http.get(url).then(function (resp) {
					if ($scope.codis_name != codis_name) {
						return ;
					}
					$scope.updateStats(resp.data);
				});
			}
		}

		$scope.proxy_chart_div_info = [];
		$scope.server_chart_div_info = [];
		$scope.addProxyChart = function () {
			for (var i=0; i<$scope.proxy_chart_div_info.length ;i++) {
				var qps_chart = echarts.init(document.getElementById("proxy_chart_ops" + i));
				qps_chart.setOption(newChatsOpsConfigEcharts("proxy", $scope.qps_bunch_size));
				$scope.proxy_qps_chart_list.push(qps_chart);
			}
		}

		$scope.addServerChart = function () {
			for (var i=0; i<$scope.server_chart_div_info.length ;i++) {
				var qps_chart = echarts.init(document.getElementById("server_chart_ops" + i));
				qps_chart.setOption(newChatsOpsConfigEcharts("server", $scope.qps_bunch_size));
				$scope.server_qps_chart_list.push(qps_chart);
			}
		}

		$scope.addQpsChart = function () {
			if ($("#proxy_chart_ops0").length>0 && $scope.proxy_qps_chart_list.length <= 0) {
				$scope.addProxyChart();
			}

			if ($("#server_chart_ops0").length>0 && $scope.server_qps_chart_list.length <= 0) {
				$scope.addServerChart();
			}
		}
		   
		$scope.newQpsDiv = function (type) {
			if (type == "proxy") {
				if ($scope.proxy_qps_chart_list.length > 0) {
					for (var i=0; i<$scope.proxy_qps_chart_list.length; i++) {
						if ($scope.proxy_qps_chart_list[i] && (!($scope.proxy_qps_chart_list[i].isDisposed()))) {
							$scope.proxy_qps_chart_list[i].dispose();
						}
					}
					$scope.proxy_qps_data_list = [];
					$scope.proxy_chart_div_info = [];
					$scope.proxy_qps_chart_list = [];
					$scope.proxy_div_sum=0;
					$("#newProxyQpsBtn").removeClass("active");
					return ;
				}
				if ($scope.proxy_array.length == 0) {
					alert("无可展示的Proxy,请在集群中添加新的Proxy！");
					return ;
				}
				$("#newProxyQpsBtn").addClass("active");
				var num = Math.ceil($scope.proxy_array.length/$scope.qps_bunch_size);
				for (var i=0; i<num ;i++) {
					$scope.proxy_div_sum++;
					$scope.proxy_chart_div_info.push({type:type, id:i});
				}
				for (var i=0; i<$scope.proxy_array.length; i++) {
					var qps_echart_line = new qpsEchartLine(i);
					qps_echart_line.source = $scope.proxy_array[i].proxy_addr;
					$scope.proxy_qps_data_list.push(qps_echart_line);
				}
			} else if (type == "server") {
				if ($scope.server_qps_chart_list.length > 0) {
					for (var i=0; i<$scope.server_qps_chart_list.length; i++) {
						if ($scope.server_qps_chart_list[i] && (!($scope.server_qps_chart_list[i].isDisposed()))) {
							$scope.server_qps_chart_list[i].dispose();
						}
					}
					$scope.server_qps_data_list = [];
					$scope.server_chart_div_info = [];
					$scope.server_qps_chart_list = [];
					$scope.server_div_sum = 0;
					$("#newServerQpsBtn").removeClass("active");
					return ;
				}
				var used_group_num = 0;
				for (var i=0; i<$scope.group_array.length; i++) {
					if ($scope.group_array[i].servers.length > 0) {
						used_group_num++;
					}
				}
				if (used_group_num == 0) {
					alert("无可展示的Server,请在集群中添加新的Server！");
					return ;
				}

				$("#newServerQpsBtn").addClass("active");
				var num = Math.ceil(used_group_num/$scope.qps_bunch_size);
				for (var i=0; i<num; i++) {
					$scope.server_div_sum++;
					$scope.server_chart_div_info.push({type:type, id:i});
				}

				for (var i=0; i<$scope.group_array.length; i++) {
					if ($scope.group_array[i].servers.length <= 0) {
						continue ;
					}
					var qps_echart_line = new qpsEchartLine(i);
					qps_echart_line.source = $scope.group_array[i].servers[0].server;
					$scope.server_qps_data_list.push(qps_echart_line);
				}
			} 
		}

		$scope.refreshQpsCharts = function () {
			if ($scope.proxy_qps_chart_list.length > 0) {
				$scope.newQpsDiv("proxy");
				$scope.newQpsDiv("proxy");
			}
			if ($scope.server_qps_chart_list.length > 0) {
				$scope.newQpsDiv("server");
				$scope.newQpsDiv("server");
			}
		}

		$scope.proxy_history_chart = [];
		$scope.server_history_chart = [];
		$scope.proxyHistoryShow = false;
		$scope.serverHistoryShow = false;
		$scope.newHistoryDiv = function (type) {
			if (type == "proxy") {
				if ($scope.proxy_history_chart.length > 0) {
					for (var i=0; i<$scope.history_proxy_chart_map.length; i++) {
						if ($scope.history_proxy_chart_map[i] && (!($scope.history_proxy_chart_map[i].isDisposed()))) {
							$scope.history_proxy_chart_map[i].dispose();
						}
					}
					$scope.proxy_history_chart = [];
					$scope.history_proxy_chart_map = [];
					$scope.proxyHistoryShow = false;
					$("#newHistoryBtn").removeClass("active");
					return ;
				}
				$scope.proxyHistoryShow = true;
				var div_num = Math.ceil($scope.proxy_array.length/$scope.history_bunch_size);
				if ($scope.proxy_array.length == 0) {
					alert("无可展示的Proxy，请在集群中添加新的Proxy！");
					return ;
				}
				$("#newHistoryBtn").addClass("active");
				for (var i=0; i<div_num; i++) {
					var proxy_group = [];
					var admin_group = [];
					for (var j=0; j<$scope.history_bunch_size; j++){
						proxy_group.push($scope.proxy_array[i*$scope.history_bunch_size + j].proxy_addr);
						admin_group.push($scope.proxy_array[i*$scope.history_bunch_size + j].admin_addr);
						if (i*$scope.history_bunch_size+j >= $scope.proxy_array.length-1) {
							break ;
						}
					}
					$scope.proxy_history_chart.push({id:i, proxy_addr_list:proxy_group, admin_addr_list:admin_group});
				}
			} else if (type == "server") {
				if ($scope.server_history_chart.length > 0) {
					for (var i=0; i<$scope.history_server_chart_map.length; i++) {
						if ($scope.history_server_chart_map[i] && (!($scope.history_server_chart_map[i].isDisposed()))) {
							$scope.history_server_chart_map[i].dispose();
						}
					}
					$scope.server_history_chart = [];
					$scope.history_server_chart_map = [];
					$scope.serverHistoryShow = false;
					$("#newHistoryBtn").removeClass("active");
					return ;
				}
				$scope.serverHistoryShow = true;
				var group_num = 0;
				for (var i=0; i<$scope.group_array.length; i++) {
					if ($scope.group_array[i].servers.length != 0) {
						group_num++;
					}
				}
				if (group_num == 0) {
					alert("无可展示的Server，请在集群中添加新的Server！");
					return ;
				}
				$("#newHistoryBtn").addClass("active");
				var div_num = Math.ceil(group_num/$scope.history_bunch_size);
				var k = 0;
				for (var i=0; i<div_num; i++) {
					var server_group = [];
					for (var j=0; j<$scope.history_bunch_size; j++){
						if (k >= $scope.group_array.length) {
							break ;
						}
						var group = $scope.group_array[k].servers;
						if (group.length == 0) {
							k++;
							j--;
							continue ; 
						}
						server_group.push(group[0].server);
						k++;
					}
					$scope.server_history_chart.push({id:i, server_addr_list:server_group});
				}
			}
			$scope.getHistory(type);
		}

		$scope.prev_chart_type = "";
		$scope.prev_chart_id = "";
		$scope.showChartContainer = function (type, id){
			$("#chart_container_div").show();
			full_screen_chart_for_iframe.clear();
			full_screen_chart_for_iframe.resize();

			if (type == "" || id == -1) {
				full_screen_chart_orgin = qps_chart;
				qps_chart = full_screen_chart_for_iframe;
				qps_chart.setOption(newChatsOpsConfigEcharts("", 1)); 
				qps_chart.setOption({
					series: [{
						data: $scope.qps_arr,
					}],
					grid: {
						top: '10%',
						bottom: '0%',
					}
				});
				$scope.prev_chart_type = "qpsChart";
			} else {
				if (type == "proxy") {
					full_screen_chart_orgin = $scope.proxy_qps_chart_list[id];
					$scope.proxy_qps_chart_list[id] = full_screen_chart_for_iframe;
					$scope.proxy_qps_chart_list[id].setOption(full_screen_chart_orgin.getOption());
				} else if (type == "server") {
					full_screen_chart_orgin = $scope.server_qps_chart_list[id];
					$scope.server_qps_chart_list[id] = full_screen_chart_for_iframe;
					$scope.server_qps_chart_list[id].setOption(full_screen_chart_orgin.getOption());
				}
				$scope.prev_chart_type = type;
				$scope.prev_chart_id = id;
			}
		}

		$scope.showHistoryChartContainer = function (type, id){
			$("#chart_container_div").show();
			full_screen_chart_for_iframe.clear();
			full_screen_chart_for_iframe.resize();
			
			var my_chart;
			if (type == "" || id == -1) {
				my_chart = $scope.history_main_chart;
			} else {
				if (type == "proxy") {
					my_chart = $scope.history_proxy_chart_map[id];
				} else if (type == "server") {
					 my_chart = $scope.history_server_chart_map[id];
				} else if (type == "cmd_info"){
					my_chart = $scope.history_chart_map[id];
				} else if (type == "tp999_all") {
					my_chart = $scope.history_chart_tp999_all[id];
				} else if (type == "qps_all") {
					my_chart = $scope.history_chart_qps_all[id];
				} else if (type == "proxy_all_cmd_tp999") {
					my_chart = $scope.history_chart_map[type];
				}
			}
			var option = my_chart.getOption();
			option.dataZoom = [
				{
					type: 'inside',
					show: false,
					xAxisIndex: [0],
				}
			];
			full_screen_chart_for_iframe.setOption(option);
			$(window).resize(function(){
				if (!($("#chart_container").is(":hidden"))) {
					full_screen_chart_for_iframe.resize();
				}
			});
			$scope.prev_chart_type = "history";
		}

		$scope.exitChartContainer = function (){
			full_screen_chart_owner = ""; 
			if ($scope.prev_chart_type == ""){
				;
			}else if ($scope.prev_chart_type == "qpsChart") {
				qps_chart = full_screen_chart_orgin;
				qps_chart.setOption({
					series: [{
						data: $scope.qps_arr,
					}],
					grid: {
						top: '10%',
						bottom: '0%',
					}
				});
				qps_chart.resize();
			} else if ($scope.prev_chart_type == "history") {
				;
			} else {
				if ($scope.prev_chart_type == "proxy"){				
					full_screen_chart_orgin.setOption(full_screen_chart_for_iframe.getOption());
					$scope.proxy_qps_chart_list[$scope.prev_chart_id] = full_screen_chart_orgin;
				} else if ($scope.prev_chart_type == "server") {
					full_screen_chart_orgin.setOption(full_screen_chart_for_iframe.getOption());
					$scope.server_qps_chart_list[$scope.prev_chart_id] = full_screen_chart_orgin;
				}
				full_screen_chart_orgin.resize();
			}
			$("#chart_container_div").hide();
		}

		$scope.refresh_proxy_tree = function () {
			for (var i=0; i<codis_treeview.length; i++){
				if (codis_treeview[i].text == $scope.codis_name) {
					codis_treeview[i].nodes[0].nodes = [];
					for (var j=0; j<$scope.proxy_array.length; j++) {
						codis_treeview[i].nodes[0].nodes.push({
							text: $scope.proxy_array[j].proxy_addr,
							type: 'proxynode',
							href: '#' + $scope.codis_name,
							codisname: $scope.codis_name,
							proxytoken: $scope.proxy_array[j].token,
						});
					}
					return ;
				}
			}
		}

		$scope.refreshHistoryCharts = function () {
			if ($scope.proxyHistoryShow) {
				$scope.newHistoryDiv("proxy");
				$scope.newHistoryDiv("proxy");
			}
			if ($scope.serverHistoryShow) {
				$scope.newHistoryDiv("server");
				$scope.newHistoryDiv("server");
			}    
		}

		$scope.checkExpendNodeChange = function (codisname, operate) {
			//如果没有节点展开直接返回，不需要调整节点id
			if ($scope.expanded_nodeid.parentid == -1) {
				return ;
			}
			var expandedNodeText = $('#treeview_codis_list').treeview('getNode',$scope.expanded_nodeid.parentid).text;
			for (var i=0; i<codis_treeview.length; i++) {
				if (codis_treeview[i].text == codisname) {
					//如果展开节点在前，那么展开状态不发生改变
					if (codis_treeview[i].text == expandedNodeText) {
						break;
					} else {
						if (operate == "add") {
							if ($scope.expanded_nodeid.parentid >= 0) {
								$scope.expanded_nodeid.parentid++;
							}
							if ($scope.expanded_nodeid.proxyid >= 0) {
								$scope.expanded_nodeid.proxyid++;
							}
							if ($scope.expanded_nodeid.serverid >= 0) {
								$scope.expanded_nodeid.serverid++;
							}
						} else if (operate == "dec") {
							if ($scope.expanded_nodeid.parentid >= 0) {
								$scope.expanded_nodeid.parentid--;
							}
							if ($scope.expanded_nodeid.proxyid >= 0) {
								$scope.expanded_nodeid.proxyid--;
							}
							if ($scope.expanded_nodeid.serverid >= 0) {
								$scope.expanded_nodeid.serverid--;
							}
						}
					}
					break;
				} else if (codis_treeview[i].text == expandedNodeText) {
					break;
				}
			}
		}

		$scope.createProxy = function (proxy_addr) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(proxy_addr)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/proxy/create/" + xauth + "/" + proxy_addr, codis_name);
				$http.put(url).then(function () {
					$scope.treeview_proxy_ischange = true;
					$scope.proxy_num_ischange = true;
					$scope.treeview_proxy_add = true;
					$scope.checkExpendNodeChange(codis_name, "add");
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		$scope.removeProxy = function (proxy, force) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var prefix = "";
				if (force) {
					prefix = "[FORCE] ";
				}
				alertAction(prefix + "Remove and Shutdown proxy: " + toJsonHtml(proxy), function () {
					var xauth = genXAuth(codis_name);
					var value = 0;
					if (force) {
						value = 1;
					}
					var url = concatUrl("/api/topom/proxy/remove/" + xauth + "/" + proxy.token + "/" + value, codis_name);
					$http.put(url).then(function () {
						$scope.treeview_proxy_ischange = true;
						$scope.proxy_num_ischange = true;
						$scope.treeview_proxy_add = false;
						$scope.checkExpendNodeChange(codis_name, "dec");
						$scope.refreshStats();
					}, function (failedResp) {
						alertErrorResp(failedResp);
					});
				});
			}
		}

		$scope.reinitProxy = function (proxy) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var confused = [];
				for (var i = 0; i < $scope.group_array.length; i ++) {
					var group = $scope.group_array[i];
					var ha_real_master = -1;
					for (var j = 0; j < group.servers.length; j ++) {
						if (group.servers[j].ha_status == "ha_real_master") {
							ha_real_master = j;
						}
					}
					if (ha_real_master >= 0) {
						confused.push({group: group.id, logical_master: group.servers[0].server, ha_real_master: group.servers[ha_real_master].server});
					}
				}
				if (confused.length == 0) {
					alertAction("Reinit and Start proxy: " + toJsonHtml(proxy), function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/proxy/reinit/" + xauth + "/" + proxy.token, codis_name);
						$http.put(url).then(function () {
							$scope.refreshStats();
						}, function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				} else {
					var prompts = toJsonHtml(proxy);
					prompts += "\n\n";
					prompts += "HA: real master & logical master area conflicting: " + toJsonHtml(confused);
					prompts += "\n\n";
					prompts += "Please fix these before resync proxy-[" + proxy.token + "].";
					alertAction2("Reinit and Start proxy: " + prompts, function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/proxy/reinit/" + xauth + "/" + proxy.token, codis_name);
						$http.put(url).then(function () {
							$scope.refreshStats();
						}, function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				}
			}
		}

		$scope.createGroup = function (group_id) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(group_id)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/group/create/" + xauth + "/" + group_id, codis_name);
				$http.put(url).then(function () {
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		$scope.removeGroup = function (group_id) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/group/remove/" + xauth + "/" + group_id, codis_name);
				$http.put(url).then(function () {
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		/*$scope.resyncGroup = function (group) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var o = {};
				o.id = group.id;
				o.servers = [];
				var ha_real_master = -1;
				for (var j = 0; j < group.servers.length; j++) {
					o.servers.push(group.servers[j].server);
					if (group.servers[j].ha_status == "ha_real_master") {
						ha_real_master = j;
					}
				}
				if (ha_real_master < 0) {
					alertAction("Resync Group-[" + group.id + "]: " + toJsonHtml(o), function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/group/resync/" + xauth + "/" + group.id, codis_name);
						$http.put(url).then(function () {
							$scope.refreshStats();
						}, function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				} else {
					var prompts = toJsonHtml(o);
					prompts += "\n\n";
					prompts += "HA: server[" + ha_real_master + "]=" + group.servers[ha_real_master].server + " should be the real group master, do you really want to resync group-[" + group.id + "] ??";
					alertAction2("Resync Group-[" + group.id + "]: " + prompts, function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/group/resync/" + xauth + "/" + group.id, codis_name);
						$http.put(url).then(function () {
							$scope.refreshStats();
						}, function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				}
			}
		}*/

		/*$scope.resyncGroupAll = function() {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var ha_real_master = -1;
				var gids = [];
				for (var i = 0; i < $scope.group_array.length; i ++) {
					var group = $scope.group_array[i];
					for (var j = 0; j < group.servers.length; j++) {
						if (group.servers[j].ha_status == "ha_real_master") {
							ha_real_master = j;
						}
					}
					gids.push(group.id);
				}
				if (ha_real_master < 0) {
					alertAction("Resync All Groups: group-[" + gids + "]", function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/group/resync-all/" + xauth, codis_name);
						$http.put(url).then(function () {
							$scope.refreshStats();
						}, function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				} else {
					alertAction2("Resync All Groups: group-[" + gids + "] (in conflict with HA)", function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/group/resync-all/" + xauth, codis_name);
						$http.put(url).then(function () {
							$scope.refreshStats();
						}, function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				}
			}
		}*/

		$scope.resyncSentinels = function () {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var servers = [];
				for (var i = 0; i < $scope.sentinel_servers.length; i ++) {
					servers.push($scope.sentinel_servers[i].server);
				}
				var confused = [];
				for (var i = 0; i < $scope.group_array.length; i ++) {
					var group = $scope.group_array[i];
					var ha_real_master = -1;
					for (var j = 0; j < group.servers.length; j ++) {
						if (group.servers[j].ha_status == "ha_real_master") {
							ha_real_master = j;
						}
					}
					if (ha_real_master >= 0) {
						confused.push({group: group.id, logical_master: group.servers[0].server, ha_real_master: group.servers[ha_real_master].server});
					}
				}
				if (confused.length == 0) {
					alertAction("Resync All Sentinels: " + toJsonHtml(servers), function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/sentinels/resync-all/" + xauth, codis_name);
						$http.put(url).then(function () {
							$scope.refreshStats();
						}, function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				} else {
					var prompts = toJsonHtml(servers);
					prompts += "\n\n";
					prompts += "HA: real master & logical master area conflicting: " + toJsonHtml(confused);
					prompts += "\n\n";
					prompts += "Please fix these before resync sentinels.";
					alertAction2("Resync All Sentinels: " + prompts, function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/sentinels/resync-all/" + xauth, codis_name);
						$http.put(url).then(function () {
							$scope.refreshStats();
						}, function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				}
			}
		}
		
		$scope.CancelSentinels = function () {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var servers = [];
				for (var i = 0; i < $scope.sentinel_servers.length; i ++) {
					servers.push($scope.sentinel_servers[i].server);
				}

				alertAction("Off All Sentinels: " + toJsonHtml(servers), function () {
					var xauth = genXAuth(codis_name);
					var url = concatUrl("/api/topom/sentinels/remove-all/" + xauth, codis_name);
					$http.put(url).then(function () {
						$scope.refreshStats();
					}, function (failedResp) {
						alertErrorResp(failedResp);
					});
				});
			}
		}

		$scope.addSentinel = function (server_addr) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(server_addr)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/sentinels/add/" + xauth + "/" + server_addr, codis_name);
				$http.put(url).then(function () {
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		$scope.delSentinel = function (sentinel, force) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var prefix = "";
				if (force) {
					prefix = "[FORCE] ";
				}
				alertAction(prefix + "Remove sentinel " + sentinel.server, function () {
					var xauth = genXAuth(codis_name);
					var value = 0;
					if (force) {
						value = 1;
					}
					var url = concatUrl("/api/topom/sentinels/del/" + xauth + "/" + sentinel.server + "/" + value, codis_name);
					$http.put(url).then(function () {
						$scope.refreshStats();
					}, function (failedResp) {
						alertErrorResp(failedResp);
					});
				});
			}
		}

		$scope.rebalanceAllSlots = function() {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/slots/rebalance/" + xauth + "/0", codis_name);
				$http.put(url).then(function (resp) {
					var actions = [];
					for (var i = 0; i < $scope.group_array.length; i ++) {
						var g = $scope.group_array[i];
						var slots = [], beg = 0, end = -1;
						for (var sid = 0; sid < 1024; sid ++) {
							if (resp.data[sid] == g.id) {
								if (beg > end) {
									beg = sid; end = sid;
								} else if (end == sid - 1) {
									end = sid;
								} else {
									slots.push("[" + beg + "," + end + "]");
									beg = sid; end = sid;
								}
							}
						}
						if (beg <= end) {
							slots.push("[" + beg + "," + end + "]");
						}
						if (slots.length == 0) {
							continue;
						}
						actions.push("group-[" + g.id + "] <== " + slots);
					}
					alertAction("Preview of Auto-Rebalance: " + toJsonHtml(actions), function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/slots/rebalance/" + xauth + "/1", codis_name);
						$http.put(url).then(function () {
							$scope.refreshStats();
						}, function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		$scope.refresh_server_tree = function () {
			for (var i=0; i<codis_treeview.length; i++){
				if (codis_treeview[i].text == $scope.codis_name) {
					codis_treeview[i].nodes[1].nodes = [];
					for (var j=0; j<$scope.group_array.length; j++) {
						for (var k=0; k<$scope.group_array[j].servers.length; k++){
							codis_treeview[i].nodes[1].nodes.push({
								text: $scope.group_array[j].servers[k].server + " [G" + $scope.group_array[j].id + "]",
								serveraddr: $scope.group_array[j].servers[k].server,
								type: 'servernode',
								href: "#" + $scope.codis_name,
								codisname: $scope.codis_name,
								groupid: $scope.group_array[j].id,
							});
						}
					}
					return ;
				}
			}
		}

		$scope.addGroupServer = function (group_id, server_addr) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(group_id) && isValidInput(server_addr)) {
				var xauth = genXAuth(codis_name);
				var datacenter = "";
					
				var suffix = "";
				if (datacenter != "") {
					suffix = "/" + datacenter;
				}
				var url = concatUrl("/api/topom/group/add/" + xauth + "/" + group_id + "/" + server_addr + suffix, codis_name);
				$http.put(url).then(function () {
					$scope.treeview_server_ischange = true;
					$scope.server_num_ischange = true;
					$scope.checkExpendNodeChange(codis_name, "add");
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		$scope.delGroupServer = function (group_id, server_addr) {
			var codis_name = $scope.codis_name;

			if (isValidInput(codis_name) && isValidInput(group_id) && isValidInput(server_addr)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/group/del/" + xauth + "/" + group_id + "/" + server_addr, codis_name);
				$http.put(url).then(function () {
					$scope.treeview_server_ischange = true;
					$scope.server_num_ischange = true;
					$scope.checkExpendNodeChange(codis_name, "dec");
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			} else {
				alert("Delete: " + server_addr + " failed");
			}
		}

		$scope.removeGroupServer = function (is_kill){
			var group_id = $scope.del_group_id;
			var server_addr = $scope.server_addr;
			var codis_name = $scope.codis_name;

			if (isValidInput(codis_name) && isValidInput(group_id) && isValidInput(server_addr)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/group/del/" + $scope.store_type + "/" + xauth + "/" + group_id + "/" + server_addr, codis_name);
				$http.put(url).then(function () {
					if (is_kill) {
						$scope.killGroupServer();
					} else {
						closeDialog("confirm");
					}
					$scope.treeview_server_ischange = true;
					$scope.server_num_ischange = true;
					$scope.checkExpendNodeChange(codis_name, "dec");
					$scope.refreshStats();
				}, function (failedResp) {
					closeDialog("confirm");
					alertErrorResp(failedResp);
				});
			} else {
				$("#delLoadingChart").hide();
				closeDialog("confirm");
			}
		}

		$scope.killGroupServer = function (){
			var group_id = $scope.del_group_id;
			var server_addr = $scope.server_addr;
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(group_id) && isValidInput(server_addr)) {
				alertAction("kill server " + server_addr + " and clear all data", function () {
					$("#delLoadingChart").show();
					var url = concatUrl("/deploy/" + $scope.del_server_type + "/destroy/" + $scope.server_addr);
					$http.put(url).then(function (resp) {
						$("#delLoadingChart").hide();
						closeDialog("confirm");
						if (resp.data == "\"Success\"") {
							resp.data = "Delete Server " + server_addr + " Success !";
							alertSuccessResp(resp);
						} else {
							alertErrorResp(resp);
						}
					}, function (failedResp) {
						$("#delLoadingChart").hide();
						closeDialog("confirm");
						alertErrorResp(failedResp);
					});
				});
			}
		}

		$scope.promoteServer = function (group, server_addr) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(server_addr)) {
				var o = {};
				o.id = group.id;
				o.servers = [];
				var ha_real_master = -1;
				for (var j = 0; j < group.servers.length; j++) {
					o.servers.push(group.servers[j].server);
					if (group.servers[j].ha_status == "ha_real_master") {
						ha_real_master = j;
					}
				}
				if (ha_real_master < 0 || group.servers[ha_real_master].server == server_addr) {
					alertAction("Promote server " + server_addr + " from Group-[" + group.id + "]: " + toJsonHtml(o), function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/group/promote/" + xauth + "/" + group.id + "/" + server_addr + "/0", codis_name);
						$http.put(url).then(function () {
							$scope.treeview_server_ischange = true;
							$scope.refreshStats();
						}, function (failedResp) {
							var suffix = "/1";
							if (failedResp.status == 404) {
								var url = concatUrl("/api/topom/group/promote/" + xauth + "/" + group.id + "/" + server_addr, codis_name);
								$http.put(url).then(function () {
									$scope.treeview_server_ischange = true;
									$scope.refreshStats();
								}, function (failedResp) {
									alertErrorResp(failedResp);
								});
							} else {
								alertDangerAction("error: " + toJsonHtml(failedResp.data) + "\r\n是否强制主从切换？", function () {
									var url = concatUrl("/api/topom/group/promote/" + xauth + "/" + group.id + "/" + server_addr + "/1", codis_name);
									$http.put(url).then(function () {
										$scope.treeview_server_ischange = true;
										$scope.refreshStats();
									}, function (failedResp) {
										alertErrorResp(failedResp);
									});
								});
							}
						});
					});
				} else {
					var prompts = toJsonHtml(o);
					prompts += "\n\n";
					prompts += "HA: server[" + ha_real_master + "]=" + group.servers[ha_real_master].server + " should be the real group master, do you really want to promote " + server_addr + " ??";
					alertAction2("Promote server " + server_addr + " from Group-[" + group.id + "]: " + prompts, function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/group/promote/" + xauth + "/" + group.id + "/" + server_addr + "/0", codis_name);
						$http.put(url).then(function () {
							$scope.treeview_server_ischange = true;
							$scope.refreshStats();
						}, function (failedResp) {
							alertDangerAction("error: " + toJsonHtml(failedResp.data) + "\r\n是否强制主从切换？", function () {
								var url = concatUrl("/api/topom/group/promote/" + xauth + "/" + group.id + "/" + server_addr + "/1", codis_name);
								$http.put(url).then(function () {
									$scope.treeview_server_ischange = true;
									$scope.refreshStats();
								}, function (failedResp) {
									alertErrorResp(failedResp);
								});
							});
						});
					});
				}
			}
		}

		$scope.promoteCommit = function (group_id) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(group_id)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/group/promote-commit/" + xauth + "/" + group_id, codis_name);
				$http.put(url).then(function () {
					$scope.treeview_server_ischange = true;
					$scope.server_num_ischange = true;
					$scope.checkExpendNodeChange(codis_name, "dec");
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		$scope.createSyncAction = function (server_addr) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(server_addr)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/group/action/create/" + xauth + "/" + server_addr, codis_name);
				$http.put(url).then(function () {
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		$scope.removeSyncAction = function (server_addr) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(server_addr)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/group/action/remove/" + xauth + "/" + server_addr, codis_name);
				$http.put(url).then(function () {
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		$scope.createSlotAction = function (slot_id, group_id) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(slot_id) && isValidInput(group_id)) {
				alertAction("Migrate Slots-[" + slot_id + "] to Group-[" + group_id + "]", function () {
					var xauth = genXAuth(codis_name);
					var url = concatUrl("/api/topom/slots/action/create/" + xauth + "/" + slot_id + "/" + group_id , codis_name);
					$http.put(url).then(function () {
						$scope.refreshStats();
					}, function (failedResp) {
						alertErrorResp(failedResp);
					});
				});
			}
		}

		$scope.createSlotActionSome = function (slots_num, group_from, group_to) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(slots_num) && isValidInput(group_from) && isValidInput(group_to)) {
				alertAction("Migrate " + slots_num + " Slots from Group-[" + group_from + "] to Group-[" + group_to + "]", function () {
					var xauth = genXAuth(codis_name);
					var url = concatUrl("/api/topom/slots/action/create-some/" + xauth + "/" + group_from + "/" + group_to + "/" + slots_num, codis_name);
					$http.put(url).then(function () {
						$scope.refreshStats();
					}, function (failedResp) {
						alertErrorResp(failedResp);
					});
				});
			}
		}

		$scope.createSlotActionRange = function (slot_beg, slot_end, group_id) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(slot_beg) && isValidInput(slot_end) && isValidInput(group_id)) {
				alertAction("Migrate Slots-[" + slot_beg + "," + slot_end + "] to Group-[" + group_id + "]", function () {
					var xauth = genXAuth(codis_name);
					var url = concatUrl("/api/topom/slots/action/create-range/" + xauth + "/" + slot_beg + "/" + slot_end + "/" + group_id, codis_name);
					$http.put(url).then(function () {
						$scope.refreshStats();
					}, function (failedResp) {
						alertErrorResp(failedResp);
					});
				});
			}
		}

		$scope.removeSlotAction = function (slot_id) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(slot_id)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/slots/action/remove/" + xauth + "/" + slot_id, codis_name);
				$http.put(url).then(function () {
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		$scope.slotsActionCancel = function (){
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/slots/action/remove-all/" + xauth, codis_name);
				$http.put(url).then(function () {
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		} 

		$scope.updateSlotActionDisabled = function (value) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/slots/action/disabled/" + xauth + "/" + value, codis_name);
				$http.put(url).then(function () {
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		$scope.updateSlotActionInterval = function (value) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/slots/action/interval/" + xauth + "/" + value, codis_name);
				$http.put(url).then(function () {
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		/*$scope.resyncSentinels = function () {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var servers = [];
				for (var i = 0; i < $scope.sentinel_servers.length; i ++) {
					servers.push($scope.sentinel_servers[i].server);
				}
				var confused = [];
				for (var i = 0; i < $scope.group_array.length; i ++) {
					var group = $scope.group_array[i];
					var ha_real_master = -1;
					for (var j = 0; j < group.servers.length; j ++) {
						if (group.servers[j].ha_status == "ha_real_master") {
							ha_real_master = j;
						}
					}
					if (ha_real_master >= 0) {
						confused.push({group: group.id, logical_master: group.servers[0].server, ha_real_master: group.servers[ha_real_master].server});
					}
				}
				if (confused.length == 0) {
					alertAction("Resync All Sentinels: " + toJsonHtml(servers), function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/sentinels/resync-all/" + xauth, codis_name);
						$http.put(url).then(function () {
							$scope.refreshStats();
						}, function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				} else {
					var prompts = toJsonHtml(servers);
					prompts += "\n\n";
					prompts += "HA: real master & logical master area conflicting: " + toJsonHtml(confused);
					prompts += "\n\n";
					prompts += "Please fix these before resync sentinels.";
					alertAction2("Resync All Sentinels: " + prompts, function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/sentinels/resync-all/" + xauth, codis_name);
						$http.put(url).then(function () {
							$scope.refreshStats();
						}, function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				}
			}
		}*/

		$scope.addSentinel = function (server_addr) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name) && isValidInput(server_addr)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/sentinels/add/" + xauth + "/" + server_addr, codis_name);
				$http.put(url).then(function () {
					$scope.refreshStats();
				}, function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		$scope.delSentinel = function (sentinel, force) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var prefix = "";
				if (force) {
					prefix = "[FORCE] ";
				}
				alertAction(prefix + "Remove sentinel " + sentinel.server, function () {
					var xauth = genXAuth(codis_name);
					var value = 0;
					if (force) {
						value = 1;
					}
					var url = concatUrl("/api/topom/sentinels/del/" + xauth + "/" + sentinel.server + "/" + value, codis_name);
					$http.put(url).then(function () {
						$scope.refreshStats();
					}, function (failedResp) {
						alertErrorResp(failedResp);
					});
				});
			}
		}

		$scope.getHistoryCmdResponseList = function () {
			var codis_name = $scope.codis_name;
			var xauth = genXAuth(codis_name);
			var table_name = getTableName("dashboard_", $scope.codis_addr);
			table_name = table_name + "_cmd_info";
			//var sql_command = "SHOW TAG VALUES FROM " + table_name + " WITH KEY=cmd_name";
			var sql_command = getShowSqlCommand(table_name);
			var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, codis_name);
			$scope.showReadCmdInfo = false;
			$scope.select_response_cmd = "";
			if ($scope.cmd_response_chart_list.length > 0) {
				$scope.select_response_cmd = $scope.cmd_response_chart_list[0];
			}
			$scope.cmd_response_time_list = [];
			$scope.cmd_response_chart_list = [];

			$http.get(url).then(function (resp) {
				if (resp.data.Results && resp.data.Results[0].Series) {
					$scope.cmd_response_time_list = [];
					var cmd_list = resp.data.Results[0].Series[0].values;
					var all_cmd_index = -1;
					for (var i=0; i<cmd_list.length; i++) {
						$scope.cmd_response_time_list.push(cmd_list[i][1]);
						$scope.showReadCmdInfo = true;
						if (cmd_list[i][1] == $scope.select_response_cmd) {
							$scope.cmd_response_chart_list.push(cmd_list[i][1]);
						}
						if (cmd_list[i][1] == "ALL") {
							all_cmd_index = i;
						}
					}
					if ($scope.cmd_response_chart_list.length <= 0 && all_cmd_index >= 0) {
						$scope.cmd_response_chart_list.push(cmd_list[all_cmd_index][1]);
					}
				}
				if ($scope.cmd_response_time_list.length > 0) {
					$scope.getHistory("response");
					//$scope.getHistory("proxy");
				} else {
					$scope.getHistory("");
				}
			},function (failedResp) {
				// 请求失败执行代码 
				$scope.getHistory("");   
			});
		}

		/*$scope.showReadCmdPerSecInfo = function () {
			if ($scope.showReadCmdInfo) {
				$("#showReadCmdInfo").removeClass("active");
			} else {
				$("#showReadCmdInfo").addClass("active");
			}
			$scope.showReadCmdInfo = !($scope.showReadCmdInfo);
			if ($scope.showReadCmdInfo) {
				$scope.getHistory("ReadCmdPerSec");
			}
		}*/

		$scope.selectHistoryCmd = function (cmd) {
			$scope.cmd_response_chart_list = [];
			$scope.cmd_response_chart_list.push(cmd);
			$scope.getHistory("response");
		}

		//operate of history charts
		$scope.search_id = "";
		$scope.show_type = "";
		$scope.search_type = "";
		$scope.data_unit = "";
		$scope.time_id = "";
		
		$scope.fillHistory = function(){  
			if ($scope.search_id == "") {
				$scope.setType("Qps", "ops_qps", "count(次)/s");
				$scope.search_id = "Qps";
				$scope.show_type = "QPS";
				$scope.search_type = "ops_qps";
				$scope.data_unit = "count(次)/s";
			} else {
				$scope.setType($scope.search_id, $scope.search_type, $scope.data_unit);
			}

			setColor("", $scope.search_id);
			setColor("", $scope.time_id);
			initTimer(6, 0);
			setColor($scope.time_id, "");
			//$scope.getHistory("");
		}

		$scope.history_chart_type = "proxy";
		$scope.setType = function (id, type, count) {
			if($scope.search_id == id){
				return ;
			}
			$scope.showReadCmdInfo = false;
			setColor($scope.search_id, id);
			$scope.search_type = type;
			$scope.search_id = id;
			$scope.show_type = id;
			if (id == "Qps") {
				$scope.show_type = "QPS";
			}
			$scope.data_unit = count;
			var chart_name = "";
			if (id=="Sessions" || id=="Ops_Fails") {
				if ($scope.server_history_chart.length > 0) {
					$scope.server_history_chart = [];
					$scope.newHistoryDiv('proxy');
				}
				
				$scope.serverHistoryShow = false;
				$scope.history_chart_type = "proxy";
			} else if (id=="Cache_Memory" || id=="Cache_Keys" || id=="Hit_Rate" || id=="Server_Mem" || id=="Server_SSD" || id=="Server_Qps") {
				if ($scope.proxy_history_chart.length > 0) {
					$scope.proxy_history_chart = [];
					$scope.newHistoryDiv('server');
				}
				
				$scope.proxyHistoryShow = false;
				$scope.history_chart_type = "server";
			} else if (id == "Qps") {
				if ($scope.server_history_chart.length > 0) {
					$scope.server_history_chart = [];
					$scope.newHistoryDiv('proxy');
				}

				$("#all_response_cmd").select2();
				$scope.history_chart_type = "proxy";
				$scope.getHistoryCmdResponseList();
				return;
			} else if (id == "QPS") {
				//$scope.history_chart_type = "proxy";
				if ($scope.proxy_history_chart.length > 0 ) {
					$scope.newHistoryDiv("proxy");
				}
				if ($scope.server_history_chart.length > 0 ) {
					$scope.newHistoryDiv("server");
				}
				$scope.history_chart_type = "";
				$scope.getHistory(id);
				return;
			}
			
			$scope.getHistory("");
			if (id=="Hit_Rate") {
				$scope.getHistory("CacheReadQPS");
			}
		}

		$scope.quickSearch = function (start, end, timeId) {
			setColor($scope.time_id, timeId);
			$scope.time_id = timeId;
			initTimer(start, end);
			$scope.getHistoryByType();
		}

		$scope.getHistoryByType = function () {
			if ($scope.search_type == "more") {
				$scope.getHistory('QPS');
			} else if ($scope.search_type == "ops_qps") {
				//$scope.getHistory('response');
				$scope.getHistoryCmdResponseList();
			} else {
				$scope.getHistory("");
			}
			if ($scope.search_type == "cache_hit_rate") {
				$scope.getHistory("CacheReadQPS");
			}
		}

		$scope.getHistory = function (chartid) {
			var codis_name = $scope.codis_name;
			var xauth = genXAuth(codis_name);
			var start_seconds = getTimeById('start_time');
			var end_seconds = getTimeById('end_time');
			var table_name = getTableName("dashboard_", $scope.codis_addr);
			var table_name_cmd_info = ""; 
			var search_type = $scope.search_type;

			if (start_seconds >= end_seconds) {
				alert("开始时间必须小于结束时间，请重新设置！");
				return ;
			}

			if (chartid != "proxy" && chartid != "server" && chartid != "response" && chartid != 'CacheReadQPS' && chartid != 'QPS' && chartid != 'change_tp_cmd'){
				var sql_command = getSelectSqlCommand(start_seconds, end_seconds, table_name, search_type, "", $scope.influxdb_period_s);
				var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, codis_name);
				$scope.getAndShowHistory("historyCharts", url, search_type, "main_chart", 0);
			}

			if (chartid == 'CacheReadQPS') {
				var search_type = "cache_read_qps";
				var sql_command = getSelectSqlCommand(start_seconds, end_seconds, table_name, search_type, "", $scope.influxdb_period_s);
				var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, codis_name);
				$scope.getAndShowHistory("read_cmd_per_sec", url, search_type, "cache_read_qps", 0);
				return;
			}

			if (chartid == 'QPS') {
				$scope.showAllCmdInfo(codis_name, xauth, start_seconds, end_seconds);
			}

			if (chartid == "change_tp_cmd") {
				$scope.getAllCmdInfoChart("proxy_all_cmd_tp999", codis_name, xauth, start_seconds, end_seconds, $scope.more_tp_cmd);
				return;
			}

			if (("response" == chartid)) {
				for (var i = 0; i < $scope.cmd_response_chart_list.length; i++) {
					var table_name = getTableName("dashboard_", $scope.codis_addr);
					table_name = table_name + "_cmd_info";
					var search_type = "qps,tp90,tp99,tp999,tp9999,tp100,avg,fails,redis_errtype";
					var sql_command = getSelectSqlCommand(start_seconds, end_seconds, table_name, search_type, "cmd_name='" + $scope.cmd_response_chart_list[i] + "'", $scope.influxdb_period_s);
					var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, codis_name);
					$scope.getDashboardCmdHistoryChart(search_type, url, $scope.cmd_response_chart_list[i]);	
				}
			}

			if ($scope.proxyHistoryShow) {
				for (var i=0; i<$scope.proxy_history_chart.length; i++) {
					table_name = "";
					table_name_cmd_info = "";
					for (var j=0; j<$scope.history_bunch_size; j++) {
						var proxy_addr = $scope.proxy_history_chart[i].proxy_addr_list[j];
						if (j >= $scope.proxy_history_chart[i].proxy_addr_list.length - 1) {
							table_name += getTableName("proxy_", proxy_addr);
							table_name_cmd_info += getTableName("proxy_", proxy_addr) + "_cmd_info";
							break ;
						}
						table_name += getTableName("proxy_", proxy_addr) + ",";
						table_name_cmd_info += getTableName("proxy_", proxy_addr) + "_cmd_info,";
					}

					var proxy_search_type = search_type;
					var sql_cmd_where = "";
					if ($scope.cmd_response_chart_list.length > 0) {
						proxy_search_type = "qps";
						table_name = table_name_cmd_info;
						sql_cmd_where = "cmd_name='" + $scope.cmd_response_chart_list[0] + "'";
					}

					var sql_command = getSelectSqlCommand(start_seconds, end_seconds, table_name, proxy_search_type, sql_cmd_where, $scope.influxdb_period_s);
					var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, codis_name);
					$scope.getAndShowHistory("proxy_history_chart" + i, url, search_type, "proxy", i);

					if ($scope.search_id == "Qps") {
						var sql_command = getSelectSqlCommand(start_seconds, end_seconds, table_name_cmd_info, "tp999", "cmd_name='" + $scope.cmd_response_chart_list[0] + "'", $scope.influxdb_period_s);
						var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, codis_name);
						$scope.getAndShowHistory("proxy_tp999_chart" + i, url, "tp999", "tp999_all", i);

						var sql_command = getSelectSqlCommand(start_seconds, end_seconds, table_name_cmd_info, "fails", "cmd_name='" + $scope.cmd_response_chart_list[0] + "'", $scope.influxdb_period_s);
						var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, codis_name);
						$scope.getAndShowHistory("proxy_fails_chart" + i, url, "qps", "qps_all", i);
					}
				}
			} else if ($scope.serverHistoryShow) {
				for (var i=0; i<$scope.server_history_chart.length; i++) {
					table_name = "";
					var group_len = $scope.server_history_chart[i].server_addr_list.length;
					for (var j=0; j<group_len; j++) {
						var server_addr = $scope.server_history_chart[i].server_addr_list[j]
						if (j >= group_len-1) {
							table_name += getTableName("server_", server_addr);
							break ;
						}
						table_name += getTableName("server_", server_addr) + ",";
					}
					var sql_command = getSelectSqlCommand(start_seconds, end_seconds, table_name, search_type, "", $scope.influxdb_period_s);
					var url = concatUrl("/api/influxdb/query/" + xauth + "/" + sql_command, codis_name);
					$scope.getAndShowHistory("server_history_chart" + i, url, search_type, "server", i);
				}
			}
		}

		$scope.getDashboardCmdHistoryChart = function (search_type, url, cmd_name) {
			var chart_id = "";
			$http.get(url).then(function (resp) {
				if (resp.data.Results && resp.data.Results[0].Series) {
					chart_id = "historyCharts";
					var chart = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, "qps", search_type);
					$scope.addDataZoomEvent(chart);

					$scope.history_main_chart = chart;
					chart_id = "dashboard_cmd_reponse_time";
					$scope.history_chart_map[chart_id] = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, "tp90,tp99,tp999,tp9999,tp100,avg",  search_type, "tp999,avg");
					$scope.addDataZoomEvent($scope.history_chart_map[chart_id]);

					chart_id = "dashboard_cmd_fails";
					$scope.history_chart_map[chart_id] = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, "fails,redis_errtype", search_type);	
					$scope.addDataZoomEvent($scope.history_chart_map[chart_id]);
				} else {
					clearHistorychart("historyCharts");
					clearHistorychart("dashboard_cmd_reponse_time");
					clearHistorychart("dashboard_cmd_fails");
					alert($scope.cmd_response_chart_list[0] + "数据获取失败！");
					return;
				}
			},function (failedResp) {
				// 请求失败执行代码
				clearHistorychart("historyCharts");
				clearHistorychart("dashboard_cmd_reponse_time");
				clearHistorychart("dashboard_cmd_fails");
				alert($scope.cmd_response_chart_list[0] + "数据获取失败！");
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
					if ($scope.search_id == "Qps") {
						$scope.getHistory("");
						$scope.getHistory("response");
					} else {
						$scope.getHistory("");
					}
					if ($scope.search_id == "Hit_Rate") {
						$scope.getHistory("CacheReadQPS");
					}
				}
			});
		}

		$scope.getAndShowHistory = function(chart_id, url, search_type, chart_type, chart_map_id){
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
					if (chart_type == "main_chart" || chart_type == "cache_read_qps") {
						table_name = search_type;
					}
					var chart = showHistory(chart_id, resp.data.Results[0].Series[0], search_type, table_name);
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
							if ($scope.search_id == "Qps") {
								$scope.getHistory("");
								$scope.getHistory("response");
							} else {
								$scope.getHistory("");
							}
							if ($scope.search_id == "Hit_Rate") {
								$scope.getHistory("CacheReadQPS");
							}
						}
					});
					if (chart_type == "proxy"){
						$scope.history_proxy_chart_map[chart_map_id] = chart;
					} else if (chart_type == "server") {
						$scope.history_server_chart_map[chart_map_id] = chart;
					} else if (chart_type == "main_chart") {
						$scope.history_main_chart = chart;
					} else if (chart_type == "cache_read_qps") {
						$scope.history_chart_map[chart_id] = chart;
					} else if (chart_type == "tp999_all") {
						$scope.history_chart_tp999_all[chart_map_id] = chart;
					} else if (chart_type == "qps_all") {
						$scope.history_chart_qps_all[chart_map_id] = chart;
					}
				} else {
					alert(search_type + "数据获取失败!");
					clearHistorychart(chart_id);   
				}
			},function (failedResp) {
				// 请求失败执行代码
				clearHistorychart(chart_id);    
			});
		}

		$scope.showAllCmdInfo = function (codis_name, xauth, start_seconds, end_seconds) {
			$scope.getAllCmdInfoChart("historyCharts", codis_name, xauth, start_seconds, end_seconds, "qps");
			$scope.getAllCmdInfoChart("proxy_all_cmd_tp999", codis_name, xauth, start_seconds, end_seconds, $scope.more_tp_cmd);
		}

		$scope.getAllCmdInfoChart = function (chart_id, codis_name, xauth, start_seconds, end_seconds, type) {
			var table_name = getTableName("dashboard_", $scope.codis_addr);
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
							$scope.getHistory("QPS");
						}
					});
					if (chart_id == "historyCharts") {
						$scope.history_main_chart = chart;
					}  else {
						$scope.history_chart_map[chart_id] = chart;
					}
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

		$scope.legendSelectedAll = false;
		$scope.selectAllCmd = function() {
			$scope.selectAllLegend("historyCharts");
			$scope.selectAllLegend("proxy_all_cmd_tp999");
			$scope.legendSelectedAll = !$scope.legendSelectedAll;
		}

		$scope.selectAllLegend = function(chart_id) {
			var option = "";
			if (chart_id == "historyCharts") {
				option = $scope.history_main_chart.getOption();
			} else {
				option = $scope.history_chart_map[chart_id].getOption();
			}
				
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
			
			if (chart_id == "historyCharts") {
				$scope.history_main_chart.setOption({
					legend:{
						data: legend,
						selected: selected_list,
						width: '85%',
					}
				});
			} else {
				$scope.history_chart_map[chart_id].setOption({
					legend:{
						data: legend,
						selected: selected_list,
						width: '85%',
					}
				});
			}
		}

		/* 
		* 成倍扩容相关函数
		* refreshExpansionList(): 重新获取dashboard上注册好的扩容任务列表
		* createExpansionPlan(): 生成新的扩容任务 
		* addExpansionPlan(): 将新生成的扩容任务添加到dashboard的扩容任务列表中并刷新本地扩容任务列表
		* prepareExpansion(): 数据同步
		* execExpansion(): slots迁移
		* cleanExpansion(): 数据清理
		*/

		$scope.show_expansion = false;
		$scope.expansion_sync_speed = 70;
		$scope.expansion_binlog_nums = 500;
		$scope.showExpansion = function () {
			if ($scope.is_redis_group) {
				alert("codis-redis不支持成倍扩容！");
				return;
			}
			if (!$scope.show_expansion) {
				if ($scope.sentinel_servers.length > 0) {
					alert("需要暂停sentinel监控");
				}
				$('#expansionTask').show();
				$scope.show_expansion = true;
				$scope.refreshExpansionList();
			} else {
				$('#expansionTask').hide();
				$scope.show_expansion = false;
			}
		}

		$scope.expansion_err_resp = "";
		$scope.refreshExpansionList = function() {
			$scope.expansion_err_resp = "";
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/expansion/pull-plan/" + xauth, codis_name);
				$http.get(url).then(function (resp) {
					if (resp.data == "") {
						$scope.expansion_refresh_status = false;
						$scope.expansion_plan_list = [];
						return;
					}
					$scope.fillExpansionList(resp.data);
				},function (failedResp) {
					if (failedResp.status == 404) {
						$scope.expansion_err_resp = "codis-dashboard版本过旧，不支持成倍扩容!";
					} else if (failedResp.status != 1500 && failedResp.status != 800) {
						$scope.expansion_err_resp = failedResp.data.toString();
					} else {
						$scope.expansion_err_resp = toJsonHtml(failedResp.data);
					}
				});
			}
		}

		$scope.fillExpansionList = function(resp) {
			$scope.expansion_plan_list = [];
			var plan_list = resp.split("\n");
			for (var i=0; i<plan_list.length; i++) {
				var plan_arr = plan_list[i].split("$");
				var action = parseInt(plan_arr[6]);
				var need_binlog_offset = false;
				if (action == 1 || action == 2) {
					need_binlog_offset = true;
				}
				var dst_have_slave = $scope.isGroupHaveSlave(parseInt(plan_arr[2]));
				$scope.expansion_plan_list.push({
					id: parseInt(plan_arr[0]),
					src_gid: parseInt(plan_arr[1]),
					dst_gid: parseInt(plan_arr[2]),
					slots_list: plan_arr[3],
					sync_speed: parseInt(plan_arr[4]),
					binlog_nums: parseInt(plan_arr[5]),
					action: action,
					step: parseInt(plan_arr[7]),
					status: plan_arr[8],
					error: plan_arr[9],
					src_master_binlog_offset: "-",
					dst_master_binlog_offset: "-",
					dst_slave_binlog_offset: "_",
					src_slotsreload_status: "-",
					src_slotsdel_status: "-",
					src_compact_status: [],
					dst_slotsreload_status: "-",
					dst_slotsdel_status: "-",
					dst_compact_status: [],
					need_binlog_offset: need_binlog_offset,
					dst_have_slave: dst_have_slave
				});
			}
			$scope.updateExpansionBinlogOffset();
		}

		$scope.updateExpansionBinlogOffset = function() {
			for (var i=0; i<$scope.expansion_plan_list.length; i++) {
				$scope.expansion_plan_list[i].src_master_binlog_offset = "-";
				$scope.expansion_plan_list[i].dst_master_binlog_offset = "-";
				$scope.expansion_plan_list[i].dst_slave_binlog_offset = "-";
				$scope.expansion_plan_list[i].src_slotsreload_status = "-";
				$scope.expansion_plan_list[i].dst_slotsreload_status = "-";
				$scope.expansion_plan_list[i].src_slotsdel_status = "-";
				$scope.expansion_plan_list[i].dst_slotsdel_status = "-";
				$scope.expansion_plan_list[i].src_compact_status = [];
				$scope.expansion_plan_list[i].dst_compact_status = [];
				var src_index = $scope.getGroupIndexByGid($scope.expansion_plan_list[i].src_gid);
				var dst_index = $scope.getGroupIndexByGid($scope.expansion_plan_list[i].dst_gid);
				if (src_index >= 0) {
					$scope.expansion_plan_list[i].src_master_binlog_offset = $scope.group_array[src_index].servers[0].binlog_offset;
					$scope.expansion_plan_list[i].src_slotsreload_status = $scope.group_array[src_index].servers[0].is_slots_reloading;
					$scope.expansion_plan_list[i].src_slotsdel_status = $scope.group_array[src_index].servers[0].is_slots_deleting;
					for (var j=0; j<$scope.group_array[src_index].servers.length; j++) {
						var prefix = "M_is_compact: ";
						if (j > 0) {
							prefix = "S_is_compact: ";
						}
						$scope.expansion_plan_list[i].src_compact_status.push(prefix + $scope.group_array[src_index].servers[j].is_compact);
					}
				}
				if (dst_index >= 0) {
					$scope.expansion_plan_list[i].dst_master_binlog_offset = $scope.group_array[dst_index].servers[0].binlog_offset;
					$scope.expansion_plan_list[i].dst_slotsreload_status = $scope.group_array[dst_index].servers[0].is_slots_reloading;
					$scope.expansion_plan_list[i].dst_slotsdel_status = $scope.group_array[dst_index].servers[0].is_slots_deleting;
					if ($scope.group_array[dst_index].servers.length > 1) {
						$scope.expansion_plan_list[i].dst_slave_binlog_offset = $scope.group_array[dst_index].servers[1].binlog_offset;
					}
					for (var j=0; j<$scope.group_array[dst_index].servers.length; j++) {
						var prefix = "M_is_compact: ";
						if (j > 0) {
							prefix = "S_is_compact: ";
						}
						$scope.expansion_plan_list[i].dst_compact_status.push(prefix + $scope.group_array[dst_index].servers[j].is_compact);
					}
				}
			}
		}

		$scope.getGroupIndexByGid = function(gid) {
			for (var i=0; i<$scope.group_array.length; i++) {
				if ($scope.group_array[i].id == gid) {
					return i;
				}
			}
			return -1;
		}

		$scope.clearSlotsPlan = function(gid) {
			$scope.expansion_slots_list = "";
		}

		$scope.createExpansionPlan = function(expansion_src_gid, expansion_dst_gid, expansion_sync_speed, expansion_binlog_nums) {
			var slots = [];
			//扫描源组上的所有slots
			if (!$scope.isGidExsit(expansion_src_gid)) {
				alert("源组不存在，请输入正确组id!");
				return;
			}
			if (!$scope.isGidExsit(expansion_dst_gid)) {
				alert("目标组不存在，请输入正确组id!");
				return;
			}

			if (expansion_sync_speed == undefined || expansion_sync_speed == "" || isNaN(expansion_sync_speed)) {
				alert("主从同步速度设置非法（0 <= speed <= 125）!");
				return;
			}

			if (expansion_sync_speed < 0 || expansion_sync_speed > 125) {
				alert("主从同步速度超出范围（0 <= speed <= 125）!");
				return;
			}

			if (expansion_binlog_nums == undefined || expansion_binlog_nums == ""|| isNaN(expansion_binlog_nums)) {
				alert("binlog文件数设置非法!");
				return;
			}

			if (expansion_binlog_nums < 10) {
				alert("binlog文件数设置过小，请重新设置!");
				return;
			}

			for (var i=0; i<$scope.slots_array.length; i++) {
				if ($scope.slots_array[i].action.target_id) {
					alert("有slot正在迁移!");
					return;
				}

				if ($scope.slots_array[i].group_id == expansion_dst_gid) {
					alert("目标组上有slots,不能用于成倍扩容");
					return;
				}

				if ($scope.slots_array[i].group_id == expansion_src_gid) {
					slots.push($scope.slots_array[i].id);
				}
			}
			if (slots.length <= 0) {
				$scope.expansion_slots_list = "";
				alert("没有可以迁移的slots");
				return;
			}
			//决定要迁移到目标组的slots区间
			$scope.expansion_slots_list = slots[0].toString();
			var sequence = 0;
			var slots_num_to_migrate = Math.floor((slots.length)/2);
			for (var i=1; i<slots_num_to_migrate; i++) {
				if (slots[i] == slots[i-1]+1 && i != slots_num_to_migrate - 1) {
					sequence++;
					continue;
				} else {
					if (slots[i] == slots[i-1]+1) {
						$scope.expansion_slots_list += "-" + slots[i].toString();
					} else if (sequence == 0) {
						$scope.expansion_slots_list += "," + slots[i].toString();
					} else {
						$scope.expansion_slots_list += "-" + slots[i-1].toString() + "," + slots[i].toString();
					}
					sequence = 0;
				}
			}
		}
		$scope.isGidExsit = function(gid) {
			for (var i=0; i< $scope.group_array.length; i++) {
				if ($scope.group_array[i].id == gid) {
					return true;
				}
			}
			return false;
		}

		$scope.addExpansionPlan = function(expansion_src_gid, expansion_dst_gid, expansion_slots_list, expansion_sync_speed, expansion_binlog_nums) {
			if (!$scope.isGidExsit(expansion_src_gid)) {
				alert("源组不存在，请输入正确组id!");
				return;
			}
			if (!$scope.isGidExsit(expansion_dst_gid)) {
				alert("目标组不存在，请输入正确组id!");
				return;
			}
			if (expansion_slots_list == undefined || expansion_slots_list == "") {
				alert("slots任务不能为空，请输入正确组slots任务!");
				return;
			}
			
			if (expansion_sync_speed == undefined || expansion_sync_speed == "" || isNaN(expansion_sync_speed)) {
				alert("主从同步速度设置非法（0 <= speed <= 125）!");
				return;
			}

			if (expansion_sync_speed < 0 || expansion_sync_speed > 125) {
				alert("主从同步速度超出范围（0 <= speed <= 125）!");
				return;
			}

			if (expansion_binlog_nums == undefined || expansion_binlog_nums == ""|| isNaN(expansion_binlog_nums)) {
				alert("binlog文件数设置非法!");
				return;
			}

			if (expansion_binlog_nums < 10) {
				alert("binlog文件数设置过小，请重新设置!");
				return;
			}

			for (var i=0; i<$scope.expansion_plan_list.length; i++) {
				if ($scope.expansion_plan_list[i].src_gid == expansion_src_gid 
					|| $scope.expansion_plan_list[i].src_gid == expansion_dst_gid
					|| $scope.expansion_plan_list[i].dst_gid == expansion_src_gid
					|| $scope.expansion_plan_list[i].dst_gid == expansion_dst_gid) {
					alert("与已存在的扩容方案冲突！");
					return;
				}
			}

			// 同步任务列表到dashboard
			var expansion_plan_str = expansion_src_gid + "$" + expansion_dst_gid + "$" + expansion_slots_list + "$" + expansion_sync_speed + "$" + expansion_binlog_nums;
			var codis_name = $scope.codis_name; 
			if (isValidInput(codis_name)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/expansion/add-plan/" + xauth + "/" + expansion_plan_str, codis_name);
				$http.put(url).then(function (resp) {
					$scope.refreshExpansionList();
				},function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		$scope.checkExpansionPlan = function(id) {
			$scope.expansion_plan_id = id;
		}

		//$scope.expansion_need_binlog_offset = false;
		$scope.prepareExpansion = function(expansion_plan_id, sync_speed, binlog_nums) {
			//$scope.refresh_interval = 1;
			$scope.expansion_refresh_status = true;
			var expansion_plan_index = -1;
			for (var i=0; i<$scope.expansion_plan_list.length; i++) {
				if ($scope.expansion_plan_list[i].id == expansion_plan_id) {
					expansion_plan_index = i;
					break;
				}
			}
			if (expansion_plan_index >= 0) {
				var codis_name = $scope.codis_name;
				var action_str = "Expansion: [db_sync_speed: " + sync_speed + "] [expire_logs_nums: " + binlog_nums + "] 是否进行源组和目标组数据同步?"
				if (isValidInput(codis_name)) {
					alertAction(action_str, function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/expansion/sync/" + xauth + "/" + expansion_plan_id, codis_name);
						$http.put(url).then(function (resp) {
							$scope.expansion_plan_list[expansion_plan_index].need_binlog_offset = true;
						},function (failedResp) {
							$scope.expansion_plan_list[expansion_plan_index].need_binlog_offset = false;
							alertErrorResp(failedResp);
						});
					});
				}
			}
		}
		
		$scope.expansionBackup = function(expansion_plan_id) {
			//$scope.refresh_interval = 1;
			$scope.expansion_refresh_status = true;
			var expansion_plan_index = -1;
			for (var i=0; i<$scope.expansion_plan_list.length; i++) {
				if ($scope.expansion_plan_list[i].id == expansion_plan_id) {
					expansion_plan_index = i;
					break;
				}
			}
			if (expansion_plan_index >= 0) {
				var gid = $scope.expansion_plan_list[expansion_plan_index].dst_gid;
				var prompt = "";
				var yes_suffix = "/1";
				var no_suffix = "/0";
				if ($scope.isGroupHaveSlave(gid)) {
					prompt = "Expansion: 是否进行目标组主从备份?";
					yes_suffix = "/1";
					no_suffix = "/0";
				} else {
					prompt = "主从备份: group-[" + gid + "] 中不存在slave实例! 是否跳过目标组主从备份?";
					yes_suffix = "/0";
					no_suffix = "/1";
				}
				var codis_name = $scope.codis_name;
				if (isValidInput(codis_name)) {
					var xauth = genXAuth(codis_name);
					alertSelectAction(prompt, function () {
						var url = concatUrl("/api/topom/expansion/backup/" + xauth + "/" + expansion_plan_id + yes_suffix, codis_name);
						$http.put(url).then(function (resp) {
							$scope.expansion_plan_list[expansion_plan_index].need_binlog_offset = true;
						},function (failedResp) {
							$scope.expansion_plan_list[expansion_plan_index].need_binlog_offset = false;
							alertErrorResp(failedResp);
						});
					}, function () {
						var url = concatUrl("/api/topom/expansion/backup/" + xauth + "/" + expansion_plan_id + no_suffix, codis_name);
						$http.put(url).then(function (resp) {
							$scope.expansion_plan_list[expansion_plan_index].need_binlog_offset = true;
						},function (failedResp) {
							$scope.expansion_plan_list[expansion_plan_index].need_binlog_offset = false;
							alertErrorResp(failedResp);
						});
					});
				}
			}
		}

		$scope.isGroupHaveSlave = function(gid) {
			for (var i=0; i<$scope.group_array.length; i++) {
				if ($scope.group_array[i].id == gid) {
					return $scope.group_array[i].servers.length > 1;
				}
			}
			return false;
		}

		$scope.execExpansion = function(expansion_plan_id) {
			//$scope.expansion_need_binlog_offset = false;
			$scope.refresh_interval = 1;
			$scope.expansion_refresh_status = true;
			var expansion_plan_index = -1;
			for (var i=0; i<$scope.expansion_plan_list.length; i++) {
				if ($scope.expansion_plan_list[i].id == expansion_plan_id) {
					expansion_plan_index = i;
					break;
				}
			}

			if (expansion_plan_index >= 0) {
				$scope.expansion_plan_list[expansion_plan_index].need_binlog_offset = false;
				var codis_name = $scope.codis_name;
				if (isValidInput(codis_name)) {
					alertAction("Expansion: 是否进行slots迁移?", function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/expansion/slots-migrate/" + xauth + "/" + expansion_plan_id, codis_name);
						$http.put(url).then(function (resp) {
							//alert(resp.data);
						},function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				}
			}
		}

		$scope.cleanExpansion = function(expansion_plan_id) {
			//$scope.refresh_interval = 1;
			$scope.expansion_refresh_status = true;
			var expansion_plan_index = -1;
			for (var i=0; i<$scope.expansion_plan_list.length; i++) {
				if ($scope.expansion_plan_list[i].id == expansion_plan_id) {
					expansion_plan_index = i;
					break;
				}
			}

			if (expansion_plan_index >= 0) {
				var action_str = "Expansion: start Dataclean Slotsreload?";
				var plan = $scope.expansion_plan_list[expansion_plan_index]
				if ((plan.action == 3 && plan.step == 2) || (plan.action == 4 && plan.step == 1)) {
					action_str = "Expansion: start Dataclean Slotsreload?";
				} else if (plan.action == 4 && plan.step == 2) {
					action_str = "Expansion: start Dataclean Slotsdel?";
				} else if (plan.action == 4 && plan.step == 3) {
					action_str = "Expansion: start Dataclean Del slots key?";
				} else if (plan.action == 4 && plan.step == 4) {
					action_str = "Expansion: start Dataclean compact?";
				} else if (plan.action == 4 && plan.step > 4) {
					action_str = "Expansion: wait compact finish?";
					alertText(action_str);
					return;
				}
				var codis_name = $scope.codis_name;
				if (isValidInput(codis_name)) {
					alertAction(action_str, function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/expansion/clean/" + xauth + "/" + expansion_plan_id, codis_name);
						$http.put(url).then(function (resp) {
							//alert(resp.data);
						},function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				}
			}
		}

		$scope.delExpansionPlan = function(expansion_plan_id) {
			var expansion_plan_index = -1;
			for (var i=0; i<$scope.expansion_plan_list.length; i++) {
				if ($scope.expansion_plan_list[i].id == expansion_plan_id) {
					expansion_plan_index = i;
					break;
				}
			}

			if (expansion_plan_index >= 0) {
				var codis_name = $scope.codis_name;
				if (isValidInput(codis_name)) {
					alertAction("Expansion: 是否删除任务?", function () {
						var xauth = genXAuth(codis_name);
						var url = concatUrl("/api/topom/expansion/del-plan/" + xauth + "/" + expansion_plan_id, codis_name);
						$http.put(url).then(function (resp) {
							$scope.refreshExpansionList();
							//alert(resp.data);
						},function (failedResp) {
							alertErrorResp(failedResp);
						});
					});
				}
			}
		}

		$scope.ExpansionCleanDataByGid = function(gid) {
			alert(gid);
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/expansion/group-clean/" + xauth + "/" + gid, codis_name);
				$http.put(url).then(function (resp) {
					//alert(resp.data);
				},function (failedResp) {
					alertErrorResp(failedResp);
				});
			}
		}

		$scope.expansion_master_index = 1;
		$scope.expansion_slave_index = 3;
		$scope.getMasterByGid = function(gid) {
			for (var i=0; i<$scope.group_array.length; i++) {
				if ($scope.group_array[i].id == gid) {
					if ($scope.group_array[i].servers.length > 0) {
						return {
							addr: $scope.group_array[i].servers[0].server, 
							version: $scope.group_array[i].servers[0].version
						};
					}
				}
			}
			return {addr: "", version: ""};
		}

		/*$scope.execExpansionCmd = function(addr, cmd) {
			var codis_name = $scope.codis_name;
			if (isValidInput(codis_name)) {
				var xauth = genXAuth(codis_name);
				var url = concatUrl("/api/topom/docmd/" + xauth + "/" + addr + "/" + cmd, codis_name);
				$http.get(url).then(function (resp) {
					alert(resp.data);
				},function (failedResp) {
					alertErrorResp(failedResp);
					return;
				});
			}
		}*/

		if (window.location.hash){
			$scope.selectCodisInstance(window.location.hash.substring(1));
		}

		var ticker = 0;
		(function autoRefreshStats() {
			if (ticker >= $scope.refresh_interval) {
				ticker = 0;
				if ($('#iframeTable').css('display') === 'none' && 
					$('#indexInfo').css('display') === 'none') {
					$scope.refreshStats();
				}
			}
			ticker++;
			$timeout(autoRefreshStats, 1000);
		}());
	}
])
;
