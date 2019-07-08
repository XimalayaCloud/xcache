'use strict';

var md5_salt = "ximalaya_salt_94k5yd1gd92ns64q";

Date.prototype.Format = function (fmt) { 
	var o = {
		"M+": this.getMonth() + 1, //月份 
		"d+": this.getDate(), //日 
		"h+": this.getHours(), //小时 
		"m+": this.getMinutes(), //分 
		"s+": this.getSeconds(), //秒 
		"q+": Math.floor((this.getMonth() + 3) / 3), //季度 
		"S": this.getMilliseconds() //毫秒 
	};
	if (/(y+)/.test(fmt)) fmt = fmt.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
	for (var k in o)
	if (new RegExp("(" + k + ")").test(fmt)) fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));
	return fmt;
}

function genXAuth(name) {
	return sha256("Codis-XAuth-[" + name + "]").substring(0, 32);
}

function jumpToLink(link){
	window.location.href = link;
}

function showRoleTable(id1, id2) {
	$(".row").hide();
	$("#" + id1).show();
	$("#" + id2).show();
	scrollTo(0,0);
}

function moveInPage(id) {
	$("html, body").animate({
		scrollTop: $("#" + id).offset().top }, {duration: 0,easing: "swing"});
	$("html,body").animate({'scrollTop' : "-=55px"}, 0);
	return false;
}

function goHomePage() {
	$("#btnHomePage", window.parent.document).click();
}

function childLogout() {
	$("#btnLogout", window.parent.document).click();
}

function getURLParam(name) { 
	var reg = new RegExp("(^|&)" + name + "=([^&]*)(&|$)", "i"); 
	var r = window.location.search.substr(1).match(reg); 
	if (r != null) {
		return unescape(r[2]);
	} 
	return ""; 
} 

function concatUrl(base, name) {
	if (name) {
		return encodeURI(base + "?forward=" + name);
	} else {
		return encodeURI(base);
	}
}

function padInt2Str(num, size) {
	var s = num + "";
	while (s.length < size) s = "0" + s;
	return s;
}

function toJsonHtml(obj) {
	var json = angular.toJson(obj, 4);
	json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
	json = json.replace(/ /g, '&nbsp;');
	json = json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
		var cls = 'number';
		if (/^"/.test(match)) {
			if (/:$/.test(match)) {
				cls = 'key';
			} else {
				cls = 'string';
			}
		} else if (/true|false/.test(match)) {
			cls = 'boolean';
		} else if (/null/.test(match)) {
			cls = 'null';
		}
		return '<span class="' + cls + '" style="word-warp:break-word; word-break:break-all;">' + match + '</span>';
	});
	return json;
}

function humanSize(size) {
	if (size < 1024) {
		return size + " B";
	}
	size /= 1024;
	if (size < 1024) {
		return size.toFixed(3) + " KB";
	}
	size /= 1024;
	if (size < 1024) {
		return size.toFixed(3) + " MB";
	}
	size /= 1024;
	if (size < 1024) {
		return size.toFixed(3) + " GB";
	}
	size /= 1024;
	return size.toFixed(3) + " TB";
}

function formatGB(size) {
	if (size < 1024 * 1024) {
		return 0 + " GB";
	}
	size /= 1024 * 1024 *1024;
	return size.toFixed(3) + " GB";
}

function numSize(size) {
	var size_level = size.slice(size.indexOf(" "));
	var size_num = parseFloat(size.slice(0, size.indexOf(" ")));
	if (isNaN(size_num)) {
		return 0;
	} else if (size_level == " B") {
		return size_num;
	} else if (size_level == " KB") {
		return size_num * 1024;
	} else if (size_level == " MB") {
		return size_num * 1024 * 1024;
	} else if (size_level == " GB") {
		return size_num * 1024 * 1024 * 1024;
	} else if (size_level == " TB") {
		return size_num * 1024 * 1024 * 1024 * 1024;
	} else {
		return 0;
	}
}

function newChatsOpsConfigEcharts(title, num) {
	var series_arr = [];
	var legend_arr = [];
	for (var i=0; i<num; i++) {
		series_arr.push({
			name: '',
			type: 'line',
			symbol: 'circle',
			symbolSize: 3.5,
			showSymbol: true,
			smooth: true,
			lineStyle: {
				normal:{
					width: 1.5,
				}
			},
			data: []
		});
		legend_arr.push('');
	}
	return {
		title: {
			show: true,
			text: title,
		},
		tooltip: {
			trigger: 'axis',
			axisPointer: {
				animation: false
			}
		},
		grid: {
			top: '50px',
			left: '3%',
			right: '3%',
			bottom: '3%',
			containLabel: true  //包括坐标轴在内的矩形区域的设定
		},
		legend: {
			data:legend_arr,
		},
		xAxis: {
			type: 'time',
			splitLine: {
				show: true
			},
			boundaryGap: false,
		},
		yAxis: {
			type: 'value',
			splitLine: {
				show: true
			},
			minInterval: 1,
			scale: true,
			boundaryGap: ['10%', '10%'],
		},
		series: series_arr,
	};
}

function newChatsOpsConfig() {
	return {
		options: {
			chart: {
				useUTC: false,
				type: 'spline',
			},
		},
		series: [{
			color: '#d82b28',
			lineWidth: 1.5,
			states: {
				hover: {
					enabled: false,
				}
			},
			showInLegend: false,
			marker: {
				enabled: true,
				symbol: 'circle',
				radius: 2,
			},
			name: 'OP/s',
			data: [],
		}],
		title: {
			style: {
				display: 'none',
			}
		},
		xAxis: {
			type: 'datetime',
			crosshair: true,
			title: {
				style: {
					display: 'none',
				}
			},
			labels: {
				formatter: function () {
					var d = new Date(this.value);
					return padInt2Str(d.getHours(), 2) + ":" + padInt2Str(d.getMinutes(), 2) + ":" + padInt2Str(d.getSeconds(), 2);
				}
			},
			tickWidth: 0,
			tickInterval: 8000,
			gridLineWidth: 1,
		},
		yAxis: [{
			title: {
				style: {
					display: 'none',
				}
			},
			lineWidth: 1,
			lineColor: "#c0d0e0",
			crosshair: true,
		}],
	};
} 

function alertText(text) {
	BootstrapDialog.show({
		title: "Warning !!",
		message: text,
		closable: true,
		buttons: [{
			label: "OK",
			cssClass: "btn-primary",
			action: function (dialog) {
				dialog.close();
			},
		}],
	});
}

function alertAction(text, callback) {
	BootstrapDialog.show({
		title: "Warning !!",
		message: text,
		closable: true,
		buttons: [{
			label: "OK",
			cssClass: "btn-primary",
			action: function (dialog) {
				dialog.close();
				callback();
			},
		},{
			label: "Cancel",
			cssClass: "btn-primary",
			action: function (dialog) {
				dialog.close();
			},
		}],
	});
}

function alertSelectAction(text, callback1, callback2) {
	BootstrapDialog.show({
		title: "Warning !!",
		message: text,
		closable: true,
		buttons: [{
			label: "Yes",
			cssClass: "btn-primary",
			action: function (dialog) {
				dialog.close();
				callback1();
			},
		},{
			label: "No",
			cssClass: "btn-primary",
			action: function (dialog) {
				dialog.close();
				callback2();
			},
		}],
	});
}

function alertDangerAction(text, callback) {
	BootstrapDialog.show({
		title: "Warning !!",
		type: "type-danger",
		message: text,
		closable: true,
		buttons: [{
			label: "强制执行",
			cssClass: "btn-danger",
			action: function (dialog) {
				dialog.close();
				callback();
			},
		},{
			label: "退出",
			cssClass: "btn-primary",
			action: function (dialog) {
				dialog.close();
			},
		}],
	});
}

function alertErrorResp(failedResp) {
	var text = "error response";
	if (failedResp.status != 1500 && failedResp.status != 800) {
		text = failedResp.data.toString();
	} else {
		text = toJsonHtml(failedResp.data);
	}
	BootstrapDialog.alert({
		title: "Error !!",
		type: "type-danger",
		closable: true,
		message: text,
	});
}

function alertSuccessResp(failedResp) {
	var text = "Success";
	if (failedResp.status != 1500 && failedResp.status != 800) {
		text = failedResp.data.toString();
	} else {
		text = toJsonHtml(failedResp.data);
	}
	BootstrapDialog.alert({
		title: "Success !",
		type: "type-success",
		closable: true,
		message: text,
	});
}

function isValidInput(text) {
	return text && text != "" && text != "NA";
}

function newRandPassword() { 
	var text = ['abcdefghijklmnopqrstuvwxyz', '1234567890']; 
	var rand = function(min, max) {
		return Math.floor(Math.max(min, Math.random() * (max+1)));
	} 
	var len = 6; // 长度为6
	var pw = ''; 
	for(var i=0; i<len; i++) { 
		var strpos = rand(0, 1); 
		pw += text[strpos].charAt(rand(0, text[strpos].length-1)); 
	} 
	return pw;
}  

function checkUserName(s) {
	var result = {isLegal:false, text:""};
	if(s=="" || s.length==0){
		result.text = "不能为空";
		return result;
	}
	if(/^[0-9]+$/.test(s)){ //全是数字
		result.text = "不能全是数字";
		return result;
	}
	if(s.length > 64){
		result.text = "长度不能大于64位";
		return result;
	}
	if(!s.match(/^[a-zA-Z0-9_]+$/)){ //包括数字字母
		result.text = "含有非法字符";
		return result;
	}

	result.isLegal = true;
	return result;
}

function checkPassword(s) {
	var result = {isLegal:false, text:""};

	if(s=="" || s.length==0){
		result.text = "不能为空";
		return result;
	}
	if(s.length < 6 || s.length > 16){
		result.text = "长度应为6-16位";
		return result;
	}
	if(!s.match(/^[a-zA-Z0-9]+$/)){ //包括数字字母
		result.text = "含有非法字符";
		return result;
	}

	result.isLegal = true;
	return result;
}

function checkCodisName(s) {
	var result = {isLegal:false, text:""};
	if(s=="" || s.length==0){
		result.text = "集群名不能为空";
		return result;
	}

	result.isLegal = true;
	return result;
}

function checkDashboard(s) {
	var result = {isLegal:false, text:""};
	if(s=="" || s.length==0){
		result.text = "IP/端口不能为空";
		return result;
	}

	result.isLegal = true;
	return result;
}

//去掉左边的空白  
function trimLeft(s){  
	if(s == null) {  
		return "";  
	}
	var whitespace = new String(" \t\n\r");  
	var str = new String(s);  
	if (whitespace.indexOf(str.charAt(0)) != -1) {  
		var j=0, i = str.length;  
		while (j < i && whitespace.indexOf(str.charAt(j)) != -1){  
			j++;  
		}  
		str = str.substring(j, i);  
	}  
	return str;  
}  
//去掉右边的空白   
function trimRight(s){  
	if(s == null) return "";  
	var whitespace = new String(" \t\n\r");  
	var str = new String(s);  
	if (whitespace.indexOf(str.charAt(str.length-1)) != -1){  
		var i = str.length - 1;  
		while (i >= 0 && whitespace.indexOf(str.charAt(i)) != -1){  
		   i--;  
		}  
		str = str.substring(0, i+1);  
	}  
	return str;  
}  

function strTrim(str) {
	return trimRight(trimLeft(str));
}

function addSalt(str) {
	return str + md5_salt;
}
