'use strict';

var admin = angular.module('admin', ["ui.bootstrap"]);

function setPassword(id, pswd) {
	$("#" + id).val(pswd);
}

$(document).ready(function() {
	$(".search-select-style").select2();
});

function getUserAddParams() {
	var username = $("#username").val();
	var password = $("#password").val();
	var usertype = $("input[name='usertype']:checked").val();
	var comment = $("#usercomment").val();
	username = strTrim(username);
	password = strTrim(password);
	return {username:username, password:password, usertype:usertype, comment:comment};
}

function getCodisAddParams() {
	var codisname = $("#codisname").val();
	var dashboard = $("#dashboard").val();
	return {codisname:codisname, dashboard:dashboard};
}

function getRelationAddParams() {
	var username = $("#relaUsername").val();
	var codisname = $("#relaCodisname").val();
	var iswrite = $("input[name='iswrite']:checked").val();
	return {username:username, codisname:codisname, iswrite:iswrite};
}

function getUserUpdateParam() {
	var username = $("#updateUser").val();
	var password = $("#updatePassword").val();
	var usertype = $("input[name='updateUsertype']:checked").val();
	var comment  = $("#updateUserComment").val();
	password = strTrim(password);
	return {username:username, password:password, usertype:usertype, comment:comment};
}
function getCodisUpdateParams() {
	var codisname = $("#updateSelectCodisName").val();
	var dashboard = $("#updateDashboard").val();
	var comment = $("#updateCodisComment").val();
	dashboard = strTrim(dashboard);
	return {codisname:codisname, dashboard:dashboard, comment:comment};
}
function getRelationUpdateParams() {
	var iswrite = $("input[name='updateiswrite']:checked").val();
	return {iswrite:iswrite};
}

function getUserNameBySelectId(id){
	return $("#" + id).val();
}

function getCodisNameBySelectId(id) {
	return $("#" + id).val();
}

function genRelationSearchSqlWhere(typeid, infoid) {
	var sql_where = "";
	var search_type = $("#" + typeid).val();
	var search_info = $("#" + infoid).val();
	if (search_type != "" && search_info != "") {
		if (search_info != "all") {
			sql_where = " and " + search_type + " = '" + search_info + "'";
		}
	}
	return sql_where;
}

function fillUserInfo(usertype, comment) {
	$("#updatePassword").val("");
	$("#updateUserComment").val(comment);
	if (usertype == 1) {
		$('input:radio[name=updateUsertype]').eq(0).prop('checked',true);
	} else {
		$('input:radio[name=updateUsertype]').eq(1).prop('checked',true);
	}
}
function clearUserInfo() {
	$("#username").val("");
	$("#password").val("");
	$("#usercomment").val("");
	$('input:radio[name=updateUsertype]').eq(1).prop('checked',true);
}

function fillCodisInfo(dashboard, comment) {
	$("#updateDashboard").val(dashboard);
	$("#updateCodisComment").val(comment);
}

function clearCodisInfo() {
	$("#codisname").val("");
	$("#dashboard").val("");
	$("#codiscomment").val("");
}

function fillUpdateIswrite(iswrite) {
	if (iswrite) {
		$('input:radio[name=updateiswrite]').eq(0).prop('checked',true);
	} else {
		$('input:radio[name=updateiswrite]').eq(1).prop('checked',true);
	}
}

function getDelListSqlWhere(tableid) {
	var list = []; 
	var sql_where = "";
	$("input[name='" + tableid +"']:checkbox").each(function() { 
		if($(this).is(":checked")) 
		{ 
			list.push($(this).val()); 
		} 
	}); 
	for (var i=0; i<list.length; i++) {
		if (i == 0) {
			sql_where += " id = " + list[i] ;
		} else {
			sql_where += " or id =" + list[i];
		}
	}
	return sql_where; 
}

function btnGroupShift(id) {
	$("#user").hide();
	$("#codis").hide();
	$("#usercodis").hide();
	$("#" + id).show();
}

function changeBtnStyle(old_id, new_id) {
	$("#" + old_id).removeClass('btn-shadow');
	$("#" + new_id).addClass('btn-shadow');
}

function userFuncShift(old_id, new_id) {
	$("#" + old_id).hide();
	$("#" + new_id).show();
}

admin.factory('redirectionUrl', [function() {
	var redirectionUrl = {
		response: function(response) {
			if (response.headers("content-type").indexOf("html") >= 0){
				jumpToLink("/login");
			}
			return response;
		}
	};
	return redirectionUrl;
}]);

admin.config(['$interpolateProvider',
	function ($interpolateProvider) {
		$interpolateProvider.startSymbol('[[');
		$interpolateProvider.endSymbol(']]');
	}
]);

admin.config(['$httpProvider', function ($httpProvider) {
	$httpProvider.defaults.useXDomain = true;
	delete $httpProvider.defaults.headers.common['X-Requested-With'];
	$httpProvider.interceptors.push('redirectionUrl');
}]);

admin.controller('MainAdminCtrl', ['$scope', '$http', '$uibModal', '$timeout',
	function ($scope, $http, $uibModal, $timeout) {
		var info_map = {};
		$scope.now_operat_div_id = "";
		$scope.now_btn_id = "btn_getuser";

		$http.get('/username').then(function (resp) {
			$scope.username = JSON.parse(resp.data);
		});

		$scope.userManage = function (btn_group_id, show_id) {
			btnGroupShift(btn_group_id);
			$scope.getUsersInit(show_id, "btn_getuser");
		}

		$scope.codisManage = function (btn_group_id, show_id) {
			btnGroupShift(btn_group_id);
			$scope.getCodisInit(show_id, "btn_getcodis");
		}
		$scope.userCodisManage = function (btn_group_id, show_id) {
			btnGroupShift(btn_group_id);
			$scope.getRelationInit(show_id, "btn_getrela");
		}

		$scope.getUsersInit = function (id, btn_id) {
			$scope.shiftBtn(id);
			changeBtnStyle($scope.now_btn_id, btn_id);
			$scope.now_btn_id = btn_id;
			$scope.queryusername = "all";
			$scope.getUserInfo();
		}
		$scope.addUsersInit = function (id, btn_id) {
			$scope.shiftBtn(id);
			changeBtnStyle($scope.now_btn_id, btn_id);
			$scope.now_btn_id = btn_id;
			clearUserInfo();
			$scope.is_username_illegal = false;
			$scope.is_password_illegal = false;
			$scope.username_tips = "用户名（数字、字母、下划线、区分大小写）";
			$scope.password_tips = "6-16位密码（数字、字母、区分大小写）";
		}
		$scope.updateUsersInit = function (id, btn_id) {
			$scope.shiftBtn(id);
			changeBtnStyle($scope.now_btn_id, btn_id);
			$scope.now_btn_id = btn_id;
			$scope.getUserInfo();
			fillUserInfo(0, "");
			$scope.updateUser = "请选择";
			$scope.is_password_illegal = false;
			$scope.show_update_password = false;
			$scope.password_tips = "6-16位密码（数字、字母、区分大小写）";
		}
		$scope.removeUsersInit = function (id, btn_id) {
			$scope.shiftBtn(id);
			changeBtnStyle($scope.now_btn_id, btn_id);
			$scope.now_btn_id = btn_id;
			$scope.removeusername = "all";
			$scope.getUserInfo();
		}

		$scope.shiftBtn = function (id) {
			userFuncShift($scope.now_operat_div_id, id);
			$scope.now_operat_div_id = id;
		}

		$scope.getUserInfo = function () {
			var sql = "select id, username, usertype, comment from users order by usertype desc, username asc";
			var url = "/sql/" + sql;
			$http.get(url).then(function (resp) {
				 var recv = resp.data;
				 $scope.user_info_list = resp.data;
				 $scope.select_user_info_list = resp.data;
				 for (var i=0; i<recv.length; i++) {
					info_map[recv[i].username] = recv[i].id;
				 }
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}

		$scope.genRandPassword = function (id){
			var pswd = newRandPassword();
			setPassword(id, pswd);
			$scope.isLegalPassword(pswd);
		}

		$scope.clearText = function (id) {
			$scope.is_password_illegal = false;
			$scope.password_tips = "6-16位密码（数字、字母、区分大小写）";
			$("#" + id).val("");
		}

		$scope.ngIsLegalUserName = function () {
			var username = $("#username").val();
			$scope.isLegalUserName(username);
		}

		$scope.ngIsLegalPassword = function (id) {
			var password = $("#" + id).val();
			$scope.isLegalPassword(password);
		}

		$scope.isLegalUserName = function (username) {
			var test_user = checkUserName(username);
			if (test_user.isLegal) {
				$scope.username_tips = "";
				$scope.is_username_illegal = false;
			} else {
				$scope.username_tips = test_user.text;
				$scope.is_username_illegal = true;
			} 
		}

		$scope.isLegalPassword = function (password) {
			var test_pswd = checkPassword(password);
			if (test_pswd.isLegal) {
				$scope.password_tips = "";
				$scope.is_password_illegal = false;
			} else {
				$scope.password_tips = test_pswd.text;
				$scope.is_password_illegal = true;
			} 
		}

		$scope.addUserInfo = function () {
			var sqlparams = getUserAddParams();

			$scope.isLegalUserName(sqlparams.username);
			$scope.isLegalPassword(sqlparams.password);
			if ($scope.is_username_illegal || $scope.is_password_illegal) {
				return ;
			} else {
				$scope.is_username_illegal = false;
				$scope.is_password_illegal = false;
				$scope.username_tips = "";
				$scope.password_tips = "";
			}

			var password = hex_md5(addSalt(sqlparams.password));
			$http({  
				method:'post',  
				url:'/add/user',  
				data:{
					name: sqlparams.username,
					password: password,
					usertype: sqlparams.usertype,
					comment: sqlparams.comment
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
				var result = resp.data;
				if (!isNaN(result)) {
					alert("添加成功");
				} else {
					alert("添加失败");
				}
			}, function error(failedresp) {
				alert("请求失败")
			});
		}

		$scope.searchUserInfo = function(id) {
			var username = getUserNameBySelectId(id);
			var sql = "select id, username, usertype, comment from users";
			if (username != "all") {
				sql = sql + " where username = '" + username +"'";
			}
			sql = sql + " order by usertype desc, username asc";
			var url = "/sql/" + sql;
			$http.get(url).then(function (resp) {
				$scope.user_info_list = resp.data;
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}

		$scope.searchUpdateUserInfo = function(id) {
			var username = getUserNameBySelectId(id);
			var sql = "select id, username, usertype, comment from users where username = '" 
					+ username + "'";
			var url = "/sql/" + sql;
			$http.get(url).then(function (resp) {
				var user_info = resp.data[0];
				$scope.update_userid = user_info.id;
				fillUserInfo(user_info.usertype, user_info.comment);
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
			$scope.is_password_illegal = false;
			$scope.show_update_password = false;
			$scope.password_tips = "6-16位密码（数字、字母、区分大小写）";
		}

		$scope.updateUserInfo = function () {
			var sqlparams = getUserUpdateParam();
			if (sqlparams.username == null) {
				alert("请选择要修改的用户");
				return ;
			}

			var ret = checkPassword(sqlparams.password);
			if (!$scope.show_update_password && sqlparams.password == "") {
				$scope.password_tips = "";
				$scope.is_password_illegal = false;
			} else if (!ret.isLegal) {
				$scope.password_tips = ret.text;
				$scope.is_password_illegal = true;
				alert("Password: " + $scope.password_tips);
				return ;
			} 

			var password = "";
			if (sqlparams.password != "") {
				password = addSalt(sqlparams.password);
				password = hex_md5(password);
			}
			$http({  
				method:'post',  
				url:'/update/user',  
				data:{
					id: $scope.update_userid,
					name: sqlparams.username,
					password: password,
					usertype: sqlparams.usertype,
					comment: sqlparams.comment
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
				var result = resp.data;
				if (!isNaN(result)) {
					$scope.password_tips = "6-16位密码（数字、字母、区分大小写）";
					alert("修改成功");
				} else {
					alert("修改失败");
				}
			}, function error(failedresp) {
				alert("请求失败")
			});
		}

		$scope.removeUserInfo = function () {
			var delUserCommand = getDelListSqlWhere("deluser");
			if (delUserCommand == "") {
				alert("请选择要删除的用户");
				return ;
			}
			var sql = "delete from users where " + delUserCommand;
			var url = "/sql/"+sql;
			$http.get(url).then(function (resp) {
				var result = resp.data;
				if (!isNaN(result)) {
					alert("删除成功");
				} else {
					alert("删除失败");
				}
				$scope.removeusername = "all";
				$scope.getUserInfo();
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}

		$scope.userManage('user','queryuser');

		//codis operat
		$scope.getCodisInit = function (id, btn_id) {
			userFuncShift($scope.now_operat_div_id, id);
			$scope.now_operat_div_id = id;
			changeBtnStyle($scope.now_btn_id, btn_id);
			$scope.now_btn_id = btn_id;
			$scope.querycodisname = "all";
			$scope.getCodisInfo();
		}
		$scope.addCodisInit = function (id, btn_id) {
			userFuncShift($scope.now_operat_div_id, id);
			changeBtnStyle($scope.now_btn_id, btn_id);
			$scope.now_btn_id = btn_id;
			clearCodisInfo();
			$scope.now_operat_div_id = id;
			$scope.codisname_tips = "";
			$scope.dashboard_tips = "";
			$scope.is_codisname_illegal = false;
			$scope.is_dashboard_illegal = false;
		}
		$scope.updateCodisInit = function (id, btn_id) {
			userFuncShift($scope.now_operat_div_id, id);
			$scope.now_operat_div_id = id;
			changeBtnStyle($scope.now_btn_id, btn_id);
			$scope.now_btn_id = btn_id;
			$scope.getCodisInfo();
			fillCodisInfo("", "");
			$scope.updateSelectCodisName = "请选择";
			$scope.is_dashboard_illegal = false;
			$scope.update_dashboard = "";
		}
		$scope.removeCodisInit = function (id, btn_id) {
			userFuncShift($scope.now_operat_div_id, id);
			$scope.now_operat_div_id = id;
			changeBtnStyle($scope.now_btn_id, btn_id);
			$scope.now_btn_id = btn_id;
			$scope.removecodisname = "all";
			$scope.getCodisInfo();
		}

		$scope.getCodisInfo = function() {
			var sql = "select id,codisname,dashboard,comment from codis order by codisname";
			var url = "/sql/" + sql;
			$http.get(url).then(function (resp) {
				var recv = resp.data;
				 $scope.select_codis_info_list = resp.data;
				 $scope.codis_info_list = resp.data;
				 for (var i=0; i<recv.length; i++) {
					info_map[recv[i].codisname] = recv[i].id;
				 }
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}

		$scope.addCodisInfo = function () {
			var sqlparams = getCodisAddParams();
			
			$scope.isCodisnameIllegal(sqlparams.codisname);
			$scope.isDashboardIllegal(sqlparams.dashboard);

			if ($scope.is_codisname_illegal || $scope.is_dashboard_illegal) {
				return ;
			} else {
				$scope.codisname_tips = "";
				$scope.dashboard_tips = "";
				$scope.is_codisname_illegal = false;
				$scope.is_dashboard_illegal = false;
			}

			var sql = "insert into codis(codisname,dashboard) values('" 
					+ sqlparams.codisname + "','" 
					+ sqlparams.dashboard + "')";
			var url = "/sql/"+sql;

			$http.get(url).then(function (resp) {
				var result = resp.data + 1;
				if (!isNaN(result)) {
					alert("codis信息添加成功");

					// 代替触发器将codis的权限分配给所有管理员用户
					var sql = "insert into user_codis (userid, codisid, iswrite) select user.id as userid, codis.id as codisid, 1 from user,codis where user.usertype = 1 and codis.codisname = '" + sqlparams.codisname + "' and codis.dashboard = '" + sqlparams.dashboard + "';"
					var url = "/sql/"+sql;
					$http.get(url).then(function (resp) {
						;
					});
				} else {
					alert("codis信息添加失败");
				}
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}

		$scope.ngIsCodisnameIllegal = function () {
			var codisname = $("#codisname").val();
			$scope.isCodisnameIllegal(codisname);
		}

		$scope.ngIsDashboardIllegal = function (id) {
			var dashboard = $("#" + id).val();
			$scope.isDashboardIllegal(dashboard);
		}

		$scope.isCodisnameIllegal = function (codisname) {
			var ret = checkCodisName(codisname);
			if (ret.isLegal) {
				$scope.is_codisname_illegal = false;
			} else {
				$scope.is_codisname_illegal = true;
			}
			$scope.codisname_tips = ret.text;
		}

		$scope.isDashboardIllegal = function (dashboard) {
			var ret = checkDashboard(dashboard);
			if (ret.isLegal) {
				$scope.is_dashboard_illegal = false;
			} else {
				$scope.is_dashboard_illegal = true;
			}
			$scope.dashboard_tips = ret.text;
		}

		$scope.searchCodisInfo = function(id) {
			var searchSqlCommand = getCodisNameBySelectId(id);
			var sql = "select id, codisname, dashboard, comment from codis";
			if (searchSqlCommand != "all") {
				sql = sql + " where codisname='" + searchSqlCommand + "'";
			} 
			sql = sql + " order by codisname";
			var url = "/sql/" + sql;
			$http.get(url).then(function (resp) {
				var recv = resp.data[0];
				$scope.update_codisid = recv.id;
				$scope.codis_info_list = resp.data;
				fillCodisInfo(recv.dashboard, recv.comment);
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}

		$scope.searchUpdateCodisInfo = function(id) {
			var searchSqlCommand = getCodisNameBySelectId(id);
			var sql = "select id, codisname, dashboard, comment from codis where codisname='" 
					+ searchSqlCommand + "'";
			var url = "/sql/" + sql;
			$http.get(url).then(function (resp) {
				var recv = resp.data[0];
				$scope.update_codisid = recv.id;
				fillCodisInfo(recv.dashboard, recv.comment);
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
			$scope.is_dashboard_illegal = false;
			$scope.dashboard_tips = "";
		}

		$scope.updateCodisInfo = function () {
			var sqlparams = getCodisUpdateParams();
			if (sqlparams.codisname == null) {
				alert("请选择要删除的Codis");
				return ;
			}

			var ret = checkDashboard(sqlparams.dashboard);
			if (ret.isLegal) {
				$scope.dashboard_tips = ret.text;
				$scope.is_dashboard_illegal = false;
			} else {
				$scope.dashboard_tips = ret.text;
				$scope.is_dashboard_illegal = true;
				alert("Dashboard: " + $scope.dashboard_tips);
				return ;
			}

			var sql = "update codis set codisname='" + sqlparams.codisname 
					+ "', dashboard='" + sqlparams.dashboard 
					+ "', comment = '" + sqlparams.comment 
					+ "'  where id=" + $scope.update_codisid;

			var url = "/sql/"+sql;
			$http.get(url).then(function (resp) {
				var result = resp.data;
				if (!isNaN(result)) {
					alert("修改成功");
				} else {
					alert("修改失败");
				}
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}

		$scope.removeCodisInfo = function () {
			var delUserCommand = getDelListSqlWhere("delcodis");
			if (delUserCommand == "") {
				alert("请选择要删除的Codis");
				return ;
			}
			var sql = "delete from codis where " + delUserCommand;
			var url = "/sql/"+sql;
			$http.get(url).then(function (resp) {
				var result = resp.data;
				if (!isNaN(result)) {
					alert("删除成功");
				} else {
					alert("删除失败");
				}
				$scope.removecodisname = "all";
				$scope.getCodisInfo();
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}

		//relation operat
		$scope.getRelationInit = function (id, btn_id){
			userFuncShift($scope.now_operat_div_id, id);
			$scope.now_operat_div_id = id;
			changeBtnStyle($scope.now_btn_id, btn_id);
			$scope.now_btn_id = btn_id;
			$scope.relationSearchType = "username";
			$("#relationSearchType option:first").prop("selected", true).select2();
			$scope.relationSearchInfo = "all";
			$scope.getSelectListByType("relationSearchType");
		}

		$scope.addRelationInit = function (id, btn_id){
			userFuncShift($scope.now_operat_div_id, id);
			$scope.now_operat_div_id = id;
			changeBtnStyle($scope.now_btn_id, btn_id);
			$scope.now_btn_id = btn_id;

			$scope.relaUsername = "noselect";
			$scope.relaCodisname = "noselect";
			$scope.is_userid_null = false;
			$scope.is_codisid_null = false;
			$scope.relation_user = "";
			$scope.relation_codis = "";

			$("#relaCodisname option:first").text("---请选择---");
			$("#relaCodisname option:first").prop("selected", true).select2();
			$("#relaCodisname").select2();
			$('input:radio[name=iswrite]').eq(1).prop('checked',true);
			$scope.getUserInfo();
		}

		$scope.updateRelationInit = function (id, btn_id){
			userFuncShift($scope.now_operat_div_id, id);
			$scope.now_operat_div_id = id;
			changeBtnStyle($scope.now_btn_id, btn_id);
			$scope.now_btn_id = btn_id;

			$scope.updaterelaUsername = "noselect";
			$scope.updaterelaCodisname = "noselect";
			$("#updaterelaCodisname option:first").prop("selected", true).select2();
			$("#relaCodisname").select2();
			$('input:radio[name=updateiswrite]').removeAttr('checked');
			$scope.update_rela_codis_list = [];

			$scope.getUserInfo();
		}

		$scope.removeRelationInit = function (id, btn_id){
			userFuncShift($scope.now_operat_div_id, id);
			$scope.now_operat_div_id = id;
			changeBtnStyle($scope.now_btn_id, btn_id);
			$scope.now_btn_id = btn_id;
			$scope.removereRelationSearchType = "username";
			$("#removereRelationSearchType option:first").prop("selected", true).select2();
			$scope.removereRelationSearchInfo = "all";
			$scope.getSelectListByType("removereRelationSearchType");
		}

		$scope.getRelationInfo = function(search_type) {
			var order_by = "";
			if (search_type == "codisname") {
				order_by = " order by codisname, username";
			} else {
				order_by = " order by username, codisname";
			}
			var sql = "select user_codis.id,username,codisname,iswrite,dashboard from user_codis,users,codis where user_codis.userid = users.id and user_codis.codisid = codis.id" + order_by;
			var url = "/sql/" + sql;
			$http.get(url).then(function (resp) {
				 $scope.relation_info_list = resp.data;
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}

		$scope.getCodisNameByUsername = function () {
			var userid = $("#updaterelaUsername").val();

			$scope.updaterelaCodisname = "noselect";
			$("#updaterelaCodisname").select2();
			$('input:radio[name=updateiswrite]').removeAttr('checked');

			if (username == null) {
				$scope.relation_username_null = true;
				$scope.relation_username_tips = "请选择用户名";
			} else {
				$scope.relation_username_null = false;
				$scope.relation_username_tips = "";
			}

			var sql = "select user_codis.id, codisname from user_codis, codis where userid = " + userid + " and codisid = codis.id order by codisname";
			var url = "/sql/" + sql;
			$http.get(url).then(function (resp) {
				 $scope.update_rela_codis_list = resp.data;
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}
		$scope.fillRelationInfo = function () {
			$scope.user_codis_id = $("#updaterelaCodisname").val();

			if ($scope.user_codis_id == null) {
				$scope.relation_codisname_null = true;
				$scope.relation_codisname_tips = "请选择集群名";
			} else {
				$scope.relation_codisname_null = false;
				$scope.relation_codisname_tips = "";
			}

			var sql = "select iswrite from user_codis where id = " + $scope.user_codis_id;
			var url = "/sql/" + sql;
			$http.get(url).then(function (resp) {
				 fillUpdateIswrite(resp.data[0].iswrite);
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}

		$scope.getCodisNotBind = function () {
			$scope.isUseridNull();
			var username = $("#relaUsername").val();
			var sql = "select id, codisname from codis where id not in (select codisid from user_codis where userid = " + info_map[username] + ")"
			var url = "/sql/" + sql;
			$http.get(url).then(function (resp) {
				if (resp.data.length == 0) {
					$("#relaCodisname option:first").text("---已添加所有集群---");
				} else {
					$("#relaCodisname option:first").text("---请选择---");
				}
				$scope.relaCodisname = "noselect";
				$("#relaCodisname option:first").prop("selected", true).select2();
				$("#relaCodisname").select2();
				$('input:radio[name=iswrite]').eq(1).prop('checked',true);
				$scope.rela_codis_info_list = resp.data;
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			}); 
		}

		$scope.addRelationInfo = function () {
			var sqlparams = getRelationAddParams();
			if (sqlparams.username == null) {
				$scope.relation_user = "用户名不能为空";
				$scope.is_userid_null = true;
			} else {
				$scope.relation_user = "";
				$scope.is_userid_null = false;
			}
			if (sqlparams.codisname == null) {
				$scope.relation_codis = "Codis名称不能为空";
				$scope.is_codisid_null = true;
			} else {
				$scope.relation_codis = "";
				$scope.is_codisid_null = false;
			}
			if ($scope.is_userid_null || $scope.is_codisid_null) {
				return ;
			} else {
				$scope.relation_user = "";
				$scope.relation_codis = "";
			}
			
			var sql = "insert into user_codis(userid, codisid, iswrite) values('" + info_map[sqlparams.username] + "','" + sqlparams.codisname + "'," + sqlparams.iswrite + ")";
			var url = "/sql/"+sql;
			$http.get(url).then(function (resp) {
				var result = resp.data;
				if (!isNaN(result)) {
					$scope.getCodisNotBind();
					$scope.relaCodisname = "noselect";
					$("#relaCodisname option:first").prop("selected", true).select2();
					$('input:radio[name=iswrite]').eq(1).prop('checked',true);
					alert("权限添加成功");
				} else {
					alert("权限添加失败");
				}
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}

		$scope.isUseridNull = function(){
			if ($("#relaUsername").val() == "noselect") {
				$scope.relation_user = "用户名不能为空";
				$scope.is_userid_null = true;
			} else {
				$scope.relation_user = "";
				$scope.is_userid_null = false;
			}
		}

		$scope.isCodisidNull = function(){
			if ($("#relaCodisname").val() == "noselect") {
				$scope.relation_codis = "Codis名称不能为空";
				$scope.is_codisid_null = true;
			} else {
				$scope.relation_codis = "";
				$scope.is_codisid_null = false;
			}
		}

		$scope.searchRelationInfoList = function(typeid, infoid) {
			var sql_where = genRelationSearchSqlWhere(typeid, infoid);
			var sql = "select user_codis.id, username, codisname, dashboard, iswrite from user_codis, users, codis where user_codis.userid = users.id and user_codis.codisid = codis.id " + sql_where + " order by username";
			var url = "/sql/" + sql;
			$http.get(url).then(function (resp) {
				$scope.relation_info_list = resp.data;
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}

		$scope.getSelectListByType = function(typeid, showid) {
			var search_type = $("#"+typeid).val();
			if (search_type == "username") {
				var sql = "select username as searchname from users";
			} else if (search_type == "codisname") {
				var sql = "select codisname as searchname from codis";
			}
			$scope.getSearchList(sql);
			$scope.relationSearchInfo = "all";
			$scope.removereRelationSearchInfo = "all";
			$("#" + showid + " option:first").prop("selected", true).select2();
			$scope.getRelationInfo(search_type);
		}

		$scope.getSearchList = function (sql) {
			var url = "/sql/" + sql;
			$http.get(url).then(function (resp) {
				$scope.search_list =  resp.data;
			},function (failedResp) {
			// 请求失败
				alert("请求失败");
			});
		}

		$scope.updateRelationInfo = function () {
			var sqlparams = getRelationUpdateParams();
			var username = $("#updaterelaUsername").val();
			var codisname = $("#updaterelaCodisname").val();
			if (username == null) {
				$scope.relation_username_null = true;
				$scope.relation_username_tips = "请选择用户名";
			} else {
				$scope.relation_username_null = false;
				$scope.relation_username_tips = "";
			}

			if (codisname == null) {
				$scope.relation_codisname_null = true;
				$scope.relation_codisname_tips = "请选择集群名";
			} else {
				$scope.relation_codisname_null = false;
				$scope.relation_codisname_tips = "";
			}

			if ($scope.relation_username_null 
				|| $scope.relation_codisname_null 
				|| typeof(sqlparams.iswrite)=="undefined") {
				return ;
			}

			var sql = "update user_codis set iswrite='" + sqlparams.iswrite + "' where id=" + $scope.user_codis_id;
			var url = "/sql/"+sql;
			$http.get(url).then(function (resp) {
				var result = resp.data;
				if (!isNaN(result)) {
					alert("修改成功");
				} else {
					alert("修改失败");
				}
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}

		$scope.removeRelationInfo = function () {
			var delUserCommand = getDelListSqlWhere("delrelation");
			if (delUserCommand == "") {
				alert("请选择要删除的权限关系");
				return ;
			}
			var sql = "delete from user_codis where " + delUserCommand;
			var url = "/sql/"+sql;
			$http.get(url).then(function (resp) {
				var result = resp.data;
				if (!isNaN(result)) {
					alert("删除成功");
				} else {
					alert("删除失败");
				}

				$scope.searchRelationInfoList('removereRelationSearchType','removereRelationSearchInfo');
				/*$scope.removereRelationSearchInfo = "all";
				$("#removereRelationSearchInfo option:first").prop("selected", true).select2();
				$scope.getRelationInfo($("#removereRelationSearchType").val());*/
			},function (failedResp) {
				// 请求失败
				alert("请求失败");
			});
		}
	}
])
;