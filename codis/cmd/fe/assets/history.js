'use strict';

function initTimer(start, end) {
    $(function(){
        $('#start_time').datetimepicker({
            timeFormat: "HH:mm",
            dateFormat: "yy-mm-dd",
            changeMonth: true,
            changeYear: true,
        });

        $('#end_time').datetimepicker({
            timeFormat: "HH:mm",
            dateFormat: "yy-mm-dd",
            changeMonth: true,
            changeYear: true,
        });
        $.datepicker.setDefaults($.datepicker.regional['zh-CN']);
    });


    var now = Date.parse(new Date());   //now date
    var end_time = (new Date(now - end*3600*1000)).Format("yyyy-MM-dd hh:mm");
    var start_time = (new Date(now - start*3600*1000)).Format("yyyy-MM-dd hh:mm");
   
    setDateValue(start_time,end_time);
}

function getTimeById(id) {
    var date = $('#' + id).val();
    date = new Date(Date.parse(date.replace(/-/g, "/")));
    date = date.getTime();
    return date;
}

function showHistory(id, data, search_type, search_list, query_list = "", select_list = "") {
    var my_chart = echarts.init(document.getElementById(id));
    if (!data.columns || !data.values || data.values.length<=0){
        my_chart.clear();
        return ;
    }

    var field_data = [];
    var legend_arr = [];
    var query_arr = [];
    var select_arr = [];
    var selected_list = {};
    var series_arr = [];
    var response_data = data.values;

    if (query_list == "") {
        query_list = search_list;
    }
    if (select_list == "") {
        select_list = search_list;
    }
    legend_arr = search_list.split(",");
    query_arr = query_list.split(",");
    select_arr = select_list.split(",");

    for (var i=0; i<legend_arr.length; i++) {
        selected_list[legend_arr[i]] = false;
    }
    for (var i=0; i<select_arr.length; i++) {
        selected_list[select_arr[i]] = true;
    }

    for (var i=0; i<response_data[0].length; i++){
        field_data[i] = new Array();
    }

    for (var i=0; i<response_data.length; i++){
        for (var j=0; j<response_data[0].length; j++){
            if (j == 0) {
                field_data[j].push((new Date(response_data[i][j])).Format("yyyy-MM-dd hh:mm:ss"));
            } else {
                if (response_data[i][j] != null) {
                    if (search_type.indexOf("memory") != -1 || search_type.indexOf("used_ssd") != -1) {
                        field_data[j].push((response_data[i][j]/1000000).toFixed(2));
                    } else {
                        field_data[j].push(Math.round(response_data[i][j]));
                    }
                } else {
                    field_data[j].push(response_data[i][j]);
                }
            }
        }
    }

    for (var i=0; i<legend_arr.length; i++) {
        var index = -1;
        for (var j=0; j<query_arr.length; j++) {
            if (legend_arr[i] == query_arr[j]) {
                index = j;
            }
        }
        if (index < 0) {
            continue;
        }
        series_arr.push({
            name:legend_arr[i],
            type:'line',
            symbol:'circle',
            showSymbol: false,
            smooth: true,
            data:field_data[index+1],
        });
    }

    var interval = Math.floor(data.values.length/8);
    var yAxis = {
        type: 'value',
        scale: true,
        splitLine: {
            show: true
        },
        boundaryGap: ['0%', '10%'],
    };
    if (search_type.indexOf("used_memory") != -1 || search_type.indexOf("used_ssd") != -1) {
        yAxis.minInterval = 1;
    }

    var toolbox = {};
    //if (id == "historyCharts") {
    toolbox = {
        feature: {
            dataZoom: {
                yAxisIndex: 'none',
                title: {
                    zoom: '缩放',
                    back: '还原'
                }
            },
            dataView: {
                show: true,
                title: "数据视图",
            }
        },
        top: 10,
    }
    //}
    
    my_chart.setOption({
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                animation: false
            },
        },
        legend: {
            data: legend_arr,
            selected: selected_list
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true  //包括坐标轴在内的矩形区域的设定
        },
        xAxis: {
            type: 'category',
            //position: 'top',   将x轴放置在上面，如果有第二个X轴则放在其对面
            axisTick: {
                alignWithLabel: false
            },
            axisLabel: {
                interval: interval,
                formatter: function(category)
                {
                    return category.substring(11,19) + "\n" + category.substring(5,10);
                },
            },
            splitLine:{  
                show: true,
            }, 
            data: field_data[0],
            boundaryGap: false,
        },
        toolbox: toolbox,
        yAxis: yAxis,
        series: series_arr,
    });
    return my_chart;
}

function getTableName(typestr, orgi_name){
    return  typestr + orgi_name.replace(/\./g, "_").replace(/\:/g, "_");
}

function getShowSqlCommand(table_name) {
    return "SHOW TAG VALUES FROM \"" + table_name + "\" WITH KEY=cmd_name";
}

function getSelectSqlCommand(start_seconds, end_seconds, table_name, search_type, search_cond, influxdb_period = 1) {
    var dif_timer = (end_seconds - start_seconds)/1000;   //s
    var step_timer = parseInt(dif_timer/800);
    var format_table_name = "";

    format_table_name = "\"";
    for (var i=0; i<table_name.length; i++) {
        if (',' == table_name[i]) {
            format_table_name += "\",\"";
        } else {
            format_table_name += table_name[i];
        }
    }
    format_table_name += "\"";
    //最小取值为1
    if (step_timer <= influxdb_period) { 
        step_timer = influxdb_period < 1 ? 1 : influxdb_period;
    }
    
    var search_arr = search_type.split(",");
    var mean_field = "";
    var sql_func = "mean";
    for (var i=0; i<search_arr.length; i++) {
        if (search_arr[i] == "tp100") {
            sql_func = "max";
        } else {
            sql_func = "mean";
        }
        /*if (search_arr[i] == "tp90") {
            sql_func = "percentile";
            search_arr[i] = search_arr[i] + ",90";
        } else if (search_arr[i] == "tp99") {
            sql_func = "percentile";
            search_arr[i] = search_arr[i] + ",99";
        } else if (search_arr[i] == "tp999") {
            sql_func = "percentile";
            search_arr[i] = search_arr[i] + ",99.9";
        } else if (search_arr[i] == "tp9999") {
            sql_func = "percentile";
            search_arr[i] = search_arr[i] + ",99.99";
        }*/
        if (i != search_arr.length - 1) {
            mean_field += sql_func + "(" + search_arr[i] + "),";
        } else {
            mean_field += sql_func + "(" + search_arr[i] + ")";
        }
    }
    if (search_cond != "") {
        search_cond = "and " + search_cond
    }
    return "select " + mean_field + " from " + format_table_name + " where time > " + start_seconds + "ms and time < " + end_seconds + "ms " + search_cond + " group by time(" + step_timer + "s)";
}

function clearHistorychart(id) {
	var myChart = echarts.init(document.getElementById(id));
	myChart.clear();
}

/*function historyHide() {
    $("#demo").removeClass("in");
}

function historyShow() {
    $("#demo").addClass("in");
}
*/
function setColor(old_id, id) {
    $("#" + old_id).css('background-color','lightgray');
    $("#" + id).css('background-color','#84c1ff');
}

function setDateValue(start, end) {
    $('#start_time').val(start);
    $('#end_time').val(end);
}

