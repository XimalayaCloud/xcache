#!/bin/bash

main_dir=/usr/local/codis-fe
fe_conf=$main_dir/conf/fe.toml
bin=$main_dir/bin/codis-fe

cmd=$1

print_usage()
{
	echo -e "codis-fe run.sh usage"
	echo -e "./run.sh start|stop|restat"
	echo -e "\tstart:\t\tstart codis-fe"
	echo -e "\tstop:\t\tstop codis-fe"
	echo -e "\trestart:\trestart codis-fe"
}

chmod +x $bin
if [ "$?" != 0 ]; then
        echo "codis-fe maybe cannt execute"
        exit 1
fi


if [ "$cmd" == "start" ]
then
	bin_cmd="nohup $bin -c $fe_conf >/dev/null 2>&1 &"
	echo $bin_cmd
	eval $bin_cmd

	if [ "$?" != 0 ]; then
		echo "codis-fe start failed"
		exit 1
	fi

	echo "codis-fe start success"
	exit 0
elif [ "$cmd" == "stop" ]
then
	bin_cmd="$bin -c $fe_conf -s stop >/dev/null 2>&1"
	echo $bin_cmd
	eval $bin_cmd
	if [ "$?" != 0 ]; then
		echo "codis-fe stop failed"
		exit 1
	fi

	echo "codis-fe stop success"
	exit 0
elif [ "$cmd" == "restart" ]
then
	bin_cmd="$bin -c $fe_conf -s stop >/dev/null 2>&1"
	echo $bin_cmd
	eval $bin_cmd
	if [ "$?" != 0 ]; then
		echo "codis-fe stop failed"
		exit 1
	fi

	bin_cmd="nohup $bin -c $fe_conf >/dev/null 2>&1 &"
	echo $bin_cmd
	eval $bin_cmd

	if [ "$?" != 0 ]; then
		echo "codis-fe start failed"
		exit 1
	fi
	
	echo "codis-fe start success"
	exit 0
else 
	print_usage
fi


