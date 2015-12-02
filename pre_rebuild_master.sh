#!/bin/sh

# This sript runs on master node to do two things:
# 1: Check if there are hosts completed healcheck. send the hosts to rebuild or fail based on the status.
# 2: Launch health check on new hosts
# This script runs every 60 min, 

#queues used in this script
SATADOM_HOSTS="/root/satadom/master_list.hosts"
HOST_HEALTH_CHECK_PENDING="/var/log/satadom/host_health_check_pending.hosts"
HOSTS_FAILED_ON_HEALTHCHECK="/var/log/satadom/host_failed_on_health_check.hosts"
HOSTS_REBUILD_IN_PROGRESS="/var/log/satadom/host_rebuild_in_progress.hosts"
HOSTS_POST_REBUILD_PENDING="/var/log/satadom/host_post_rebuilding_pending.hosts"
HOSTS_FAILED_POST_REBUILD="/var/log/satadom/host_failed_post_rebuild.hosts"
DEAD_HOST_LIST="/var/log/satadom/dead_hosts.hosts"

LOGFILE="/var/log/pre_rebuild_master.log"

AUP_JOBS="/tmp/satadom_aup_jobs"


# Checking health_check pending queue.
check_health_check_pending(){

for i in $(cat $HOST_HEALTH_CHECK_PENDING)
    do
	    timeout 10 ssh $i "sudo ls /tmp/health_check_success"
	    status1=$?
	    timeout 10 ssh $i "sudo ls /tmp/health_check_failed"
	    status2=$?
        if [ $status1 -eq 0 ];then
		    echo `date` "INFO: Health check succeeded on Host $i. Remove $i from HOST_HEALTH_CHECK_PENDING queue and start rebuild" >> $LOGFILE 
		    sed -i "/$i/d" $HOST_HEALTH_CHECK_PENDING  
		    rebuild $i   
	   elif [ $status2 -eq 0 ];then
	        echo `date` "Critical: Host $i failed health check. Move $i to HOSTS_FAILED_ON_HEALTHCHECK queue " >> $LOGFILE
	        sed -i "/$i/d" $HOST_HEALTH_CHECK_PENDING              
	        health_check_failed $i
	   else
	       echo `date` "INFO: Health check is still in progress on $i, will check again.." >> $LOGFILE   
	   fi
    done	
 
 }   

#check master list
check_master_list(){

	list=`cat $SATADOM_HOSTS |wc -l`
	if [ $list -eq 0 ];then
		echo `date` "Warning: There is no host in master list to rebuild" >> $LOGFILE
		return 1
	else
		return 0
	fi

}

check_missing_block(){

	echo `date`  "INFO: Master starts checking missing block through fsck... " >> $LOGFILE
	courrupted_file=`sudo -u apps hdfs fsck -list-corruptfileblocks|grep blk|awk '{print $2}'|sort|uniq` 
	echo "corrupted file list is "$courrupted_file >> $LOGFILE
    #if courrupted file is empty then there is no corruted file
	if [ -z "$courrupted_file" ];then
		echo `date`  "INFO: No corrupted files found by fsck" >> $LOGFILE
		return 0
	else 
		echo `date`  "Critical: Master found missing block, cannot launch new rebuild..." >> $
		echo `date` "Pre-rebuld master found missing block, cannot launch new rebuild."|mail -s "Pre-rebuild Master check failure notification" liang.fang@turn.com
		return 1
	fi
}

# Check if queues are full. 
check_queues(){

	echo `date` "Start checking queues to determine if it is ok to rebuild a new host..." >> $LOGFILE
	echo `date` "New health check can start if  the total queue length <=10 and health_check queue lenth <=1" >> $LOGFILE
    health_check_pending_hosts=`cat $HOST_HEALTH_CHECK_PENDING |wc -l`
    echo `date` "INFO: Found $health_check_pending_hosts host in HOST_HEALTH_CHECK_PENDING queue " >> $LOGFILE
	health_check_failed_hosts=`cat $HOSTS_FAILED_ON_HEALTHCHECK |wc -l`
	echo `date` "INFO: Found $failed_hosts host in failed on healthcheck list" >> $LOGFILE
	health_check_queues=$(($health_check_pending_hosts+$health_check_failed_hosts))
	echo `date` "INFO: The total health check queue length is $health_check_queues." >> $LOGFILE
	hosts_rebuild_in_progress=`cat $HOSTS_REBUILD_IN_PROGRESS|wc -l`
    echo `date` "INFO: Found $hosts_rebuild_in_progress host in build in-progress queue" >> $LOGFILE
	post_rebulild_pending=`cat $HOSTS_POST_REBUILD_PENDING|wc -l`
	echo `date` "INFO: Found $post_rebulild_pending host in post_rebuild queue" >> $LOGFILE
	post_rebulild_failed=`cat $HOSTS_FAILED_POST_REBUILD|wc -l`
	echo `date` "INFO: Found $post_rebulild_failed host in post_rebuild queue" >> $LOGFILE
	queue_length=$(($health_check_pending_hosts+$health_check_failed_hosts+$hosts_rebuild_in_progress+$post_rebulild_pending+$post_rebulild_failed))
	echo `date` "INFO: the total queue length is $queue_length, the HOST_HEALTH_CHECK_PENDING is $health_check_pending_hosts, the HOSTS_FAILED_ON_HEALTHCHECK queue is $health_check_failed_hosts"  >> $LOGFILE
	
	if [ $queue_length -lt 10 ] && [ $health_check_pending_hosts -lt 1 ];then
		echo `date` "INFO: Rebuild queue check passed, get a new host from master list..." >> $LOGFILE
		return 0
    else 
    	echo `date` "Critical: Rebuld queues are full, will retry in the next run ..." >> $LOGFILE
    	echo `date` "Rebuild queues are full, please check the logs"|mail -s "Pre-rebuild Master check failure notification" liang.fang@turn.com 
    	return 1
    fi
}

#check if the host is live
get_host(){

	echo `date` "INFO: Start to find out a host that has the longest running task less than 15 min" >> $LOGFILE
	new_host=""
	for i in $(cat $SATADOM_HOSTS)
	do 
	    echo `date` "INFO: Checking $i.." >> $LOGFILE
	    execution_seconds=`timeout 10 ssh $i ps -ef --sort=start_time|less|grep java|grep attempt\
	    |egrep -v 'tasktracker|datanode|pepagent|overlord-controller'|head -1\
	    |awk '{print $7}'|awk -F':' '{print $1*3600+$2*60+$3}'`
        if [ $execution_seconds -lt 900 ];then
        	echo `date` "INFO: Found target host $i, the longest running task is $execution_seconds seconds.. Further check if there are any aup jobs running on this host.." >> $LOGFILE
        	ssh $i ps -ef|grep -f $AUP_JOBS
        	aup_list=$?
        	if [ $aup_list -ne 0 ];then       
        	    echo `date` "INFO: No AUP jobs found on $i." >> $LOGFILE
        	    new_host=$i
        	    break
            else
                echo `date` "INFO: Fond AUP jobs on $i. Will check the next host." >> $LOGFILE
           fi
        else echo `date` "INFO: $i has long running tasks. The longest task execution time is $execution_seconds seconds. Will check the next host" >> $LOGFILE  
       fi
    done
    
    health_check $new_host

}

#launch health check on new host
health_check(){

 new_host=$1
 echo `date` "INFO: Starting health check on host $new_host..." >> $LOGFILE
 #remove the new host from master list and send it to healthcheck pending queue..
 echo `date` "INFO: Move the $new_host to HOST_HEALTH_CHECK_PENDING queue..." >> $LOGFILE
 echo $new_host >> $HOST_HEALTH_CHECK_PENDING
 sed -i "/$new_host/d" $SATADOM_HOSTS
 timeout 10 ssh $new_host screen -d -m "sudo sh /hadoop8/health_check_slave.sh"

}

rebuild(){

	host_to_rebuild=$1
	echo `date` "INFO: Starting rebuild on $host_to_rebuild. $host_to_rebuild will reboot automatically. " >> $LOGFILE
	echo `date` "INFO: Move $host_to_rebuild to HOSTS_REBUILD_IN_PROGRESS queue." >> $LOGFILE
	echo $host_to_rebuild >> $HOSTS_REBUILD_IN_PROGRESS
	timeout 10 ssh $host_to_rebuild "touch /tmp/rebuild-with-preserve;touch /tmp/satadom_rebuild"
    timeout 10 ssh $host_to_rebuild screen -d -m "sudo /admin/rebuild.rb"

}

health_check_failed(){

	host_failed_health_check=$1
	echo $host_failed_health_check >> $HOSTS_FAILED_ON_HEALTHCHECK

}

start(){

check_health_check_pending

check_missing_block
status=$?
if [ $status -ne 0 ];then
    exit 1	
fi

check_master_list
status=$?
if [ $status -ne 0 ];then
	echo "There is no host in master list to rebuild. Existing..."|mail -s "Master list $SATADOM_HOSTS is empty." liang.fang@turn.com 
    exit 1	
fi

check_queues
status=$?
if [ $status -ne 0 ];then
    exit 1	
fi

get_host

exit 0
}

start  

