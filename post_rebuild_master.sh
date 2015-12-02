#!/bin/sh

# This sript runs on master node to launch the post_rebuild_slave.sh on each nodes.
# This script runs every 30 min. 
# At begining, it will check if any hosts in HOSTS_POST_REBUILD_PENDING completed post-rebuild or failed post-rebuild
# It checks if any hosts in Hosts_rebulild_in_prgress queue have sucessfully rebooted.
# if yes, it will send the hosts to HOSTS_POST_REBUILD_PENDING queue and start post-rebuilding.

#Queue used in this script
HOSTS_REBUILD_IN_PROGRESS="/var/log/satadom/host_rebuild_in_progress.hosts"
HOSTS_POST_REBUILD_PENDING="/var/log/satadom/host_post_rebuilding_pending.hosts"
HOSTS_COMPLETED_POST_REBUILD="/var/log/satadom/host_completed_post_rebuild.hosts"
HOSTS_FAILED_POST_REBUILD="/var/log/satadom/host_failed_post_rebuild.hosts"
#logfile
LOGFILE="/var/log/post_rebuild_master.log"

#Check hosts status in existing HOSTS_POST_REBUILD_PENDING
check_complete_or_fail(){

	for i in $(cat $HOSTS_POST_REBUILD_PENDING)
	    do
	    	timeout 10 ssh $i "sudo ls /tmp/post_rebuild_success"
	    	status=$?
            if [ $status -eq 0 ];then
		        echo `date` "INFO: Host $i completed rebuild. Move $i from HOSTS_POST_REBUILD_PENDING queue to HOSTS_COMPLETED_POST_REBUILD queue" >> $LOGFILE
		        echo `date` $i >> $HOSTS_COMPLETED_POST_REBUILD
		        echo 'date' "INFO: Send notification email for post-rebuild sucess.." >> $LOGFILE
		        echo "Congradulations! $i completed rebuild."|mail -s "INFO: from post-rebulid master,hosts completed rebuild sucessfully." liang.fang@turn.com 
                 
	       else
		        echo `date` "Critical: Host $i failed in post rebuild. Move $i from HOSTS_POST_REBUILD_PENDING queue to HOSTS_FAILED_POST_REBUILD " >> $LOGFILE
		        echo `date` $i >> $HOSTS_FAILED_POST_REBUILD
		        echo 'date' "INFO: Send notification email for post-rebuld failure.." >> $LOGFILE
		        echo "$i Failed post rebuild. Please check local log"|mail -s "Critial: from post-rebulid master,hosts failed post rebuild" liang.fang@turn.com 
		   fi
		 done

	echo `date` "Empty HOSTS_POST_REBUILD_PENDING queue " >> $LOGFILE
	cat /dev/null > $HOSTS_POST_REBUILD_PENDING	
    
}

check_ready_hosts(){
	#check quickstart timestamp
	echo `date`  "INFO: Checking check_kickstart timestamp..." >> $LOGFILE
	for i in $(cat $HOSTS_REBUILD_IN_PROGRESS)
	    do 
	        echo `date` "INFO: Start check quick_start.txt on host $i"  >> $LOGFILE
	        kickstart_result=`timeout 10 ssh $i "find /root/kickstart-date.txt -mmin -120"|awk -F'-' '{print $2}'`
	        if [ "$kickstart_result" = "date.txt" ]; then
	            echo `date` "INFO: Host $i kickstart-date is up-to-date. Continue to check puppet run status" >> $LOGFILE
	            puppet_result=`timeout 10 ssh $i "grep 'Finished catalog run in' /var/log/messages|head -1"|awk '{print $NF}'`
	            if [ "$puppet_result" = "seconds" ];then
	        	    echo `date` "INFO: Puppet run completed. Move $i from HOSTS_REBUILD_IN_PROGRESS queue to HOSTS_POST_REBUILD_PENDING queue... " >> $LOGFILE
		            echo $i >> $HOSTS_POST_REBUILD_PENDING
		            sed -i "/$i/d" $HOSTS_REBUILD_IN_PROGRESS
		        else
		            echo `date` "Warning: Puppet run has not completed on $i yet. leave $i in HOSTS_REBUILD_IN_PROGRESS queue.. " >> $LOGFILE
		        fi
	        else
	        	echo `date` "WARNING: Host $i kickstart-date is not up-to-date, leave $i in HOSTS_REBUILD_IN_PROGRESS queue.. " >> $LOGFILE
	        fi
        done
}

run_post_rebuild(){
	for i in $(cat $HOSTS_POST_REBUILD_PENDING)
	do
		echo  `date` "INFO: starting running post rebuild on host $i" >> $LOGFILE
		timeout 10 ssh $i screen -d -m "sudo sh /hadoop8/post_rebuild_slave.sh"
		echo `date` "INFO: Move $i to HOSTS_POST_REBUILD_PENDING queue..."  >> $LOGFILE
	done
}

start(){
	echo `date` "INFO: Check if there are any hosts in HOSTS_POST_REBUILD_PENDING queue.. " >> $LOGFILE
	host_list=`cat $HOSTS_POST_REBUILD_PENDING|wc -l`
	if [ $host_list -gt 0 ];then
		echo `date` "INFO: found hosts in HOSTS_POST_REBUILD_PENDING queue, check if the hosts completed post-rebuild.. " >> $LOGFILE
		check_complete_or_fail
	else 
		echo `date` "INFO: No host found in HOSTS_POST_REBUILD_PENDING, will ingore rebulild completeion check.." >> $LOGFILE
	fi

	echo `date` "INFO: Check if there are any hosts in HOSTS_REBUILD_IN_PROGRESS queue.. " >> $LOGFILE
	host_list=`cat $HOSTS_REBUILD_IN_PROGRESS|wc -l`
	if [ $host_list -gt 0 ];then
		echo `date` "INFO: found hosts in HOSTS_REBUILD_IN_PROGRESS queue, continue to check if the hosts are ready for post rebuild... " >> $LOGFILE
		check_ready_hosts
	else
        echo `date` "INFO: No host found in HOSTS_REBUILD_IN_PROGRESS queue, won't add new hosts to HOSTS_POST_REBUILD_PENDING queue." >> $LOGFILE
	fi
	echo `date` "INFO: Check if there are any hosts in HOSTS_POST_REBUILD_PENDING queue.. " >> $LOGFILE
	host_list=`cat $HOSTS_POST_REBUILD_PENDING|wc -l`
	if [ $host_list -gt 0 ];then
		echo `date` "INFO: found hosts in HOSTS_REBUILD_IN_PROGRESS queue, start post rebuild on the slave. Exiting master now... " >> $LOGFILE
		run_post_rebuild
	else
		echo `date` "INFO: No hosts found in HOSTS_REBUILD_IN_PROGRESS queue, will not start a new postrebuilding. Existing master now... " >> $LOGFILE 
	fi
	exit 
}

start