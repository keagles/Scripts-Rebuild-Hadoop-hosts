#!/bin/sh

# This script runs to make sure /hadoop1 can be safely unmounted before rebuild/reboot the host.
# This script runs at localhost
# To check missing block, it has to wait 21 min after DN is fullly stopped. The formula is 2*dfs.namenode.heartbeat.recheck.interval + 10 * heartbeat.interval

HOST=`hostname`
LOGFILE="/hadoop8/health_check_slave.log"
SUCCESS_FLAG="/tmp/health_check_success"
FAILURE_FLAG="/tmp/health_check_failed"
MISSING_BLOCK_FLAG='/tmp/missing_block'

#schedule downtime
schedule_downtime(){

	sudo -u lfang /tmp/nagios_host_dt.rb
}

# stop TT and DN
stop_services(){

	echo `date` "INFO: Stopping hadoop services..." >> $LOGFILE
	sudo service tasktracker stop 2>&1 >> $
	sleep 10
	sudo service tasktracker stop 2>&1 >> $LOGFILE
	sudo service datanode stop 2>&1 >> $
	sleep 10
	sudo service datanode stop 2>&1 >> $
	# Keep  checking if DN is sucesfully stopped. timeout after 30 min
	for i in {1..180}
	do 
	    tt_status=`sudo service tasktracker status`
	    dn_status=`sudo service datanode status` 
	    if [ "$tt_status" = "No tasktracker running" ] && [ "$dn_status" = "No datanode running" ]; then
		    break
	    fi
    sleep 10
    done

    if [ "$tt_status" = "No tasktracker running" ] && [ "$dn_status" = "No datanode running" ]; then
        echo `date` "INFO: datanode and tasktracker stopped sucessfully"  >> $LOGFILE
        return 0
    else 
        echo `date` "Critical: Failed to stop datanode or tasktracker, exiting... " >> $LOGFILE
        return 1
	fi

}

#check missing block
check_missing_block1(){

	echo `date`  "INFO: Checking missing block through fsck... " >> $LOGFILE
	courrupted_file=`sudo -u apps hdfs fsck -list-corruptfileblocks|grep blk|awk '{print $2}'|sort|uniq` 
	echo "corrupted file list is "$courrupted_file >> LOGFILE
    #if courrupted file is empty then there is no corruted file
	if [ -z "$courrupted_file" ];then
		echo `date`  "INFO: No corrupted files found by fsck" >> $LOGFILE
		return 0
	else 
		# restart DN
		touch $MISSING_BLOCK_FLAG
		echo `date` "Warning: The following corrupted files found by fsck" $courrupted_file >> $LOGFILE
		echo `date` "INFO: Will try restart services and replicate the corrupted files..." >> $LOGFILE
		echo `date` "INFO: Start dn service.." >> $LOGFILE
		sudo service datanode start 2>&1 >> $LOGFILE
		echo `date` "INFO: sleep for 10 min to wait dn fully started and missing blocks are recaptured.." >> $LOGFILE
		sleep 600
		
		# replicate files
		echo `date` "INFO: replicate the courrupted files, will timeout after 10 min" >> $LOGFILE
		for i in $courrupted_file
		do 
		    #first check if file exist or not, if not do no replicate the file, timeout in 1 sec.
		    sudo -u apps hadoop fs -ls $i
		    file_exist=$?
		    if [ $file_exist -eq 0 ];then
		    	timeout_seconds=600
		    else
		    	timeout_seconds=1
		    fi
		    timeout $timeout_seconds sudo -u apps hadoop fs -setrep -w 3 $i 2>&1 >> $LOGFILE
	    done

	    echo `date` "INFO: Stop dn again to check missing block still present...."
	    sudo service datanode stop 2>&1 >> $LOGFILE
	    echo `date` "INFO waiting for 10 min to make sure DN is fully stopped."
	    sleep 600

		echo `date` "INFO: waiting for 21 min then check corrupted files using fsck again" >> $LOGFILE
		sleep 1260
        courrupted_file=`sudo -u apps hdfs fsck -list-corruptfileblocks|grep blk|awk '{print $2}'|sort|uniq` 
        if [ -z "$courrupted_file" ];then
		echo `date`  "INFO: No corrupted files found by fsck" >> $LOGFILE
		    return 0
	    else 
	    	echo `date` "Critical: Corrupted files still presents, fail health check..." >> $LOGFILE
	    	sudo service datanode start
	        return 1
        fi	
	fi
}

check_missing_block2(){

	echo `date` "INFO: Checking missing block from JMX... "  >> $LOGFILE
	courrupted_file=`/tmp/turn_check_hadoop_blocks.rb -H 69.194.253.58 -c 5000 -m 0|awk -F':' '{print $1}'`
	if  [ "$courrupted_file" = "OK" ]; then
		echo `date` "INFO: No corrupted files found" >> $LOGFILE
        return 0
    else 
    	echo `date` "Critical: Corrupted blocks found on jmx. " >> $LOGFILE
		return 1
	fi

}


# send email notice
health_check_failure_email(){

	subject="From $HOST: health check failed on healtcheck."
	echo "Please check https://intranet.turn.com/display/TechOps/Satadom+rebuild+health+check+failure" | mail -s "$subject" DataInfrastructure@turn.com 
}

health_check_success_email(){
	subject="From $HOST: Pre-rebulild health check succeeded. "
	echo "Pre-rebulild health check succeeded" | mail -s "$subject" DataInfrastructure@turn.com
}

backup_hadoop_conf(){

	 tar cvfh /hadoop8/hadoop-conf.tar /etc/hadoop/conf
}

#Run the script
run(){

	schedule_downtime

	stop_services
	status=$?
	echo $status
	if [ $status -ne 0 ];then
		health_check_failure_email
        exit 1
    fi
    
    echo `date` "INFO: Sleep 21 min then checking if there are any missing blocks" >> $LOGFILE
    sleep 1260
	check_missing_block1
	status1=$?
	check_missing_block2
	status2=$?
	if [ $status1 -ne 0 ] || [ $status2 -ne 0 ];then
		health_check_failure_email
		touch $FAILURE_FLAG
		exit 1  
    fi	
    
    backup_hadoop_conf

    health_check_success_email

    touch $SUCCESS_FLAG
}

run