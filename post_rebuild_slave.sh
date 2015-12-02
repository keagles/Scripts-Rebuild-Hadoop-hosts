#!/bin/sh

# This script runs at localhost to handlel post-rebuild checks.

HOST=`hostname`
LOGFILE="/hadoop8/post_rebuild_slave.log"


post_config(){
  umount /dev/sdi1 2>&1 >> $LOGFILE
    sed -i '/\/hadoop9/d' /etc/fstab
    sed -i '/\/hadoop10/d' /etc/fstab
    sed -i '/\/hadoop11/d' /etc/fstab
    sed -i '/\/hadoop12/d' /etc/fstab
}

# check root is not mounted on sdi
check_mount(){

  result=`mount|grep hadoop1|grep sdi`
  if [ -z "$result" ];then
    echo `date`  "INFO: Checking mount passed. " >> $LOGFILE
    return 0
    else
      echo `date`  "Critical: Checking mount failed. " >> $LOGFILE
      return 1
    fi
}

# run ol-reload
ol_reload(){

  sudo ol-reload
  status=$?
  if [ $status -eq 0 ];then
    echo `date` "INFO: OL-reload completed sucessfully." >> $LOGFILE
    return 0
  else
    echo `date` "Critical: failed to start CM agent" >> $LOGFILE
    return 1  
  fi
}

# Restart cm agent 
start_cm_agent(){

  sudo /etc/init.d/cloudera-scm-agent restart 2>&1 >> $LOGFILE
  sleep 60
  sudo /etc/init.d/cloudera-scm-agent status 2>&1 >> $LOGFILE
  status=$?
  if [ $status -eq 0 ];then
    echo `date` "INFO: CM agment restarted suceesfully" >> $LOGFILE
    return 0
  else
    echo `date` "Critical: failed to start CM agent" >> $LOGFILE
    return 1
  fi
  
}

# run puppet
puppet(){
  echo `date` "INFO: Run puppet.." >> $LOGFILE
  sudo puppet-run -y 
  sleep 10
  sudo puppet-run -y 
  echo `date` "INFO: sleep 3 min waiting for puppet turn to complete..." >> $LOGFILE
  sleep 180
}

# restart pepper data
pepper_data(){
  sudo service pepagentd restart 2>&1 >> $LOGFILE
  sudo service pepcollectd restart 2>&1 >> $LOGFILE
  sudo service pepagentd status 2>&1 >> $LOGFILE
  status1=$?
    sudo service pepcollectd status 2>&1 >> $LOGFILE
    status2=$?
      if [ $status1 -eq 0 ] && [ $status2 -eq 0 ] ;then
        echo `date` "INFO: CM agment restarted suceesfully" >> $LOGFILE
    return 0
      else
        echo `date` "Critical: failed to start CM agent" >> $LOGFILE
    return 1
      fi
}

restore_hadoop_conf(){
    echo `date` "INFO: Restore hadoop client conf.."
    tar -xf /hadoop8/hadoop-conf.tar -C /
}

# start dn and tt
start_hadoop(){

  sudo service datanode start 2>&1 >> $LOGFILE
  sudo service tasktracker start 2>&1 >> $LOGFILE
  sleep 10
  dn_status=`sudo service datanode status|awk -F':' '{print $1}'` 
  tt_status=`sudo service datanode status|awk -F':' '{print $1}'` 

  if  [ "$dn_status" = "datanode is running" ] ||  [ "$dtt_status" = "tasktracker is running" ]; then
    echo `date` "INFO: Datanode  and tasktracker started sucessfully" >> $LOGFILE
        return 0
    else 
      echo `date` "Critical: Failed to start datanode or tasktracker. " >> $LOGFILE
    return 1
  fi

}

post_rebuild_complete_email(){
echo "post-rebuiled completed on $HOST."| mail -s "From $HOST: post-rebuiled completed sucessfully" DataInfrastructure@turn.com  
}

post_rebuild_failed_email(){
echo "post-rebuiled failed. please check https://intranet.turn.com/display/TechOps/Satadom+rebuild+post-config+failed."|mail -s "From $HOST: post-rebuiled failed" DataInfrastructure@turn.com 
}

#Run the script
run(){
    # run post config
    echo `date` "INFO: Run post config to remove satadom drive sdi from /etc/fstab" >> LOGFILE
    post_config
    echo `date`  "INFO: Checking mount... " >> $LOGFILE
    check_mount
    status=$?
    if [ $status -ne 0 ];then
    post_rebuild_failed_email
        exit 1
    fi
 
    #run puppet
    
    puppet

    #run ol-reload
    echo `date` "INFO: Run ol-reload.." >>  $LOGFILE
    ol_reload
    status=$?
    if [ $status -ne 0 ];then
    post_rebuild_failed_email
        exit 1
    fi

    #restart cm agent
    echo `date` "INFO: Restart CM agent.." >> $LOGFILE
    start_cm_agent
    status=$?
    if [ $status -ne 0 ];then
    post_rebuild_failed_email
        exit 1
    fi

    # restart pepper data
    echo `date` "INFO: restart pepper data services.." >> $LOGFILE
    pepper_data
    status=$?
    if [ $status -ne 0 ];then
    post_rebuild_failed_email
        exit 1
    fi
    
    restore_hadoop_conf

    #restart hadoop dn and tt
    echo `date` "INFO: Restart hadoop services..." >> $LOGFILE
    start_hadoop
    status=$?
    if [ $status -ne 0 ];then
      echo `date` "Critical: Failed to start hadoop services, rebuild failed. Existing..." >> $LOGFILE
    post_rebuild_failed_email
        exit 1
    else
      echo `date` "INFO: Rebuild completed sucessfully. Existing..." >> $LOGFILE
      touch /tmp/post_rebuild_success
      post_rebuild_complete_email
      exit 0
    fi
}

run
