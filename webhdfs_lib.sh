#!/bin/bash

CONF_FILE=`pwd`"/settings.conf"
if [ -f "$CONF_FILE" ]
then
 . "$CONF_FILE"
else
 echo "Config-file ${CONF_FILE} was not found;"
 exit 1
fi

if [ ! -e "$LOG_FILE" ]
then
 touch "$LOG_FILE" 1>/dev/null 2>&1
 if [ "$rc" -ne "0" ]  
 then 
  echo "Can not create log-file: ${LOG_FILE}"; exit 1;
 fi
fi

RUNID=`date +%s.%N`

#====================================================
#== Misc ==
log_info() {
local datetime=`date +%Y.%m.%d:%H.%M.%S`
local data_source="$2"
local v_x

if [ "$data_source" == "logfile" ]
then
 [ "$CONSOLE_OUTPUT" == "Y" ] && cat "$1" | awk -v runid=$RUNID '{print runid": "$0}'
 cat "$1" | awk -v runid=$RUNID '{print runid": "$0}' >> $LOG_FILE
else
 [ -e "$LOG_FILE" ] && echo "${RUNID}:${datetime}: $1" >> $LOG_FILE
 [ "$CONSOLE_OUTPUT" == "Y" ] && echo "${RUNID}:${datetime}: $1"
fi

if [ "$FOLLOW_LINE_LIMITS" == "Y" ]
then
 v_x=`cat $LOG_FILE | wc -l`
 if [ "$v_x" -gt "$LOG_FILE_LINES_LIMIT" ]
 then
  v_x=`echo $v_x-$LOG_FILE_LINES_LIMIT | bc`
  sed -i "1,${v_x}d" $LOG_FILE
 fi
fi

}

parse_liststatus() {
$PYTHON << __EOF__
import sys,  json
json_string="""$1"""
parsed_string = json.loads(json_string)
parsed_string=parsed_string['FileStatuses']
parsed_string=parsed_string['FileStatus']
for i in range(len(parsed_string)):
	f=parsed_string[i]
	itype="FILE"
	if f['type'] == "DIRECTORY":
		itype="DIR"
	print('%s\t%s\t%s\t%s\t%s' % (itype, f['permission'], f['modificationTime'], f['length'], f['pathSuffix']))
exit()
__EOF__
}
#== HDSF ==
# mk_dir	- $1: directory name; $2 - permission mode, default 644 for files, 755 for directories; Valid Values 0 - 1777
# ls_dir	-

ls_dir() {
 local v_module="ls_dir"
 local v_count v_cmd rc V_URL=""
 local v_operation="?op=LISTSTATUS"

 V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
 V_URL=${V_URL%$'\r'}
 #log_info "$v_module url: ${V_URL}"
 export V_LOGIN="$V_LOGIN"
 export V_PWD="$V_PWD"
 export V_URL="$V_URL"
 v_cmd="${CURL} -X GET ${GENERIC_OPTION} -u \"$V_LOGIN:$V_PWD\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>&1"
 log_info "$v_module ${v_cmd}" 
 v_count="1"
 while [ "$v_count" -lt "$RETRY_LIMIT" ]
 do
  eval "$v_cmd"
  rc="$?" 
  if [ "$rc" -eq "0" ]
  then
   v_count="$RETRY_LIMIT"
   log_info "$TEMP_FILE" "logfile"
   parse_liststatus $(cat "$TEMP_FILE")
  else
   log_info "$v_module attempt ${v_count} fail ${rc}"
  fi
  ((v_count++))
 done
 return "$rc"
}

mk_dir() {
 local v_module="mk_dir"
 local v_count v_cmd rc V_URL=""
 local v_operation="?op=MKDIRS"
 local v_permission="$2"
 local v_hdfspath="$1"

 v_hdfspath=${v_hdfspath/"/"""} 
 v_hdfspath="/"${v_hdfspath}
 [ ! -z "$v_permission" ] && v_operation="${v_operation}&permission=${v_permission}"
 V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
 V_URL=${V_URL%$'\r'}
 #log_info "$v_module url: ${V_URL}"
 export V_LOGIN="$V_LOGIN"
 export V_PWD="$V_PWD"
 export V_URL="$V_URL"
 [ -f "$TEMP_FILE" ] && cat /dev/null > "$TEMP_FILE"
 v_cmd="${CURL} -X PUT ${GENERIC_OPTION} -u \"$V_LOGIN:$V_PWD\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>&1"
 log_info "$v_module ${v_cmd}"
 v_count="1"
 while [ "$v_count" -lt "$RETRY_LIMIT" ]
 do
   eval "$v_cmd"
  rc="$?"
  if [ "$rc" -eq "0" ]
  then
   v_count="$RETRY_LIMIT"
   log_info "$TEMP_FILE" "logfile"
  else
   log_info "$v_module attempt ${v_count} fail ${rc}"
  fi
  ((v_count++))
 done
 return "$rc"
}
#====================================================

