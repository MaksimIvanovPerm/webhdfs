#!/bin/bash

CONF_FILE="/home/oracle/webhdfs/settings.conf"
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
	if [ "$?" -ne "0" ]  
	then 
		echo "Can not create log-file: ${LOG_FILE}"; exit 1;
	fi
fi

RUNID=`date +%s.%N`
[ ! -z "$SILENT" ] && CONSOLE_OUTPUT="N"
V_PWD=$($OPR -r "$V_OPRATTR" "$V_LOGIN" | tr -d [:cntrl:])
[ "$?" -ne "0" ] && {
	echo "can not obtain passwd for logopass to hdfs;"
	echo "obtaining was executed as: ${OPR} -r ${V_OPRATTR} ${V_LOGIN}"
	echo "exiting with error"
	return 1
}
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
	v_x=${1//${V_PWD}/"..."}
	[ -e "$LOG_FILE" ] && echo "${RUNID}:${datetime}: ${v_x}" >> $LOG_FILE
	[ "$CONSOLE_OUTPUT" == "Y" ] && echo "${RUNID}:${datetime}: ${v_x}"
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
#== Python modules =======================================================
parse_liststatus() {
local v_module="parse_liststatus"
#log_info "${v_module} 1: >${1}<"
if [ ! -z "$1" ]
then
	$PYTHON << __EOF__
import sys,  json
json_string="""$1"""
parsed_string = json.loads(json_string)
if 'FileStatuses' in parsed_string:
	parsed_string=parsed_string['FileStatuses']['FileStatus']
	for i in range(len(parsed_string)):
	        if i == 0:
        	        print('%s\t%s\t%s\t%s\t%s' % ("type", "permission", "mTime", "bytes", "name"))
	        f=parsed_string[i]
        	itype="FILE"
	        if f['type'] == "DIRECTORY":
        	        itype="DIR"
	        print('%s %s %s %s %s' % (itype, f['permission'], f['modificationTime'], f['length'], f['pathSuffix']))
	sys.exit(0)
elif 'RemoteException' in parsed_string:
        parsed_string=parsed_string['RemoteException']
        msg="RemoteException"
        if 'exception' in parsed_string:
                msg=msg+': '+parsed_string['exception']
        if 'message' in parsed_string:
                msg=msg+' '+parsed_string['message']
        print('%s' % (msg))
        sys.exit(1)
else:
	msg="Unknown json from hdfs"
	print('%s' % (msg))
	sys.exit(2)
__EOF__
	return "$?"
fi
return "0"
}

parse_getfilestatus() {
local rc v_msg
if [ ! -z "$1" ]
then
	$PYTHON << __EOF__
import sys,  json
json_string="""$1"""
parsed_string = json.loads(json_string)
if "RemoteException" in parsed_string:
	msg="error: "
	if "exception" in parsed_string['RemoteException']:
		msg=msg+" "+parsed_string['RemoteException']['exception']
	if "message" in parsed_string['RemoteException']:
		msg=msg+parsed_string['RemoteException']['message']
	print('%s' % (msg))
	sys.exit(1)
elif 'FileStatus' in parsed_string:
	parsed_string=parsed_string['FileStatus']
	msg=""
	for k, v in parsed_string.iteritems():
		msg=msg+" "+str(k)+": "+str(v)+"\n"
	print('%s' % (msg))
	sys.exit(0)
else:
	msg="Unknown json-structure"
	print('%s' % (msg))
	sys.exit(2)
__EOF__
	return "$?"
fi
return 0
}

parse_boolean_response() {
local v_module="parse_delete_response"
#log_info "${v_module} 1: >${1}<"
if [ ! -z "$1" ]
then
        $PYTHON << __EOF__
import sys,  json
json_string="""$1"""
parsed_string = json.loads(json_string)
if 'boolean' in parsed_string:
	msg="executed successfully"
	print('%s' % (msg))
	sys.exit(0)
elif 'RemoteException' in parsed_string:
	parsed_string=parsed_string['RemoteException']
	msg="RemoteException"
	if 'exception' in parsed_string:
		msg=msg+': '+parsed_string['exception']
	if 'message' in parsed_string:
		msg=msg+' '+parsed_string['message']
	print('%s' % (msg))
	sys.exit(1)
else:
	msg="Unknown json-format from hdfs"
	print('%s' % (msg))
	sys.exit(2)
__EOF__
        return "$?"
fi
return "0"
}
parse_getxattrs_response() {
local v_module="parse_getxattrs_response"
if [ ! -z "$1" ]
then
	$PYTHON << __EOF__
import sys,  json
json_string="""$1"""
parsed_string = json.loads(json_string)
if "XAttrs" in parsed_string:
	parsed_string=parsed_string['XAttrs']
	if len(parsed_string) == 0:
		sys.exit(0)
	else:
		for i in range(len(parsed_string)):
			f=parsed_string[i]
			msg=f['value']
			if msg is None:
				msg="null"
			print('%s %s' % (f['name'], msg))
		sys.exit(0)
elif 'RemoteException' in parsed_string:
        parsed_string=parsed_string['RemoteException']
        msg="RemoteException"
        if 'exception' in parsed_string:
                msg=msg+': '+parsed_string['exception']
        if 'message' in parsed_string:
                msg=msg+' '+parsed_string['message']
        print('%s' % (msg))
        sys.exit(1)
else:
	msg="Unknown json-format from hdfs"
	print('%s' % (msg))
	sys.exit(2)
__EOF__
        return "$?"
fi
return "0"
}

parse_error_response() {
local v_module="parse_error_response"
[ -z "$1" ] && return 1
$PYTHON << __EOF__
import sys,  json
json_string="""$1"""
parsed_string = json.loads(json_string)
if "RemoteException" in parsed_string:
	parsed_string=parsed_string['RemoteException']
	msg="RemoteException"
	if 'exception' in parsed_string:
	        msg=msg+': '+parsed_string['exception']
	if 'message' in parsed_string:
	        msg=msg+' '+parsed_string['message']
	print('%s' % (msg))
	sys.exit(2)
else:
	msg="Unknown json-format from hdfs"
	print('%s' % (msg))
	sys.exit(3)
__EOF__
return "$?"
}
#============================================================================================
create_file_usage() {
printf "%s\n" "Usage: create_file [options]
Here the options are:
-n|--name	[file name]		- Name of file to create in hdfs; Mandatory;
-l|--local	[file name]		- Name of local-file, which content'll be uploaded to hdfs with name from -n option; Mandatory;
-o|--overwrite	[true|false|t|f]	- Do (t|true) of do not (f|false - default) rewtite hdfs-file, if there is file with given name;
-p|--permission	[0-1777]		- Permission for new file, default: 644;
-m|--maxtime	[int]			- Limit for uploading time; In curl's term it's value for --max-time option; Default 10 seconds;
-r|--replication	[int]		- Replication factor for file in hdfs; Default: 2, valid value: [1-9]{1}
-h|--help				- This help
"
}

delete_usage() {
printf "%s\n" "Usage: del_file [options]
Here the options are:
-n|--name	[name]	- Name of file|directory to delete in hdfs; Mandatory;
-r|--recursive		- Do delete recursively; Default: non-recursively; Optional;
-h|--help		- This help
"
}

getxattr_usage() {
printf "%s\n" "Usage: getxattr [options]
Here the options are:
-n|--name       [name]  - Name of hdfs-attribute (file|folder) which attribute you want to get; Optional
			- In case it not set: root point of your hdfs-hierarchy will be processed;
-a|--attribute	[name]  - Name of attribute to get it out from  hdfs; Optional;
			  In case it empty: all attributes of a given hdfs-item will be retrieved (if they are);
			  Please note, you have to prefix attribute name with 'user.';
			  And it'll be retrieved with that prefix;
			  See https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/ExtendedAttributes.html
-e|--encoding		- How to encode value of retrieved attribue; Optional; Def: text; Allowed: text|hex|base64;
-b|--base64		- In case you asking hdfs about value of an one xattr, and you made the xattr earlier through setxattr -b,
			  You can orderm by -b|--base64 decoding this value of this xattr from base64;
-h|--help               - This help
"
}

setxattr_usage() {
printf "%s\n" "Usage: setxattr [options]
Here the options are:
-n|--name       [name]  - Name of hdfs-attribute (file|folder) which attribute you want to get; Optional
			- In case it not set: root point of your hdfs-hierarchy will be processed;
-a|--attribute  [name]  - Name of attribute to set; Mandatory;
                        - Please note, you have to prefix attribute name with 'user.';
                        - See https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/ExtendedAttributes.html
-v|--value		- Value of attribute; Optional;
			- In case it not set, or set as \"\" hdfs'll return it as 'null'
			- Please avoid to use space and|or unprintable and|or special characters, like quotes; 
			- In case you really need to set such complex string as attr-value: use -b option
-b|--base64		- If that parameter is set: attr-value will be encoded in base64, before posting it to hdfs;
-f|--flag		- create|replace attribute; Mandatory; create - should not be used for attributes which already exist;
			- Def.: create
-h|--help               - This help
"
}

rmxattr_usage() {
printf "%s\n" "Usage: rmxattr [options]
Here the options are:
-n|--name       [name]  - Name of hdfs-attribute (file|folder) which attribute you want to remove; Optional
                        - In case it not set: root point of your hdfs-hierarchy will be processed;
-a|--attribute  [name]  - Name of attribute to set; Mandatory;
                        - Please note, you have to prefix attribute name with 'user.';
                        - See https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/ExtendedAttributes.html
-h|--help               - This help
"
}

mk_dir_usage() {
printf "%s\n" "Usage: mk_dir [options]
Here the options are:
-n|--name       [name]		- Name of directory to create in hdfs; Mandatory; No error if dir alreay is;
				- path with subfolders allowed;
-p|--permission [0-1777]	- Permission for new dir, default: 755;
-h|--help			- This help
"
}

ls_dir_usage() {
printf "%s\n" "Usage: ls_dir [options]
Here the options are:
-n|--name       [name]          - Name of file|folder in hdfs to list; Optional;
                                - In case -n ommited: root-point of your hdfs-path is supposed;
-h|--help                       - This help
"
}

item_status_usage() {
printf "%s\n" "Usage: item_status [options]
Here the options are:
-n|--name       [name]          - Name of file|folder in hdfs to list; Optional;
                                - In case -n ommited: root-point of your hdfs-path is supposed;
-h|--help                       - This help
"
}

getfile_usage() {
printf "%s\n" "Usage: getfile [options]
Here the options are:
-n|--name       [name]	- Name of file in hdfs to download; Mandatory; It has to be file;
-l|--local	[name]	- Name of local file, where to save content of the hdfs-file, which given by '-n' option; Mandatory;
			- If this localfile already is: it'll be overvriten;
-m|--maxtime	[int]	- Limit for downloading time; In curl's term it's value for --max-time option; Default 10 seconds;
-h|--help		- This help

Note: offset, length, buffersize parameter of rest-api OPEN request - currentlly not supported, by this bash-library wrapper;
"
}

appendtofile_usage() {
printf "%s\n" "Usage: appendtofile [options]
Here the options are:
-n|--name       [name]  - Name of hdfs-file, to which append data; Mandatory;
-l|--local      [name]  - Name of local file, from where to read content for appending the hdfs-file; Mandatory;
-m|--maxtime    [int]   - Limit for downloading time; In curl's term it's value for --max-time option; Default 10 seconds;
-h|--help               - This help

Note: buffersize parameter of rest-api APPEND request - currentlly not supported, by this bash-library wrapper;
"
}

renamefile_usage() {
printf "%s\n" "Usage: renamefile [options]
Here the options are:
-n|--name	[name]  - Name of hdfs-item, which you want to rename; Mandatory;
-d|--dest	[name]  - New name of for renaming; Mandatory; It has to be absolute path;
-h|--help               - This help
"
}

setmod() {
printf "%s\n" "Usage: renamefile [options]
Here the options are:
-n|--name       [name]  - Name of hdfs-item, for which you want to change permissions; Mandatory;
-p|--permission	[int]	- The permission of a file/directory, octal, valid value: 0 - 1777; Mandatory;
-h|--help               - This help
"
}

usage() {
printf "%s\n" "HDFS-interface's subprograms brief desc:
mk_dir		- Make hdfs-directory with given name and permission mode.
		  Default permission is 644 for files, 755 for directories; Valid Values 0 - 1777
ls_dir		- Listing items in given hdfs-directory, or show some info about given hdfs-file;
itemstatus	- Show info about given at hdfs-item
		  In case of success it returns block of new-line delimited lines, with hdfs-item attributes, in key-valu form;
		  see FileStatus JSON object in webhdfs doc;

createfile	- Make file at hdfs, by uploading there given local file;
delete		- Delete file|directory at hdfs, optionally - recursively;
getfile         - Get given file from hdfs
appendtofile    - Append content of given local-file to given hdfs-file;
renamefile	- Renaming
setmod		- Set permission of a file/directory;

getxattr	- Get out from hdfs extended attribute(s), of the given file|folder
setxattr	- Set an extended-attribute to the given file|folder in hdfs
rmxattr		- Remove given extended-attribute of the given file|folder in hdfs

All subprograms have -h|--help call option
In all subprograms value for option: -n|--name - has to be an absolute path
--------------------------------------------------------------------------------------
ENV:
SILENT		- In case it non-zero: turns off output of library-subprogram messages to stdout; 
		  Messages still will be made by the subproc, but will be written to logfile only;
---------------------------------------------------------------------------------------
Logdile:	- ${LOG_FILE}
Config:		- ${CONF_FILE}"
}

grep_json() {
local v_output=""
v_output=$(cat "$1" | egrep -v "^[\>\<\*] .*") #in case curl was called with -v option
v_output=${v_output//\\/\\\\}
v_output=${v_output%%[[:control:]]}
printf -v "$2" %s "$v_output"
}

prefix_hdfspath() {
local v_path="$1"

if [ "${v_path}" == "/" ]
then
 echo -n ""
 return 0
fi

if [ "${#v_path}" -gt "0" ]
then
	if [ "${v_path:0:1}" != "/" ] 
	then
	        v_path="/"${v_path}
	fi
fi
echo -n "$v_path"
}

empty_args_notallowed() {
local v_args="$1"
local v_procname="$2"
if [ -z "$v_args" ]
then
	log_info "${v_procname} you should not launch this routine without any args"
	return 1
fi
return 0
}
#== HDSF ====================================================================================================================
setmod() {
local v_module="setmod"
local v_count v_cmd rc v_x v_y V_URL=""
local v_operation="?op=SETPERMISSION"
local v_hdfspath=""
local v_permission=""

log_info "${v_module} starting"
empty_args_notallowed "$1" "$v_module"
if [ "$?" -ne "0" ]
then
        setmod_usage; return 1
fi

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                setmod_usage; return 0
                ;;               
                "-n"|"--name")
                if [ -z "$2" ]
                then
                        log_info "${v_module} you have to specify value for -n|--name option;"
                        setmod_usage; return 1
                fi
                v_hdfspath="$2"
                shift 2
                ;;
                "-p"|"--permission")
                if [ -z "$2" ]
                then
                        log_info "${v_module} you have to specify value for -p|--permission option;"
                        setmod_usage; return 1
                fi
                v_permission="$2"
                shift 2
                ;;
		*)
                log_info "${v_module} ${1} is not an option;"
                setmod_usage; return 1
                ;;
        esac
done

if [ -z "$v_hdfspath" -o -z "$v_permission" ]
then
	log_info "${v_module} you have to set name and permission-mode"
	setmod_usage; return 1
fi

if [[ ! "$v_permission" =~ [0-9]{3,4} ]]
then
	log_info "${v_module} ${v_permission} is a wrong value for -p|--permission option;"
	setmod_usage; return 1
fi
if [ "$v_permission" -gt "1777" -o "$v_permission" -lt "0" ] 
then
	log_info "${v_module} ${v_permission} is a wrong value for -p|--permission option;"
	setmod_usage; return 1
fi

v_hdfspath=$(prefix_hdfspath "$v_hdfspath")
v_operation="${v_operation}&permission=${v_permission}"

V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
V_URL=${V_URL%$'\r'}
v_cmd="${CURL} -X PUT ${GENERIC_OPTION} -u \"${V_LOGIN}:${V_PWD}\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>/dev/null"
log_info "$v_module ${v_cmd}"

v_count="1"
while [ "$v_count" -lt "$RETRY_LIMIT" ]
do
        cat /dev/null > "$TEMP_FILE"
        eval "$v_cmd"
        rc="$?"
        if [ "$rc" -eq "0" ]
        then
                log_info "${v_module} hdfs-response was obtained successfully"
                log_info "$TEMP_FILE" "logfile"
                v_x=""
                grep_json "$TEMP_FILE" v_x
                if [ -z "$v_x" ]
                then
                        return 0
                else
                        v_x=$(parse_error_response "$v_x"); v_y="$?"
                        echo "$v_x"
                        return "$v_y"
                fi
        else
                log_info "${v_module} attempt ${v_count} fail ${rc}"
        fi
        ((v_count++))
done
log_info "${v_module} done"
return "$?"
}

renamefile() {
local v_module="renamefile"
local v_count v_cmd rc v_x v_y V_URL=""
local v_operation="?op=RENAME"
local v_hdfspath=""
local v_newname=""

log_info "${v_module} starting"
empty_args_notallowed "$1" "$v_module"
if [ "$?" -ne "0" ]
then
        renamefile_usage; return 1
fi

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                renamefile_usage; return 0
                ;;
                "-n"|"--name")
                if [ -z "$2" ]
                then
                        log_info "${v_module} you have to specify value for -n|--name option;"
                        appendtofile_usage; return 1
                fi
                v_hdfspath="$2"
                shift 2
                ;;
                "-d"|"--dest")
                if [ -z "$2" ]
                then
                        log_info "${v_module} you have to specify value for -d|--dest option;"
                        appendtofile_usage; return 1
                fi
                v_newname="$2"
                shift 2
                ;;
                *)
                log_info "${v_module} ${1} is not an option;"
                renamefile_usage; return 1
                ;;
        esac
done

if [ -z "$v_newname" -o -z "$v_hdfspath" ]
then
	log_info "${v_module} current name and|or new-name is|are not set"
	renamefile_usage; return 1
fi

v_hdfspath=$(prefix_hdfspath "$v_hdfspath")
v_newname=$(prefix_hdfspath "$v_newname")
v_newname="/${WH_PATH}"${v_newname}
v_operation="${v_operation}&destination=${v_newname}"

V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
V_URL=${V_URL%$'\r'}
v_cmd="${CURL} -X PUT ${GENERIC_OPTION} -u \"${V_LOGIN}:${V_PWD}\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>/dev/null"
log_info "$v_module ${v_cmd}"

v_count="1"
while [ "$v_count" -lt "$RETRY_LIMIT" ]
do
        cat /dev/null > "$TEMP_FILE"
        eval "$v_cmd"
        rc="$?"
        if [ "$rc" -eq "0" ]
        then
                log_info "${v_module} hdfs-response was obtained successfully"
                log_info "$TEMP_FILE" "logfile"
                v_x=""
                grep_json "$TEMP_FILE" v_x
                if [ -z "$v_x" ]
                then
                        return 0
                else
                        v_x=$(parse_boolean_response "$v_x"); v_y="$?"
                        echo "$v_x"
                        return "$v_y"
                fi
        else
                log_info "${v_module} attempt ${v_count} fail ${rc}"
        fi
        ((v_count++))
done
log_info "${v_module} done"
return "$?"

}

appendtofile() {
local v_module="appendtofile"
local v_count v_cmd rc v_x v_y V_URL=""
local v_operation="?op=APPEND"
local v_hdfspath=""
local v_buffersize=""
local v_noredirect="false"
local v_localfile=""
local v_location=""
local v_maxtime="10"

log_info "${v_module} starting"
empty_args_notallowed "$1" "$v_module"
if [ "$?" -ne "0" ]
then
        appendtofile_usage; return 1
fi

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                appendtofile_usage; return 0
                ;;
                "-m"|"--maxtime")
                if [ -z "$2" ]
                then
                        log_info "${v_module} you have to specify value for -m|--maxtime option;"
                        appendtofile_usage; return 1
                fi
                if [[ ! "$2" =~ [0-9]+ ]]
                then
                        log_info "${v_module} value for -m|--maxtime option should be int-digit;"
                        appendtofile_usage; return 1
                fi
                v_maxtime="$2"
                if [ "$v_maxtime" -lt "10" ]
                then
                        log_info "${v_module} value for -m|--maxtime option is too small: ${v_maxtime};"
                        log_info "${v_module} This value: ignored, default value '10' will be used;"
                        v_maxtime="10"
                fi
                shift 2
                ;;
                "-n"|"--name")
                if [ -z "$2" ]
                then
                        log_info "${v_module} you have to specify value for -n|--name option;"
                        appendtofile_usage; return 1
                fi
                v_hdfspath="$2"
                shift 2
                ;;
                "-l"|"--local")
                if [ -z "$2" ]
                then
                        log_info "${v_module} you have to specify value for -l|--local option;"
                        appendtofile_usage; return 1
                fi
                if [ ! -f "$2" -a -r "$2" ]
                then
                        log_info "${v_module} object, which setted in -l|--local option: ${2} is not a file and|or is not readable;"
                        return 2
                fi
                v_localfile="$2"
                shift 2
                ;;
	
                *)
                log_info "${v_module} ${1} is not an option;"
                appendtofile_usage; return 1
                ;;
        esac
done

if [ -z "$v_localfile" -o -z "$v_hdfspath" ]
then
        log_info "${v_module} you have to set hdfs-file to write-append to and local-file to read from"
        appendtofile_usage; return 1
fi

v_hdfspath=$(prefix_hdfspath "$v_hdfspath")
[ ! -z "$v_buffersize" ] && v_operation="${v_operation}&buffersize=${v_buffersize}"
[ ! -z "$v_noredirect" ] && v_operation="${v_operation}&noredirect=${v_noredirect}"

V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
V_URL=${V_URL%$'\r'}
#### -v option and 2>&1 is essential here ###
v_cmd="${CURL} -X POST -v ${GENERIC_OPTION} -u \"${V_LOGIN}:${V_PWD}\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>&1"
#############################################
log_info "$v_module ${v_cmd}"

v_count="1"
while [ "$v_count" -lt "$RETRY_LIMIT" ]
do
        cat /dev/null > "$TEMP_FILE"
        eval "$v_cmd"
        rc="$?"
        if [ "$rc" -eq "0" ]
        then
                v_location=`cat $TEMP_FILE | egrep "Location: " | awk '{printf "%s", $NF;}'`
		v_location=${v_location%$'\r'}
                v_count="$RETRY_LIMIT"
                log_info "${v_module} success, locaton is: ${v_location}"
        else
                #sleep 1
                log_info "$v_module attempt ${v_count} fail ${rc}"
        fi
        ((v_count++))
done

if [ -z "$v_location" ]
then
        log_info "${v_module} well, so sorry but inode-location for your file was not provided by hdfs; exiting"
        return 1
else
        v_cmd="${CURL} -X POST -f ${GENERIC_OPTION} -u \"${V_LOGIN}:${V_PWD}\" -T ${v_localfile} --connect-timeout ${CONNECT_TIMEOUT} --max-time ${v_maxtime} ${v_location}"
        log_info "$v_module try to append date from ${v_localfile} to ${v_hdfspath}; Max-time limit is: ${v_maxtime}"
        log_info "$v_module ${v_cmd}"
        v_count="1"
        while [ "$v_count" -lt "$RETRY_LIMIT" ]
        do
                eval "$v_cmd"
                rc="$?"
                if [ "$rc" -eq "0" ]
                then
                        log_info "${v_module} success"
                        return 0
                else
                        log_info "${v_module} attempt ${v_count} fail ${rc}"
                fi
                ((v_count++))
        done
        return "$rc"
fi

}

getfile() {
local v_module="getfile"
local v_count v_cmd rc v_x v_y V_URL=""
local v_operation="?op=OPEN"
local v_hdfspath=""
local v_offset=""
local v_length=""
local v_buffersize=""
local v_noredirect="false"
local v_localfile=""
local v_location=""
local v_maxtime="10"

log_info "${v_module} starting"
empty_args_notallowed "$1" "$v_module"
if [ "$?" -ne "0" ]
then
        getfile_usage; return 1
fi

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                getfile_usage; return 0
                ;;
                "-m"|"--maxtime")
                if [ -z "$2" ]
                then
                        log_info "${v_module} you have to specify value for -m|--maxtime option;"
                        getfile_usage; return 1
                fi
		if [[ ! "$2" =~ [0-9]+ ]]
		then 
			log_info "${v_module} value for -m|--maxtime option should be int-digit;"
			getfile_usage; return 1
		fi
		v_maxtime="$2"
		if [ "$v_maxtime" -lt "10" ]
		then
			log_info "${v_module} value for -m|--maxtime option is too small: ${v_maxtime};"
			log_info "${v_module} This value: ignored, default value '10' will be used;"
			v_maxtime="10"
		fi
                shift 2
                ;;
                "-n"|"--name")
                if [ -z "$2" ]
                then
                        log_info "${v_module} you have to specify value for -n|--name option;"
                        getfile_usage; return 1
                fi
                v_hdfspath="$2"
                shift 2
                ;;
                "-l"|"--local")
                if [ -z "$2" ]
                then
                        log_info "${v_module} you have to specify value for -l|--local option;"
                        getfile_usage; return 1
                fi
		if [ ! -f "$2" -a -w "$2" ]
		then
			log_info "${v_module} object, which setted in -l|--local option: ${2} is not a file and|or is not writable;"
			return 2
		fi
                v_localfile="$2"
                shift 2
                ;;
	        *)
	        log_info "${v_module} ${1} is not an option;"
	        getfile_usage; return 1
	        ;;
        esac
done

if [ -z "$v_localfile" -o -z "$v_hdfspath" ]
then
	log_info "${v_module} you have to set hdfs-file to read from and local-file to save read data"
	getfile_usage; return 1
fi

v_hdfspath=$(prefix_hdfspath "$v_hdfspath")
[ ! -z "$v_offset" ] && v_operation="${v_operation}&offset=${v_offset}"
[ ! -z "$v_length" ] && v_operation="${v_operation}&length=${v_length}"
[ ! -z "$v_buffersize" ] && v_operation="${v_operation}&buffersize=${v_buffersize}"
[ ! -z "$v_noredirect" ] && v_operation="${v_operation}&noredirect=${v_noredirect}"

V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
V_URL=${V_URL%$'\r'}
#### -v option and 2>&1 is essential here ###
v_cmd="${CURL} -X GET -v ${GENERIC_OPTION} -u \"${V_LOGIN}:${V_PWD}\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>&1"
#############################################
log_info "$v_module ${v_cmd}"

v_count="1"
while [ "$v_count" -lt "$RETRY_LIMIT" ]
do
	cat /dev/null > "$TEMP_FILE"
        eval "$v_cmd"
        rc="$?"
        if [ "$rc" -eq "0" ]
        then
                v_location=`cat $TEMP_FILE | egrep "Location: " | awk '{printf "%s", $NF;}'`
		v_location=${v_location%$'\r'}
                v_count="$RETRY_LIMIT"
                log_info "${v_module} success, locaton is: ${v_location}"
        else
                #sleep 1
                log_info "$v_module attempt ${v_count} fail ${rc}"
        fi
        ((v_count++))
done
if [ -z "$v_location" ]
then
        log_info "${v_module} well, so sorry but inode-location for your file was not provided by hdfs; exiting"
        return 1
else
	cat /dev/null > "$v_localfile"
        v_cmd="${CURL} -X GET -f ${GENERIC_OPTION} -u \"${V_LOGIN}:${V_PWD}\" -o ${v_localfile} --connect-timeout ${CONNECT_TIMEOUT} --max-time ${v_maxtime} ${v_location}"
        log_info "$v_module try to download ${v_hdfspath} as ${v_localfile}; Max-time limit is: ${v_maxtime}"
        log_info "$v_module ${v_cmd}"
        v_count="1"
        while [ "$v_count" -lt "$RETRY_LIMIT" ]
        do
                eval "$v_cmd"
                rc="$?"
                if [ "$rc" -eq "0" ]
                then
                        log_info "${v_module} success"
                        return 0
                else
                        log_info "${v_module} attempt ${v_count} fail ${rc}"
                fi
                ((v_count++))
        done
        return "$rc"
fi
}

rmxattr() {
local v_module="rmxattr"
local v_count v_cmd rc v_x v_y V_URL=""
local v_operation="?op=REMOVEXATTR"
local v_hdfspath=""
local v_xattrname=""

log_info "${v_module} starting"
empty_args_notallowed "$1" "$v_module"
if [ "$?" -ne "0" ]
then
        rmxattr_usage; return 1
fi

while [ "$1" != "" ]
do
        case "$1" in
		"-h"|"--help")
		rmxattr_usage; return 0
		;;
		"-n"|"--name")
		if [ -z "$2" ]
		then
		        log_info "${v_module} you have to specify file or folder name in -n|--name option;"
		        rmxattr_usage; return 1
		fi
		v_hdfspath="$2"
		shift 2
		;;
		"-a"|"--attribute")
		if [ -z "$2" ]
		then
		        log_info "${v_module} you have to specify attribute name in -a|--attribute option;"
		        rmxattr_usage; return 1
		fi
		v_xattrname="$2"
		shift 2
		;;
	        *)
	        log_info "${v_module} ${1} is not an option;"
	        setxattr_usage; return 1
	        ;;
	esac
done

if [ -z "$v_xattrname" ]
then
        log_info "${v_module} name of extended-attribute should not be empty"
        setxattr_usage; return 1
fi
v_hdfspath=$(prefix_hdfspath "$v_hdfspath")
v_operation="${v_operation}&xattr.name=${v_xattrname}"
V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
V_URL=${V_URL%$'\r'}
v_cmd="${CURL} -X PUT ${GENERIC_OPTION} -u \"${V_LOGIN}:${V_PWD}\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>/dev/null"
log_info "$v_module ${v_cmd}"

v_count="1"
while [ "$v_count" -lt "$RETRY_LIMIT" ]
do
        cat /dev/null > "$TEMP_FILE"
        eval "$v_cmd"
        rc="$?"
        if [ "$rc" -eq "0" ]
        then
                log_info "${v_module} hdfs-response was obtained successfully"
                log_info "$TEMP_FILE" "logfile"
		v_x=""
                grep_json "$TEMP_FILE" v_x
                if [ -z "$v_x" ]
                then
                        return 0
                else
                        v_x=$(parse_error_response "$v_x"); v_y="$?"
                        echo "$v_x"
                        return "$v_y"
                fi
        else
                log_info "${v_module} attempt ${v_count} fail ${rc}"
        fi
        ((v_count++))
done
log_info "${v_module} done"
return "$rc"
}

setxattr() {
local v_module="setxattrs"
local v_count v_cmd rc v_x v_y V_URL=""
local v_operation="?op=SETXATTR"
local v_hdfspath=""
local v_xattrname=""
local v_xattrval=""
local v_flag="CREATE" # https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#XAttr_set_flag
local v_base64=""

log_info "${v_module} starting"
empty_args_notallowed "$1" "$v_module"
if [ "$?" -ne "0" ]
then
	setxattr_usage; return 1
fi

while [ "$1" != "" ]
do
        case "$1" in
	        "-h"|"--help")
	        setxattr_usage; return 0
	        ;;
	        "-n"|"--name")
	        if [ -z "$2" ]
	        then
	                log_info "${v_module} you have to specify file or folder name in -n|--name option;"
	                setxattr_usage; return 1
	        fi
	        v_hdfspath="$2"
	        shift 2
	        ;;
	        "-a"|"--attribute")
	        if [ -z "$2" ]
	        then
	                log_info "${v_module} you have to specify attribute name in -a|--attribute option;"
	                setxattr_usage; return 1
	        fi
	        v_xattrname="$2"
	        shift 2
	        ;;
		"-v"|"--value")
		v_xattrval="$2"	
		shift 2
		;;
		"-f"|"--flag")
		v_flag=$(echo -n "$2" | tr [:lower:] [:upper:])
		if [[ ! "$v_flag" =~ (CREATE|REPLACE) ]]
		then
			log_info "${v_module} wrong value for -f|--flag option;"
			setxattr_usage; return 1
		fi
		shift 2
		;;
		"-b"|"--base64")
		v_base64="1"
		shift 1
		;;
		*)
		log_info "${v_module} ${1} is not an option;"
		setxattr_usage; return 1
		;;
	esac
done

if [ -z "$v_xattrname" ]
then
	log_info "${v_module} name of extended-attribute should not be empty"
	setxattr_usage; return 1
fi

if [ ! -z "$v_base64" ]
then
	[ ! -z "$v_xattrval" ] && v_xattrval=$(echo -n "$v_xattrval" | base64)
fi

v_hdfspath=$(prefix_hdfspath "$v_hdfspath")
v_operation="${v_operation}&xattr.name=${v_xattrname}"
v_operation="${v_operation}&xattr.value=${v_xattrval}"
v_operation="${v_operation}&flag=${v_flag}"
V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
V_URL=${V_URL%$'\r'}
v_cmd="${CURL} -X PUT ${GENERIC_OPTION} -u \"${V_LOGIN}:${V_PWD}\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>/dev/null"
log_info "$v_module ${v_cmd}"
v_count="1"
while [ "$v_count" -lt "$RETRY_LIMIT" ]
do
        cat /dev/null > "$TEMP_FILE"
        eval "$v_cmd"
        rc="$?"
        if [ "$rc" -eq "0" ]
        then
                log_info "${v_module} hdfs-response was obtained successfully"
                log_info "$TEMP_FILE" "logfile"
		v_x=""
                grep_json "$TEMP_FILE" v_x
		if [ -z "$v_x" ]
		then
			return 0
		else
			v_x=$(parse_error_response "$v_x"); v_y="$?"
                        echo "$v_x"
			return "$v_y"
                fi
        else
                log_info "${v_module} attempt ${v_count} fail ${rc}"
        fi
        ((v_count++))
done
log_info "${v_module} done"
return "$rc"
}

getxattr() {
local v_module="getxattrs"
local v_count v_cmd rc v_x v_y V_URL=""
local v_operation="?op=GETXATTRS"
local v_hdfspath=""
local v_encoding="text"
local v_xattrname=""
local v_decodefrom=""

log_info "${v_module} starting"
while [ "$1" != "" ]
do
	case "$1" in
		"-h"|"--help")
		getxattr_usage; return 0
		;;
		"-b"|"--base64")
		v_decodefrom="base64"
		shift 1
		;;
		"-n"|"--name")
		if [ -z "$2" ]
		then
			log_info "${v_module} you have to specify file or folder name in -n|--name option;"
			getxattr_usage; return 1
		fi
		v_hdfspath="$2"
		shift 2
		;;
		"-a"|"--attribute")
		if [ -z "$2" ]
		then
			log_info "${v_module} you have to specify attribute name in -a|--attribute option;"
			getxattr_usage; return 1
		fi
		v_xattrname="$2"
		shift 2
		;;
		"-e"|"--encoding")
		v_encoding=$(echo -n "$2" | tr [:upper:] [:lower:])
		if [[ ! "$v_encoding" =~ (text|hex|base64) ]]
		then
			log_info "${v_module} you have to specify text|hex|base64 name in -e|--encoding option;"
			getxattr_usage; return 1
		fi
		shift 2
		;;
		*)
		log_info "${v_module} ${1} is not an option;"
		getxattr_usage; return 1
		;;
	esac
done

v_hdfspath=$(prefix_hdfspath "$v_hdfspath")
#log_info "${v_module} v_hdfspath: ${v_hdfspath}; v_xattrname: ${v_xattrname}; v_encoding: ${v_encoding}"
[ ! -z "$v_xattrname" ] && v_operation="$v_operation&xattr.name=${v_xattrname}"
v_operation="${v_operation}&encoding=${v_encoding}"
V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
V_URL=${V_URL%$'\r'}
v_cmd="${CURL} -X GET ${GENERIC_OPTION} -u \"${V_LOGIN}:${V_PWD}\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>/dev/null"
log_info "$v_module ${v_cmd}"

v_count="1"
while [ "$v_count" -lt "$RETRY_LIMIT" ]
do
        cat /dev/null > "$TEMP_FILE"
        eval "$v_cmd"
        rc="$?"
        if [ "$rc" -eq "0" ]
        then
                log_info "${v_module} hdfs-response was obtained successfully"
                log_info "$TEMP_FILE" "logfile"
		grep_json "$TEMP_FILE" v_x
                v_x=$(parse_getxattrs_response "$v_x"); v_y="$?"
		if [ "$v_y" -eq "0" ]
		then
			if [ -z "$v_decodefrom" ]
			then
		                echo "$v_x" | column -t
			elif [ "$v_decodefrom" == "base64" ]
			then
				v_count=$(echo "$v_x" | wc -l)
				if [ "$v_count" -eq "1" ]
				then
					v_key=$(echo "$v_x" | awk '{printf "%s", $1;}')	
					v_value=$(echo "$v_x" | awk '{printf "%s", $2;}')
					v_value=${v_value//\"/""}
					#log_info "${v_module} v_value: ${v_value}"
					v_value=$(echo "$v_value" | base64 -d 2>/dev/null); v_rc="$?"
					if [ "$v_rc" -eq "0" ]
					then
						echo "${v_key} ${v_value}" | column -t
					else
						log_info "${v_module} in key-value ${v_x} value can not ne decoded from base64"
						return "$v_rc"
					fi
				else
					log_info "${v_module} hdfs-answer contains several attribute stricture"
					log_info "${v_module} decoding from base64 is supposed to be used with only one attr"
					log_info "${v_module} so base64 decoding attr-values: will be ignored;"
					echo "$v_x" | column -t					
				fi
			else
				log_info "${v_module} unknown value in v_decodefrom: ${v_decodefrom}"
			fi
		else
			echo "$v_x"
		fi
                return "$v_y"
        else
                log_info "${v_module} attempt ${v_count} fail ${rc}"
        fi
        ((v_count++))
done
log_info "${v_module} done"
return "$rc"

}

delete() {
local v_module="delete"
local v_count v_cmd rc v_x v_y V_URL=""
local v_operation="?op=DELETE"
local v_hdfspath=""
local v_recursive="false"

empty_args_notallowed "$1" "$v_module"
if [ "$?" -ne "0" ]
then
	delete_usage; return 1
fi

log_info "${v_module} starting"
while [ "$1" != "" ]
do
        case "$1" in
		"-h"|"--help")
		delete_usage; return 0
		;;
		"-n"|"--name")
		if [ -z "$2" ]
	       	then
			log_info "${v_module} you have to specify file name in -n|--name option;"
			delete_usage; return 1
		fi
		v_hdfspath="$2"
		shift 2
		;;
		"-r"|"--recursive")
		if [ -z "$2" ]
		then
			v_recursive="true"
		fi
		shift 1
		;;
		*)
		log_info "${v_module} ${1} is not an option;"
		delete_usage; return 1
		;;
	esac
done

if [ -z "$v_hdfspath" ]
then
        log_info "${v_module} you have to specify file name in -n|--name option;";
        delete_usage; return 1
fi

v_hdfspath=$(prefix_hdfspath "$v_hdfspath")

v_operation="${v_operation}&recursive=${v_recursive}"
log_info "${v_module} ok, let's begin"
V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
V_URL=${V_URL%$'\r'}
v_cmd="${CURL} -X DELETE ${GENERIC_OPTION} -u \"${V_LOGIN}:${V_PWD}\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>/dev/null"
log_info "$v_module ${v_cmd}"

v_count="1"
while [ "$v_count" -lt "$RETRY_LIMIT" ]
do
	cat /dev/null > "$TEMP_FILE"
        eval "$v_cmd"
        rc="$?"
        if [ "$rc" -eq "0" ]
        then
		log_info "${v_module} hdfs-response was obtained successfully"
		log_info "$TEMP_FILE" "logfile"
		grep_json "$TEMP_FILE" v_x
		v_x=$(parse_boolean_response "$v_x"); v_y="$?"
		echo "$v_x"
		return "$v_y"
	else
		log_info "${v_module} attempt ${v_count} fail ${rc}"
	fi
	((v_count++))
done
log_info "${v_module} done"
return "$rc"
}

createfile() {
local v_module="createfile"
local v_count v_cmd rc v_location v_source V_URL=""
local v_operation="?op=CREATE"
local v_hdfspath=""
local v_overwrite=""
local v_permission=""
local v_maxtime="10"
local v_replication="2"

empty_args_notallowed "$1" "$v_module"
if [ "$?" -ne "0" ]
then
	create_file_usage; return 1
fi

log_info "${v_module} starting"
while [ "$1" != "" ]
do
	case "$1" in
		"-h"|"--help")
		create_file_usage; return 0
		;;
                "-m"|"--maxtime")
                if [ -z "$2" ]
                then
                        log_info "${v_module} you have to specify value for -m|--maxtime option;"
                        getfile_usage; return 1
                fi
                if [[ ! "$2" =~ [0-9]+ ]]
                then
                        log_info "${v_module} value for -m|--maxtime option should be int-digit;"
                        getfile_usage; return 1
                fi
                v_maxtime="$2"
                if [ "$v_maxtime" -lt "10" ]
                then
                        log_info "${v_module} value for -m|--maxtime option is too small: ${v_maxtime};"
                        log_info "${v_module} This value: ignored, default value '10' will be used;"
                        v_maxtime="10"
                fi
                shift 2
                ;;
		"-l"|"--local")
		if [ -z "$2" ]
		then
			log_info "${v_module} you have to specify file name in -s|--source option;"
			create_file_usage; return 1
		fi
		[ ! -f "$2" ] && {
			log_info "${v_module} ${2} is not a file, or does not exist;"
			create_file_usage; return 1
		}
		v_source="$2"
		shift 2
		;;
		"-n"|"--name")
		if [ -z "$2" ]
		then
			log_info "${v_module} you have to specify file name in -n|--name option;"
			create_file_usage; return 1
		fi
		v_hdfspath="$2"
		shift 2
		;;
		"-o"|"--overwrite")
		v_overwrite=$(echo -n "$2" | tr [:upper:] [:lower:])
		if [[ ! "$v_overwrite" =~ (true|false|t|f) ]]
		then
			log_info "${v_module} ${v_overwrite} is a wrong value for -o|--overwrite option;"
			create_file_usage; return 1
		fi
		[ "$v_overwrite" == "t" ] && v_overwrite="true"
		[ "$v_overwrite" == "f" ] && v_overwrite="false"
		shift 2
		;;
		"-p"|"--permission")
		if [[ ! "$2" =~ [0-9]{3,4} ]]
		then
			log_info "${v_module} ${2} is a wrong value for -p|--permission option;"
			create_file_usage; return 1
		fi
		[ "$2" -gt "1777" -o "$2" -lt "0" ] && {
			log_info "${v_module} ${2} is a wrong value for -p|--permission option;"
			create_file_usage; return 1
		}
		v_permission="$2"
		shift 2
		;;
                "-r"|"--replication")
                if [[ ! "$2" =~ [0-9]{1} ]]
                then
                        log_info "${v_module} ${2} is a wrong value for -r|--replication option;"
                        create_file_usage; return 1
                fi
                [ "$2" -gt "9" -o "$2" -lt "1" ] && {
                        log_info "${v_module} ${2} is a wrong value for -r|--replication option;"
                        create_file_usage; return 1
                }
                v_replication="$2"
                shift 2
                ;;
		*)
		log_info "${v_module} ${1} is not an option;"
		create_file_usage; return 1
		;;
	esac
done

if [ -z "$v_source" -o -z "$v_hdfspath" ]
then
	log_info "${v_module} you have to set both hdfs-name and name of local-file;"
	create_file_usage; return 1
fi

v_hdfspath=$(prefix_hdfspath "$v_hdfspath")

[ ! -z "$v_overwrite" ] && v_operation="${v_operation}&overwrite=${v_overwrite}"
[ ! -z "$v_permission" ] && v_operation="${v_operation}&permission=${v_permission}"
v_operation="${v_operation}&replication=${v_replication}"
V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
V_URL=${V_URL%$'\r'}
#### -v option and 2>&1 is essential here ###
v_cmd="${CURL} -X PUT -v ${GENERIC_OPTION} -u \"${V_LOGIN}:${V_PWD}\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>&1"
#############################################
log_info "$v_module ${v_cmd}"
v_count="1"
while [ "$v_count" -lt "$RETRY_LIMIT" ]
do
        eval "$v_cmd"
        rc="$?"
        if [ "$rc" -eq "0" ]
        then
		v_location=`cat $TEMP_FILE | egrep "Location: " | awk '{printf "%s", $NF;}'`
		v_location=${v_location%$'\r'}
                v_count="$RETRY_LIMIT"
                log_info "${v_module} success, locaton is: ${v_location}"
        else
		#sleep 1
                log_info "$v_module attempt ${v_count} fail ${rc}"
        fi
        ((v_count++))
done

if [ -z "$v_location" ]
then
	log_info "${v_module} well, so sorry but inode-location for your file was not provided by hdfs; exiting"
	return 1
else
	#v_cmd="${CURL} -T \"${v_source}\" -X PUT -i -f ${GENERIC_OPTION} -u \"${V_LOGIN}:${V_PWD}\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${v_maxtime} --trace-ascii \"${TEMP_FILE}\" ${v_location}"
	v_cmd="${CURL} -T \"${v_source}\" -X PUT -i -f ${GENERIC_OPTION} -u \"${V_LOGIN}:${V_PWD}\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${v_maxtime} ${v_location}"
	log_info "$v_module try to upload ${v_source} as ${v_hdfspath}; Max-time limit is: ${v_maxtime}"
	log_info "$v_module ${v_cmd}"
	v_count="1"
	while [ "$v_count" -lt "$RETRY_LIMIT" ]
	do
		cat /dev/null > "$TEMP_FILE"
		eval "$v_cmd"
		rc="$?"
		if [ "$rc" -eq "0" ]
		then
			log_info "${v_module} success"
			return 0
		else
			log_info "${v_module} attempt ${v_count} fail ${rc}"
		fi
		log_info "$TEMP_FILE" "logfile"
		((v_count++))
	done
	return "$rc"
fi
return 0
}

itemstatus() {
local v_module="itemstatus"
local v_count v_cmd rc v_x v_y V_URL=""
local v_operation="?op=GETFILESTATUS"
local v_hdfspath=""

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                item_status_usage; return 0
                ;;
                "-n"|"--name")
                if [ -z "$2" ]
                then
                        log_info "${v_module} you have to specify file or folder name in -n|--name option;"
                        item_status_usage; return 1
                fi
                v_hdfspath="$2"
                shift 2
                ;;
                *)
                log_info "${v_module} ${1} is not an option;"
                item_status_usage; return 1
                ;;
        esac
done

v_hdfspath=$(prefix_hdfspath "$v_hdfspath")

V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
V_URL=${V_URL%$'\r'}
v_cmd="${CURL} -X GET ${GENERIC_OPTION} -u \"$V_LOGIN:$V_PWD\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>/dev/null"
log_info "$v_module ${v_cmd}"
v_count="1"
while [ "$v_count" -lt "$RETRY_LIMIT" ]
do
	cat /dev/null > "$TEMP_FILE"
	eval "$v_cmd"
	rc="$?"
	if [ "$rc" -eq "0" ]
	then
		log_info "${v_module} some hdfs-response was obtained successfully"
		log_info "$TEMP_FILE" "logfile"
		grep_json "$TEMP_FILE" v_x
		v_x=$(parse_getfilestatus "$v_x"); v_y="$?"
		if [ "$v_y" -eq "0" ]
		then
			echo "$v_x" | column -t
		else
			echo "$v_x"
		fi
		return "$v_y"
	else
		log_info "$v_module attempt ${v_count} fail ${rc}"
	fi
	((v_count++))
done
log_info "$v_module done" 
return "$rc"
}

ls_dir() {
local v_module="ls_dir"
local v_count v_cmd rc v_x V_URL=""
local v_operation="?op=LISTSTATUS"
local v_hdfspath=""

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                ls_dir_usage; return 0
                ;;
                "-n"|"--name")
                if [ -z "$2" ]
                then
                        log_info "${v_module} you have to specify file or folder name in -n|--name option;"
                        ls_dir_usage; return 1
                fi
                v_hdfspath="$2"
                shift 2
                ;;
                *)
                log_info "${v_module} ${1} is not an option;"
                ls_dir_usage; return 1
                ;;
        esac
done
 
v_hdfspath=$(prefix_hdfspath "$v_hdfspath")

V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
V_URL=${V_URL%$'\r'}
#log_info "$v_module url: ${V_URL}"
v_cmd="${CURL} -X GET ${GENERIC_OPTION} -u \"$V_LOGIN:$V_PWD\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>/dev/null"
log_info "$v_module ${v_cmd}" 
v_count="1"
while [ "$v_count" -lt "$RETRY_LIMIT" ]
do
	cat /dev/null > "$TEMP_FILE"
	eval "$v_cmd"
	rc="$?"
	if [ "$rc" -eq "0" ]
	then
		log_info "${v_module} some hdfs-response was obtained successfully"
		log_info "$TEMP_FILE" "logfile"
		grep_json "$TEMP_FILE" v_x
		v_x=$(parse_liststatus "$v_x"); v_y="$?"
		if [ "$v_y" -eq "0" ]
		then
			echo "$v_x" | column -t
			return 0
		else
			echo "$v_x"
			return "$v_y"
		fi
	else
		log_info "$v_module attempt ${v_count} fail ${rc}"
	fi
	((v_count++))
done
log_info "$v_module done"
return "$rc"
}

mk_dir() {
local v_module="mk_dir"
local v_count v_cmd rc v_x v_y V_URL=""
local v_operation="?op=MKDIRS"
local v_permission=""
local v_hdfspath=""

empty_args_notallowed "$1" "$v_module"
if [ "$?" -ne "0" ]
then
	mk_dir_usage; return 1
fi

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                mk_dir_usage; return 0
                ;;
	        "-n"|"--name")
	        if [ -z "$2" ]
	        then
	                log_info "${v_module} you have to specify file or folder name in -n|--name option;"
	                mk_dir_usage; return 1
	        fi
	        v_hdfspath="$2"
	        shift 2
	        ;;
                "-p"|"--permission")
		v_permission="$2"
		if [[ ! "$v_permission" =~ [0-9]+ ]]
		then
			log_info "${v_module} permission have to be a digit, valid value: 0 - 1777"; 
			mk_dir_usage; return 1
		fi
                shift 2
                ;;
	        *)
	        log_info "${v_module} ${1} is not an option;"
	        mk_dir_usage; return 1
	        ;;
        esac
done

if [ -z "$v_hdfspath" ]
then
	log_info "${v_module} you have to specify file or folder name in -n|--name option;"
	mk_dir_usage; return 1
fi

v_hdfspath=$(prefix_hdfspath "$v_hdfspath")

[ ! -z "$v_permission" ] && v_operation="${v_operation}&permission=${v_permission}"
V_URL="https://${WEBHDFS_SERVER}:${WEBHDFS_PORT}/gateway/default/webhdfs/v1/${WH_PATH}${v_hdfspath}${v_operation}"
V_URL=${V_URL%$'\r'}
#log_info "$v_module url: ${V_URL}"
v_cmd="${CURL} -X PUT ${GENERIC_OPTION} -u \"$V_LOGIN:$V_PWD\" --connect-timeout ${CONNECT_TIMEOUT} --max-time ${MAX_TIME} \"$V_URL\" 1>\"$TEMP_FILE\" 2>/dev/null"
log_info "$v_module ${v_cmd}"
v_count="1"
while [ "$v_count" -lt "$RETRY_LIMIT" ]
do
	cat /dev/null > "$TEMP_FILE"
	eval "$v_cmd"
	rc="$?"
	if [ "$rc" -eq "0" ]
	then
		log_info "${v_module} some hdfs-response was obtained successfully"
		log_info "$TEMP_FILE" "logfile"
		grep_json "$TEMP_FILE" v_x
		v_x=$(parse_boolean_response "$v_x"); v_y="$?"
		echo "$v_x"
		return "$v_y"
	else
		log_info "$v_module attempt ${v_count} fail ${rc}"
	fi
	((v_count++))
done
log_info "$v_module done"
return "$rc"
}
#====================================================

