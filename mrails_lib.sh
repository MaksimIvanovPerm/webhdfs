#!/bin/bash
CONF_FILE="/home/oracle/webhdfs/mrails.conf"

if [ -f "$CONF_FILE" ]
then
	. "$CONF_FILE"
else
	echo "${CONF_FILE} does not exist or is not a file"
	return 1
fi

if [ -f "$WEBHDFS_LIB" ]
then
	. "$WEBHDFS_LIB"
else
	echo "${v_name} can not library ${WEBHDFS_LIB} error-exiting"
	return 1
fi

[ -z "$UPLOADTIME_LIMIT" ] && UPLOADTIME_LIMIT="120"
#================================================================
# Misc
usage() {
printf "%s\n" "
initdb			- Create tables in local sqlite-db, for datacollection metadata;
get_attribute		- Get attribute value of given datacollection;
upload_to_storage	- Upload data from csv-liked file to storage; Set of attributes, which are needed for processing 
					  of a given csv-file named here as a datacollection, or datasource attributes;
					  Techinaclly thouse ds-attributes is a bundle of rows in sqlite-tables;
					  See get_attribute, set_attribute: routines for managing attributes;
check_attributes	- Auxiliary routine, used in upload_to_storage as a checker of various attributes,
					  needed for processing of data from csv-liked file;
set_attribute		- Set attribute
upload_awrto_storage	- Upload awr-data to hdfs; 
check_awr_attributes	- Used in upload_awrto_storage as a checker of various attributes which're necessery for this process;
upload_atopto_storage	- Upload atop-data to hdfs;
check_atop_attributes	- Used in upload_atopto_storage as a checker of various attributes which're necessery for this process;
"
}
delete_aux_files() {
#deleting temporary files, see definitions in $CONF_FILE
[ -f "$SPOOL_FILE" ] && rm -f "$SPOOL_FILE"
[ -f "$TEMP_FILE" ] && rm -f "$TEMP_FILE"
[ -f "$HOME/.cookie_$$" ] && rm -f "$HOME/.cookie_$$"
}
#== Metric data level processing ===========================================================================================
get_attribute() {
local v_module="get_attribute"
local v_dcname=""
local v_dcid=""
local v_atname=""
local v_atvalue=""
local rc v_count v_x v_y v_fsize="0"

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                printf "%s\n" "
-h|--help       - This help
-d|--dcname     - Name of datacollection, which attribute(s) you want to get out; Mandatory;
-a|--atname     - Name of attribute of which value you want ot get; Optional;
                  If not set: all available pairs of attribute's name-value, of given dc, will be showed;"
                return 0
                ;;
                "-d"|"--dcname")
                if [ -z "$2" ]
                then
                        log_info "${LAYER_NAME}.${v_module} You have to set value for -d|--dcname option;"
                        log_info "${LAYER_NAME}.${v_module} Please use -h|--help to see reference"
                        return 1
                fi
                v_dcname="$2"
                shift 2
                ;;
                "-a"|"--atname")
                v_atname="$2"
                shift 2
                ;;
                *)
                log_info "${LAYER_NAME}.${v_module} ${1} is not an option; Please use -h|--help to see reference"; return 1
                ;;
        esac
done

if [ -z "$v_dcname" ]
then
        log_info "${LAYER_NAME}.${v_module} You have to set value for -d|--dcname option;"
        log_info "${LAYER_NAME}.${v_module} Please use -h|--help to see reference"
        return 1
fi

rc=$(echo "select count(*) from datasets where dataset_name='${v_dcname}';" | "$SQLITE" "$SQLITEDB" 2>/dev/null | tr -d [:cntrl:] | tr -d [:space:])

if [ "$rc" -ne "1" ]
then
        log_info "${LAYER_NAME}.${v_module} datacollection ${v_dcname} not registered"
        return 1
fi

v_dcid=$(echo "select id from datasets where dataset_name='${v_dcname}';" | "$SQLITE" "$SQLITEDB" 2>/dev/null | tr -d [:cntrl:] | tr -d [:space:])
if [ -z "$v_atname" ]
then
        "$SQLITE" "$SQLITEDB" << __EOF__ 2>/dev/null | column -s "|" -t
select attr_name, attr_value from datasets_attributes where ds_id=${v_dcid} order by attr_name;
select
.exit
__EOF__
else
        rc=$(echo "select count(*) from datasets_attributes where ds_id='${v_dcid}' and attr_name='${v_atname}';" | "$SQLITE" "$SQLITEDB" 2>/dev/null | tr -d [:cntrl:] | tr -d [:space:])
        if [ "$rc" -eq "1" ]
        then
                v_atvalue=$(echo "select attr_value from datasets_attributes where ds_id='${v_dcid}' and attr_name='${v_atname}';" | "$SQLITE" "$SQLITEDB" 2>/dev/null)
                echo -n "$v_atvalue"
                return 0
        elif [ "$rc" -gt "1" ]
        then
                log_info "${LAYER_NAME}.${v_module} attr ${v_atname} for dataset ${v_dcname} (id: ${v_dcid}) registered too many times: ${rc})"
                return 1
        else
                log_info "${LAYER_NAME}.${v_module} attr ${v_atname} for dataset ${v_dcname} (id: ${v_dcid}) not found"
                return 1
        fi
fi
return 0
}

set_attribute() {
local v_module="set_attribute"
local v_dcname=""
local v_atname=""
local v_atvalue=""
local v_dcid v_attrid rc

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                printf "%s\n" "
-h|--help       - This help
-d|--dcname     - Name of datacollection, which attribute(s) you want to get out; Mandatory;
-a|--atname     - Name of attribute of which value you want ot get; Mandatory;
-v|--value	- Value for attribute; Mandatory"
                return 0
                ;;
                "-d"|"--dcname")
                if [ -z "$2" ]
                then
                        log_info "${LAYER_NAME}.${v_module} You have to set value for -d|--dcname option;"
                        log_info "${LAYER_NAME}.${v_module} Please use -h|--help to see reference"
                        return 1
                fi
                v_dcname="$2"
                shift 2
                ;;
                "-a"|"--atname")
                if [ -z "$2" ]
                then
                        log_info "${LAYER_NAME}.${v_module} You have to set value for -a|--atname option;"
                        log_info "${LAYER_NAME}.${v_module} Please use -h|--help to see reference"
                        return 1
                fi
                v_atname="$2"
                shift 2
                ;;
                "-v"|"--value")
                if [ -z "$2" ]
                then
                        log_info "${LAYER_NAME}.${v_module} You have to set value for -v|--value option;"
                        log_info "${LAYER_NAME}.${v_module} Please use -h|--help to see reference"
                        return 1
                fi
                v_atvalue="$2"
                shift 2
		;;
                *)
                log_info "${LAYER_NAME}.${v_module} ${1} is not an option; Please use -h|--help to see reference"; return 1
                ;;
        esac
done

if [ -z "$v_atvalue" -o -z "$v_atname" -o -z "$v_dcname" ]
then
	log_info "${LAYER_NAME}.${v_module} You didn't set mandatory arguments for this routine;"
	log_info "${LAYER_NAME}.${v_module} Please use -h|--help to see reference"
	return 1
fi

v_dcid=$(echo "select id from datasets where dataset_name='${v_dcname}';" | "$SQLITE" "$SQLITEDB" 2>/dev/null | tr -d [:cntrl:] | tr -d [:space:])
if [ -z "$v_dcid" ]
then
	log_info "${LAYER_NAME}.${v_module} there is no data-collection with name '${v_dcname}'"
	return 1
else
	cat /dev/null > "$SPOOL_FILE"; v_attrid=""
	v_attrid=$(echo "select id from datasets_attributes where ds_id=${v_dcid} and attr_name='${v_atname}';" | "$SQLITE" "$SQLITEDB" 2>/dev/null | tr -d [:cntrl:] | tr -d [:space:])
	if [ -z "$v_attrid" ]
	then
		#log_info "${LAYER_NAME}.${v_module} insert into datasets_attributes(attr_name, attr_value, ds_id) values('${v_atname}', '${v_atvalue}', ${v_dcid});"
		echo "insert into datasets_attributes(attr_name, attr_value, ds_id) values('${v_atname}', '${v_atvalue}', ${v_dcid});" | "$SQLITE" "$SQLITEDB" 1>"$SPOOL_FILE" 2>&1
		rc="$?"
	else
		#log_info "${LAYER_NAME}.${v_module} update datasets_attributes set attr_value='${v_atvalue}' where ds_id=${v_dcid} and id=${v_attrid};"
		echo "update datasets_attributes set attr_value='${v_atvalue}' where ds_id=${v_dcid} and id=${v_attrid};" | "$SQLITE" "$SQLITEDB" 1>"$SPOOL_FILE" 2>&1
		rc="$?"
	fi
	if [ "$rc" -eq "0" ]
	then
		log_info "${LAYER_NAME}.${v_module} ok: ${v_atname}=${v_atvalue} setted for ${v_dcname}"
	else
		log_info "${LAYER_NAME}.${v_module} can not set ${v_atname}=${v_atvalue} for ${v_dcname}"
		log_info "$SPOOL_FILE" "logfile"
	fi
	return "$rc"
fi

return 0
}

check_awr_attributes() {
local v_module="check_awr_attributes"
local v_dcname=""
local rc v_x v_dcid v_y v_key v_value

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
		printf "%s\n" "-h|--help       - This help
-d|--dcname     - Name of datacollection, which attribute(s) you want to get out; Mandatory;
--------------------------------------------------------------------------------------------
Checks:
1	- Datacollection with name, given through -d|--dcname, is;
2	- Oracle directory: is
3	- Path, which set as oracle-directory's path: is, 
	  it's directory and it's writable 
	  and has enough free-space for awr-dump file;
"
		return 0
		;;
                "-d"|"--dcname")
                if [ -z "$2" ]
                then
                        log_info "${LAYER_NAME}.${v_module} You have to set value for -d|--dcname option;"
                        log_info "${LAYER_NAME}.${v_module} Please use -h|--help to see reference"
                        return 1
                fi
                v_dcname="$2"
                shift 2
                ;;
		*)
		log_info "${v_module} ${1} is not an option; use -h|--help;"
		return 1
        	;;
	esac
done

if [ -z "$v_dcname" ]
then
        log_info "${LAYER_NAME}.${v_module} you have to set value for -d|--dcname option; See -h|--help"; return 1
fi

v_dcid=$(echo "select id from datasets where dataset_name='${v_dcname}';" | "$SQLITE" "$SQLITEDB" 2>/dev/null | tr -d [:cntrl:] | tr -d [:space:])
if [ -z "$v_dcid" ]
then
        log_info "${LAYER_NAME}.${v_module} there is no data-collection with name '${v_dcname}'"
        return 1
fi

get_attribute -d "$v_dcname">"$SPOOL_FILE"
[ "$?" -ne "0" ] && {
        log_info "${LAYER_NAME}.${v_module} can not obtain set of attributes for data-collection with name: ${v_dcname};";
        return 1
}

declare -A v_attrset
while read line
do
        v_key=$(echo ${line} | awk '{printf "%s", $1}')
        v_value=$(echo ${line} | awk '{printf "%s", $2}')
        #printf "%s\t%s\n" "$v_key" "$v_value"
        v_attrset["$v_key"]="$v_value"
done<"$SPOOL_FILE"

v_x=${v_attrset["free_space"]}
if [ -z "$v_x" ]
then
	log_info "${LAYER_NAME}.${v_module} you should set value for free_space conf-parameter, in bytes"
	log_info "${LAYER_NAME}.${v_module}  it's requirement for amount of free space for awr-dump file"
	return 1
fi

v_x=${v_attrset["oradir"]}
if [ -z "$v_x" ]
then
	log_info "${LAYER_NAME}.${v_module} oracle-drectory object name, for dumping awr-data to file, is not definted;"
	return 1
fi

$ORACLE_HOME/bin/sqlplus -S / as sysdba << __EOFF__ 1>"$SPOOL_FILE" 2>/dev/null
set head off
set echo off
set newp none
set feedback off
set linesize 1024
whenever sqlerror exit failure
select DIRECTORY_PATH from sys.dba_directories where DIRECTORY_NAME='${v_x}';
exit
__EOFF__
rc="$?"

if [ "$rc" -ne "0" ]
then
	log_info "${LAYER_NAME}.${v_module} can not ask database about fs-path associated with oracle-dir: ${v_x}"
	return 1
fi

v_x=$(cat $SPOOL_FILE | tr -d [:cntrl:])
log_info "${LAYER_NAME}.${v_module} fs-path, assigned with ${v_attrset["oradir"]} is: ${v_x}"
if [ ! -d "$v_x" -a -w "$v_x" ]
then
	log_info "${LAYER_NAME}.${v_module} ${v_x} is not a directory and|or is not writable;"
	return 1
fi
v_y=$(df -P -B 1 "/opt/rias/spool/dba/awr/" | tail -n 1 | awk '{printf "%d", $4;}')
if [ "$v_y" -lt "${v_attrset["free_space"]}" ] 
then
	log_info "${LAYER_NAME}.${v_module} according to configuration-limit free_space (${v_attrset["free_space"]}): there isn't enought free space, in ${v_x} for making awr-dump file there"
	return 1
fi
return 0
#end of check_awr_attributes
}

check_attributes() {
local v_module="check_attributes"
local v_dcname=""
local rc v_x v_dcid v_key v_value

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                printf "%s\n" "
-h|--help       - This help
-d|--dcname     - Name of datacollection, which attribute(s) you want to get out; Mandatory;
--------------------------------------------------------------------------------------------
Checks:
1	- key-column number: is set and it's digit
2	- hdfs_file_sizelimit: is set and it's digit
3	- column_count: is set and it's a digit; 
	  actual number of column in datafile is equal to column_count value
4	- column_separator: is set
5	- datafile_path: is set and it refers to a readable file"
                return 0
                ;;
                "-d"|"--dcname")
                if [ -z "$2" ]
                then
                        log_info "${LAYER_NAME}.${v_module} You have to set value for -d|--dcname option;"
                        log_info "${LAYER_NAME}.${v_module} Please use -h|--help to see reference"
                        return 1
                fi
                v_dcname="$2"
                shift 2
                ;;
                *)
                log_info "${v_module} ${1} is not an option; use -h|--help"
                return 1
                ;;
        esac
done

if [ -z "$v_dcname" ]
then
        log_info "${LAYER_NAME}.${v_module} you have to set value for -d|--dcname option; See -h|--help"; return 1
fi

v_dcid=$(echo "select id from datasets where dataset_name='${v_dcname}';" | "$SQLITE" "$SQLITEDB" 2>/dev/null | tr -d [:cntrl:] | tr -d [:space:])
if [ -z "$v_dcid" ]
then
	log_info "${LAYER_NAME}.${v_module} there is no data-collection with name '${v_dcname}'"
        return 1
fi

get_attribute -d "$v_dcname">"$SPOOL_FILE"
[ "$?" -ne "0" ] && {
        log_info "${LAYER_NAME}.${v_module} can not obtain set of attributes for data-collection with name: ${v_dcname};"; 
	return 1
}

declare -A v_attrset
while read line
do
        v_key=$(echo ${line} | awk '{printf "%s", $1}')
        v_value=$(echo ${line} | awk '{printf "%s", $2}')
        #printf "%s\t%s\n" "$v_key" "$v_value"
        v_attrset["$v_key"]="$v_value"
done<"$SPOOL_FILE"

## get in into separate routine
if [ -z "${v_attrset["hdfs_filesize_limit"]}" ]
then
        log_info "${LAYER_NAME}.${v_module} hdfs_file_sizelimit attribute is undefined"
	return 1
fi
if [[ ! "${v_attrset["hdfs_filesize_limit"]}" =~ [0-9]+ ]]
then
        log_info "${LAYER_NAME}.${v_module} hdfs_file_sizelimit attribute is not a digit"
	return 1
fi

##if [ "${v_attrset["hdfs_file_sizelimit"]}" -lt "1048576" -o "${v_attrset["hdfs_file_sizelimit"]}" -gt "10737418240" ]
##then
##       log_info "${LAYER_NAME}.${v_module} value of hdfs_file_sizelimit attribute is too smaill, or too big"; return 1
##fi

if [ -z "${v_attrset["datafile_path"]}" ]
then
        log_info "${LAYER_NAME}.${v_module} attribute 'datafile_path' is not defined for data-collection ${v_dcname}"
	return 1
fi
if [ ! -f "${v_attrset["datafile_path"]}" -a -r "${v_attrset["datafile_path"]}" ]
then
        log_info "${LAYER_NAME}.${v_module} local datafile ${v_attrset["datafile_path"]} not exist and|or not readable"; 
	return 1
fi

##column_separator
if [ -z "${v_attrset["column_separator"]}" ]
then
	log_info "${LAYER_NAME}.${v_module} column_separator is not set"
	return 1
fi

##column count
if [ -z "${v_attrset["column_count"]}" ]
then
	log_info "${LAYER_NAME}.${v_module} column count is not set"
	return 1
fi

if [[ ! "${v_attrset["column_count"]}" =~ [0-9]+ ]]
then
	log_info "${LAYER_NAME}.${v_module} column count should be a digit"
	return 1
fi
v_x=$( head -n 10 ${v_attrset["datafile_path"]} | awk -F "${v_attrset["column_separator"]}" '{print NF;}' | sort -u | tr -d [:cntrl:] )
if [[ ! "$v_x" =~ [0-9]+ ]]
then
	log_info "${LAYER_NAME}.${v_module} can not find out number of column for ${v_attrset["datafile_path"]} with settet sep-char: ${v_attrset["column_separator"]}"
	return 1
fi

if [ "$v_x" -ne ${v_attrset["column_count"]} ]
then
	log_info "${LAYER_NAME}.${v_module} actual number of filed is: ${v_x}; It's not equal setted number of filed: ${v_attrset["column_count"]}"
	return 1
fi
## key-column is
if [ -z ${v_attrset["key_column_number"]} ]
then
	log_info "${LAYER_NAME}.${v_module} key-column for dataset-rows is not set"
	return 1
fi
if [[ ! "${v_attrset["key_column_number"]}" =~ [0-9]+ ]]
then
	log_info "${LAYER_NAME}.${v_module} value of key_column_number attribute is not a digit"
	return 1
fi

return 0
}

upload_awrto_storage() {
local v_module="upload_awrto_storage"
local v_dcname=""
local v_x v_y v_k v_z v_key v_value v_hdfsfileis v_path
local v_minsnapid v_maxsnapid v_last_snapid_inhdfs v_dbid v_hdfsdbid

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                printf "%s\n" "
-h|--help       - This help
-d|--dcname     - Name of data-collection, by which this program shuld have to obtain various necessary attribytes for data processing"
                return 0
                ;;
                "-d"|"--dcname")
                if [ -z "$2" ]
                then
                        log_info "${LAYER_NAME}.${v_module} you have to set value for -d|--dcname option; See -h|--help"; return 1
                fi
                v_dcname="$2"
                shift 2
                ;;
                *)
                log_info "${v_module} ${1} is not an option; Use -h|--help"
                return 1
                ;;
	esac
done

if [ -z "$v_dcname" ]
then
        log_info "${LAYER_NAME}.${v_module} you have to set value for -d|--dcname option; See -h|--help"; return 1
fi

log_info "${LAYER_NAME}.${v_module} ok, data-collection name is: ${v_dcname}"
check_awr_attributes -d "$v_dcname"
if [ "$?" -ne "0" ]
then
        log_info "${LAYER_NAME}.${v_module} something wrong with work with attributes and|or with attrs itself, of data-collection with name: ${v_dcname};"
        return 1
fi

get_attribute -d "$v_dcname">"$SPOOL_FILE"
declare -A v_attrset
while read line
do
        v_key=$(echo ${line} | awk '{printf "%s", $1}')
        v_value=$(echo ${line} | awk '{printf "%s", $2}')
        #printf "%s\t%s\n" "$v_key" "$v_value"
        v_attrset["$v_key"]="$v_value"
done<"$SPOOL_FILE"

#check: if hdfs-file exist; If file is then try to get max snap_id from it's xattr; If hdfs-file is but it isn't possible to obtain xattr from it: it's fail
cat /dev/null > "$SPOOL_FILE"
v_hdfsfileis="" #it means: no, hdfs-file - doesn't exist
v_y="${v_attrset["hdfs_flagfile"]}"
itemstatus -n "$v_y" 1>"$SPOOL_FILE" 2>&1
rc="$?"
if [ "$rc" -eq "0" ]
then
        #Ok we defenetly get info about hdfs-item from hdfs; Let's see: what it is
        v_x=$(cat "$SPOOL_FILE" | egrep "^type:" | awk '{printf "%s", $2;}' | tr [:lower:] [:upper:])
        if [ "$v_x" == "FILE" ]
        then
                log_info "${LAYER_NAME}.${v_module} hdfs-side available and file ${v_y} is there"
		v_hdfsfileis="y"
        else
                #Well, it's not a file; Directory, symlinc, something else but not a file; Well nothing else but erroro-ending
                log_info "${LAYER_NAME}.${v_module} hdfs-side is available but ${v_y} is not a file, it's: ${v_x};"
                return 1
        fi
else
        #well may be hdfs is not reachable; Or may be it said something about error; Let us see: what is it
        v_x=$(cat "$SPOOL_FILE" | wc -l)
        if [ "$v_x" -gt "0" ]
        then
                #cat "$SPOOL_FILE"
                cat "$SPOOL_FILE" | grep -q "FileNotFoundExceptionFile" 1>/dev/null 2>&1
                if [ "$?" -eq "0"  ] 
		then
			v_hdfsfileis="n" #So it means that hdfs is reachable and it said that asked file: is absent;
		else
			log_info "${LAYER_NAME}.${v_module} hdfs returned error on itemstatus req for: ${v_y}"
			log_info "$SPOOL_FILE" "logfile"
			return 1
		fi
        else
                log_info "${LAYER_NAME}.${v_module} hdfs is not reachable right now; error-exiting"; return 1
        fi
fi

v_last_snapid_inhdfs=""
v_hdfsdbid=""
if [ "$v_hdfsfileis" == "y" ]
then
	v_last_snapid_inhdfs=$(getxattr -n "${v_attrset["hdfs_flagfile"]}" -a "user.max_keyvalue" | egrep "^user.max_keyvalue.*" | awk '{printf "%s", $2;}')
	v_last_snapid_inhdfs=${v_last_snapid_inhdfs#\"}; v_last_snapid_inhdfs=${v_last_snapid_inhdfs%\"}
	log_info "${LAYER_NAME}.${v_module} last_snapid_inhdfs value obtained as: ${v_last_snapid_inhdfs}"
	if [ "$v_last_snapid_inhdfs" == "none" ]
	then
		log_info "${LAYER_NAME}.${v_module} v_last_snapid_inhdfs is none, ok set it to 0"
		v_last_snapid_inhdfs="0"
	fi
	if [[ ! "$v_last_snapid_inhdfs" =~ [0-9]+ ]]
	then
		log_info "${LAYER_NAME}.${v_module} and it isn't a digit"
		return 1
	fi
	v_hdfsdbid=$(getxattr -n "${v_attrset["hdfs_flagfile"]}" -a "user.dbid" | egrep "^user.dbid.*" | awk '{printf "%s", $2;}')
	v_hdfsdbid=${v_hdfsdbid#\"}; v_hdfsdbid=${v_hdfsdbid%\"}
	log_info "${LAYER_NAME}.${v_module} v_hdfsdbid obtained as: ${v_hdfsdbid}"
	if [ "$v_hdfsdbid" == "none" ]
	then
		log_info "${LAYER_NAME}.${v_module} v_hdfsdbid is none, ok set it to 0"
		v_hdfsdbid="0"
	fi
	if [[ ! "$v_hdfsdbid" =~ [0-9]+ ]]
	then
		log_info "${LAYER_NAME}.${v_module} v_hdfsdbid is not a digit"
		return 1
	fi
else
	log_info "${LAYER_NAME}.${v_module} hdfs flag-file ${v_attrset["hdfs_flagfile"]} doesn't exist, set v_last_snapid_inhdfs to 0"
	v_last_snapid_inhdfs="0"
	log_info "${LAYER_NAME}.${v_module} set v_hdfsdbid to 0"
	v_hdfsdbid="0"
fi

if [ "$v_hdfsfileis" == "y" -a -z "$v_last_snapid_inhdfs" ]
then
	log_info "${LAYER_NAME}.${v_module} can not obtain max awr-snap_id from the last awr-dump as hdfs-file"
	return 1
fi

v_dbid=""
cat /dev/null > "$SPOOL_FILE"
$ORACLE_HOME/bin/sqlplus -S / as sysdba << __EOF__ 1>"$SPOOL_FILE" 2>&1
set head off
set echo off
set newp none
set feedback off
set linesize 1024
whenever sqlerror exit failure
select dbid from v\$database;
exit
__EOF__
rc="$?"
if [ "$rc" -ne "0" ]
then
        log_info "${LAYER_NAME}.${v_module} can not obtain dbid from database;"
        log_info "$SPOOL_FILE" "logfile"
        return 1
fi
v_dbid=$(cat $SPOOL_FILE | awk '{printf "%d", $1;}' | tr -d [:cntrl:] | tr -d [:space:])
log_info "${LAYER_NAME}.${v_module} dbid of database is: ${v_dbid}"

# export awr-data to dumpfile; Probably from hdfs-found snap_id; save min|max snap_id of exported awr-data to variable v_minkey v_maxkey
cat /dev/null > "$SPOOL_FILE"
v_x=""

if [ "$v_dbid" -ne "$v_hdfsdbid" ]
then
	log_info "${LAYER_NAME}.${v_module} current dbid is not equal dbid obtained from hdfs: dbid to which most resent uploaded to he hdfs awr-data is related;"
	v_last_snapid_inhdfs="0"
fi
[ ! -z "$v_last_snapid_inhdfs" ] && v_x="AND s.snap_id>=${v_last_snapid_inhdfs}"
$ORACLE_HOME/bin/sqlplus -S / as sysdba << __EOF__ 1>"$SPOOL_FILE" 2>&1
set head off
set echo off
set newp none
set feedback off
set linesize 1024
whenever sqlerror exit failure
SELECT Min(s.snap_id)||' '||Max(s.snap_id) AS col
FROM sys.dba_hist_snapshot s
WHERE s.dbid=(select d.dbid from sys.v_\$database d) ${v_x}
  AND s.begin_interval_time>(SELECT i.startup_time FROM sys.v_\$instance i)
;
exit
__EOF__
rc="$?"

if [ "$rc" -ne "0" ]
then
	log_info "${LAYER_NAME}.${v_module} can not obtain min|max snap_id from datatase to awr-export"
	log_info "$SPOOL_FILE" "logfile"
	return 1
fi

v_minsnapid=$(cat $SPOOL_FILE | awk '{printf "%d", $1;}' | tr -d [:cntrl:])
v_maxsnapid=$(cat $SPOOL_FILE | awk '{printf "%d", $2;}' | tr -d [:cntrl:])
log_info "${LAYER_NAME}.${v_module} minsnapid: ${v_minsnapid}; maxsnapid: ${v_maxsnapid}"
if [[ ! "$v_minsnapid" =~ [0-9]+ ]] || [[ ! "$v_maxsnapid" =~ [0-9]+ ]]
then
	log_info "${LAYER_NAME}.${v_module} minsnapid and|or maxsnapid is not a digit"
	return 1
fi

v_x=${v_attrset["oradir"]}
$ORACLE_HOME/bin/sqlplus -S / as sysdba << __EOFF__ 1>"$SPOOL_FILE" 2>/dev/null
set head off
set echo off
set newp none
set feedback off
set linesize 1024
whenever sqlerror exit failure
select DIRECTORY_PATH from sys.dba_directories where DIRECTORY_NAME='${v_x}';
exit
__EOFF__
rc="$?"

if [ "$rc" -ne "0" ]
then
        log_info "${LAYER_NAME}.${v_module} can not ask database about fs-path associated with oracle-dir: ${v_x}"
        return 1
fi

v_path=$(cat $SPOOL_FILE | tr -d [:cntrl:])
find ${v_path} -type f -name "${v_attrset["dump_name"]}*" -delete
#v_x=${v_attrset["dump_name"]}"_"${v_minsnapid}"_"${v_maxsnapid}
v_x=${v_dbid}"_"${v_minsnapid}"_"${v_maxsnapid}
v_path=${v_path%\/}"/"${v_x}
v_path=${v_path}".dmp"

log_info "${LAYER_NAME}.${v_module} exporting awr-data to ${v_path}"
$ORACLE_HOME/bin/sqlplus -S "/ as sysdba" << __EOFF__ 1>$SPOOL_FILE 2>&1
set verify off
set newp none
whenever sqlerror exit failure
column v_dbid new_value v_dbid noprint;
select dbid as v_dbid from v\$database;
define dbid = "&&v_dbid"
define num_days = ""
define begin_snap = ${v_minsnapid}
define end_snap = ${v_maxsnapid}
define directory_name = "${v_attrset["oradir"]}"
define file_name      = "${v_x}"
define
@$ORACLE_HOME/rdbms/admin/awrextr.sql
exit;
__EOFF__
rc="$?"
if [ "$rc" -ne "0" ]
then
	log_info "${LAYER_NAME}.${v_module} can not export awr-data to file"
	return 1
fi
log_info "${LAYER_NAME}.${v_module} export-log: "
log_info "$SPOOL_FILE" "logfile"

if [ -f "$v_path" ]
then
	log_info "${LAYER_NAME}.${v_module} gzipping ${v_path}"
	gzip -6 "$v_path"
	v_path=${v_path}".gz"
else
	log_info "${LAYER_NAME}.${v_module} hmmm,.. awr-dump file supposed to be prepared as ${v_path}; but it isn't"
	return 1
fi

if [ "$v_hdfsfileis" == "n" ]
then
	log_info "${LAYER_NAME}.${v_module} try to create flag-file ${v_attrset["hdfs_flagfile"]}"
	cat /dev/null > $SPOOL_FILE
	createfile -n "${v_attrset["hdfs_flagfile"]}" -l "$SPOOL_FILE" -m 10 1>/dev/null 2>&1; rc="$?"
	if [ "$rc" -ne "0" ]
	then
		log_info "${LAYER_NAME}.${v_module} can not create flag file ${v_attrset["hdfs_flagfile"]}"
		v_path=$(dirname "$v_path"); find ${v_path} -type f -name "${v_attrset["dump_name"]}*" -delete
		return 1
	fi
	setxattr -n ${v_attrset["hdfs_flagfile"]} -a "user.min_keyvalue" -v "$v_minsnapid" -f "create"; v_z="$?"
	[ "$v_z" -ne "0" ] && {
        	log_info "${LAYER_NAME}.${v_module} can not set user.min_keyvalue attr for ${v_attrset["hdfs_flagfile"]}"
		v_path=$(dirname "$v_path"); find ${v_path} -type f -name "${v_attrset["dump_name"]}*" -delete
	        return "$v_z"
	}
	setxattr -n ${v_attrset["hdfs_flagfile"]} -a "user.max_keyvalue" -v "none" -f "create"; v_z="$?"	
	[ "$v_z" -ne "0" ] && {
		log_info "${LAYER_NAME}.${v_module} can not set user.max_keyvalue attr for ${v_attrset["hdfs_flagfile"]}"	
		v_path=$(dirname "$v_path"); find ${v_path} -type f -name "${v_attrset["dump_name"]}*" -delete
		return 1
	}
        setxattr -n ${v_attrset["hdfs_flagfile"]} -a "user.recent_filename" -v "none" -f "create"; v_z="$?"
        [ "$v_z" -ne "0" ] && {
                log_info "${LAYER_NAME}.${v_module} can not set user.max_keyvalue attr for ${v_attrset["hdfs_flagfile"]}"
                v_path=$(dirname "$v_path"); find ${v_path} -type f -name "${v_attrset["dump_name"]}*" -delete
                return 1
        }
	setxattr -n ${v_attrset["hdfs_flagfile"]} -a "user.dbid" -v "none" -f "create"; v_z="$?"
	[ "$v_z" -ne "0" ] && {
		log_info "${LAYER_NAME}.${v_module} can not set user.max_keyvalue attr for ${v_attrset["hdfs_flagfile"]}"
		v_path=$(dirname "$v_path"); find ${v_path} -type f -name "${v_attrset["dump_name"]}*" -delete
		return 1
	}
	log_info "${LAYER_NAME}.${v_module} flag-file ${v_attrset["hdfs_flagfile"]} created, it's xattrs setted"
else
	log_info "${LAYER_NAME}.${v_module} flag-file already is"
fi

#create new hdfs-file from the newly generated awr-dump file
log_info "${LAYER_NAME}.${v_module} ok let's upload local-file ${v_path} to hdfs-dir ${v_attrset["hdfs_folder"]}"
v_y=${v_attrset["hdfs_folder"]}
v_y=${v_y%\/}"/"$(basename "$v_path")
log_info "${LAYER_NAME}.${v_module} hdfs-file name is: ${v_y}"
createfile -n "$v_y" -l "${v_path}" -m "$UPLOADTIME_LIMIT" 1>/dev/null 2>&1; rc="$?"
if [ "$rc" -ne "0" ]
then
	log_info "${LAYER_NAME}.${v_module} can not create hdfs file"
	v_path=$(dirname "$v_path"); find ${v_path} -type f -name "${v_attrset["dump_name"]}*" -delete
	return 1	
fi
v_x="${v_attrset["hdfs_flagfile"]}"
setxattr -n "$v_x" -a "user.min_keyvalue" -v "$v_minsnapid" -f "replace"; v_z="$?"
[ "$v_z" -ne "0" ] && {
	log_info "${LAYER_NAME}.${v_module} can not set user.min_keyvalue to ${v_minsnapid} for ${v_x}"
	v_path=$(dirname "$v_path"); find ${v_path} -type f -name "${v_attrset["dump_name"]}*" -delete
	return "$v_z"
}
setxattr -n "$v_x" -a "user.max_keyvalue" -v "$v_maxsnapid" -f "replace"; v_z="$?"
[ "$v_z" -ne "0" ] && {
        log_info "${LAYER_NAME}.${v_module} can not set user.max_keyvalue to ${v_maxsnapid} for ${v_x}"
	v_path=$(dirname "$v_path"); find ${v_path} -type f -name "${v_attrset["dump_name"]}*" -delete
        return "$v_z"
}
setxattr -n "$v_x" -a "user.dbid" -v "$v_dbid" -f "replace"; v_z="$?"
[ "$v_z" -ne "0" ] && {
        log_info "${LAYER_NAME}.${v_module} can not set user.dbid to ${v_dbid} for ${v_x}"
        v_path=$(dirname "$v_path"); find ${v_path} -type f -name "${v_attrset["dump_name"]}*" -delete
        return "$v_z"
}

v_k=$(getxattr -n "${v_x}" -a "user.recent_filename" | egrep "^user.recent_filename.*" | awk '{printf "%s", $2;}')
v_k=${v_k#\"}; v_k=${v_k%\"}
setxattr -n "$v_k" -a "user.next_file" -v "$v_y" -f "create" 1>/dev/null 2>&1
setxattr -n "$v_y" -a "user.prev_file" -v "$v_k" -f "create" 1>/dev/null 2>&1

setxattr -n "$v_x" -a "user.recent_filename" -v "$v_y" -f "replace"; v_z="$?"
[ "$v_z" -ne "0" ] && {
        log_info "${LAYER_NAME}.${v_module} can not set user.max_keyvalue to ${v_maxsnapid} for ${v_x}"
        v_path=$(dirname "$v_path"); find ${v_path} -type f -name "${v_attrset["dump_name"]}*" -delete
        return "$v_z"
}

v_path=$(dirname "$v_path"); find ${v_path} -type f -name "${v_attrset["dump_name"]}*" -delete
return 0
#end of upload_awrto_storage
}

check_atop_attributes() {
local v_module="check_atop_attributes"
local v_dcname=""
local rc v_x v_dcid v_key v_value

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                printf "%s\n" "
-h|--help       - This help
-d|--dcname     - Name of datacollection, which attribute(s) you want to get out; Mandatory;
--------------------------------------------------------------------------------------------
Checks:"
                return 0
                ;;
                "-d"|"--dcname")
                if [ -z "$2" ]
                then
                        log_info "${LAYER_NAME}.${v_module} You have to set value for -d|--dcname option;"
                        log_info "${LAYER_NAME}.${v_module} Please use -h|--help to see reference"
                        return 1
                fi
                v_dcname="$2"
                shift 2
                ;;
                *)
                log_info "${v_module} ${1} is not an option; use -h|--help"
                return 1
                ;;
        esac
done

if [ -z "$v_dcname" ]
then
        log_info "${LAYER_NAME}.${v_module} you have to set value for -d|--dcname option; See -h|--help"; return 1
fi

v_dcid=$(echo "select id from datasets where dataset_name='${v_dcname}';" | "$SQLITE" "$SQLITEDB" 2>/dev/null | tr -d [:cntrl:] | tr -d [:space:])
if [ -z "$v_dcid" ]
then
        log_info "${LAYER_NAME}.${v_module} there is no data-collection with name '${v_dcname}'"
        return 1
fi

get_attribute -d "$v_dcname">"$SPOOL_FILE"
[ "$?" -ne "0" ] && {
        log_info "${LAYER_NAME}.${v_module} can not obtain set of attributes for data-collection with name: ${v_dcname};";
        return 1
}

declare -A v_attrset
while read line
do
        v_key=$(echo ${line} | awk '{printf "%s", $1}')
        v_value=$(echo ${line} | awk '{printf "%s", $2}')
        #printf "%s\t%s\n" "$v_key" "$v_value"
        v_attrset["$v_key"]="$v_value"
done<"$SPOOL_FILE"

if [ ! -d "${v_attrset["atoplog_dir"]}" ]
then
	log_info "${LAYER_NAME}.${v_module} by configuration ${v_attrset["atoplog_dir"]} should be a local-dir with atop-log;"
	log_info "${LAYER_NAME}.${v_module} But ${v_attrset["atoplog_dir"]} is not a dir and|or doesn't exist"
	return 1
fi
if [ ! -r "${v_attrset["atoplog_dir"]}" ]
then
	log_info "${LAYER_NAME}.${v_module} you don't have read-permission to directory ${v_attrset["atoplog_dir"]}, which is supposed to be atop-log dir"
	return 1
fi

return 0
#end of check_atop_attributes
}

upload_atopto_storage() {
local v_module="upload_atopto_storage"
local v_hdfsfileis v_x v_y v_key v_value v_path rc v_dcname=""

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                printf "%s\n" "
-h|--help       - This help
-d|--dcname     - Name of data-collection, by which this program shuld have to obtain various necessary attribytes for data processing"
                return 0
                ;;
                "-d"|"--dcname")
                if [ -z "$2" ]
                then
                        log_info "${LAYER_NAME}.${v_module} you have to set value for -d|--dcname option; See -h|--help"; return 1
                fi
                v_dcname="$2"
                shift 2
                ;;
                *)
                log_info "${v_module} ${1} is not an option; Use -h|--help;"
                return 1
                ;;
        esac
done

if [ -z "$v_dcname" ]
then
        log_info "${LAYER_NAME}.${v_module} you have to set value for -d|--dcname option; See -h|--help"; return 1
fi

log_info "${LAYER_NAME}.${v_module} ok, data-collection name is: ${v_dcname}"
check_atop_attributes -d "$v_dcname"
if [ "$?" -ne "0" ]
then
        log_info "${LAYER_NAME}.${v_module} something wrong with work with attributes and|or with attrs itself, of data-collection with name: ${v_dcname};"
        return 1
fi

get_attribute -d "$v_dcname">"$SPOOL_FILE"
declare -A v_attrset
while read line
do
        v_key=$(echo ${line} | awk '{printf "%s", $1}')
        v_value=$(echo ${line} | awk '{printf "%s", $2}')
        #printf "%s\t%s\n" "$v_key" "$v_value"
        v_attrset["$v_key"]="$v_value"
done<"$SPOOL_FILE"

#check: if hdfs-file exist; If file is then try to get max snap_id from it's xattr; If hdfs-file is but it isn't possible to obtain xattr from it: it's fail
cat /dev/null > "$SPOOL_FILE"
v_hdfsfileis="" #it means: no, hdfs-file - doesn't exist
v_y="${v_attrset["hdfs_flagfile"]}"
itemstatus -n "$v_y" 1>"$SPOOL_FILE" 2>&1
rc="$?"
if [ "$rc" -eq "0" ]
then
        #Ok we defenetly get info about hdfs-item from hdfs; Let's see: what it is
        v_x=$(cat "$SPOOL_FILE" | egrep "^type:" | awk '{printf "%s", $2;}' | tr [:lower:] [:upper:])
        if [ "$v_x" == "FILE" ]
        then
                log_info "${LAYER_NAME}.${v_module} hdfs-side available and file ${v_y} is there"
                v_hdfsfileis="y"
        else
                #Well, it's not a file; Directory, symlinc, something else but not a file; Well nothing else but erroro-ending
                log_info "${LAYER_NAME}.${v_module} hdfs-side is available but ${v_y} is not a file, it's: ${v_x};"
                return 1
        fi
else
        #well may be hdfs is not reachable; Or may be it said something about error; Let us see: what is it
        v_x=$(cat "$SPOOL_FILE" | wc -l)
        if [ "$v_x" -gt "0" ]
        then
                #cat "$SPOOL_FILE"
                cat "$SPOOL_FILE" | grep -q "FileNotFoundExceptionFile" 1>/dev/null 2>&1
                if [ "$?" -eq "0"  ]
                then
                        v_hdfsfileis="n" #So it means that hdfs is reachable and it said that asked file: is absent;
                else
                        log_info "${LAYER_NAME}.${v_module} hdfs returned error on itemstatus req for: ${v_y}"
                        log_info "$SPOOL_FILE" "logfile"
                        return 1
                fi
        else
                log_info "${LAYER_NAME}.${v_module} hdfs is not reachable right now; error-exiting"; return 1
        fi
fi

v_last_snapid_inhdfs=""
if [ "$v_hdfsfileis" == "y" ]
then
        v_last_snapid_inhdfs=$(getxattr -n "${v_attrset["hdfs_flagfile"]}" -a "user.max_keyvalue" | egrep "^user.max_keyvalue.*" | awk '{printf "%s", $2;}')
        v_last_snapid_inhdfs=${v_last_snapid_inhdfs#\"}; v_last_snapid_inhdfs=${v_last_snapid_inhdfs%\"}
        log_info "${LAYER_NAME}.${v_module} last_snapid_inhdfs value obtained as: ${v_last_snapid_inhdfs}"
        if [ "$v_last_snapid_inhdfs" == "none" ]
        then
                log_info "${LAYER_NAME}.${v_module} v_last_snapid_inhdfs is none, ok set it to 0"
                v_last_snapid_inhdfs="0"
        fi
        if [[ ! "$v_last_snapid_inhdfs" =~ [0-9]+ ]]
        then
                log_info "${LAYER_NAME}.${v_module} and it isn't a digit"
                return 1
        fi
else
	log_info "${LAYER_NAME}.${v_module} hdfs flag-file ${v_attrset["hdfs_flagfile"]} doesn't exist; set last timestamp to 0"
	v_last_snapid_inhdfs="0"
fi

if [ "$v_hdfsfileis" == "y" -a -z "$v_last_snapid_inhdfs" ]
then
        log_info "${LAYER_NAME}.${v_module} can not obtain max awr-snap_id from the last awr-dump as hdfs-file"
        return 1
fi

if [ "$v_hdfsfileis" == "n" ]
then
        log_info "${LAYER_NAME}.${v_module} try to create flag-file ${v_attrset["hdfs_flagfile"]}"
        cat /dev/null > $SPOOL_FILE
        createfile -n "${v_attrset["hdfs_flagfile"]}" -l "$SPOOL_FILE" -m 10 1>/dev/null 2>&1; rc="$?"
        if [ "$rc" -ne "0" ]
        then
                log_info "${LAYER_NAME}.${v_module} can not create flag file ${v_attrset["hdfs_flagfile"]}"
                return 1
        fi
        setxattr -n ${v_attrset["hdfs_flagfile"]} -a "user.max_keyvalue" -v "none" -f "create"; v_z="$?"
        [ "$v_z" -ne "0" ] && {
                log_info "${LAYER_NAME}.${v_module} can not set user.max_keyvalue attr for ${v_attrset["hdfs_flagfile"]}"
                return 1
        }
        log_info "${LAYER_NAME}.${v_module} flag-file ${v_attrset["hdfs_flagfile"]} created, it's xattrs setted"
else
        log_info "${LAYER_NAME}.${v_module} flag-file already is"
fi

log_info "${LAYER_NAME}.${v_module} try to find atop-log in ${v_attrset["atoplog_dir"]} with name ${v_attrset["atoplog_name"]} which contain data with timstamp more modern that ${v_last_snapid_inhdfs}"
cat /dev/null > "$SPOOL_FILE"
v_path=${v_attrset["hdfs_folder"]}
v_path=${v_path%\/}; v_path=${v_path}"/"
for i in $(find "${v_attrset["atoplog_dir"]}" -type f -regextype posix-extended -regex "${v_attrset["atoplog_name"]}")
do
	"$ATOP" -r ${i} -P CPU | egrep "^CPU.*" | sed -n '1p;$p' | awk '{printf "%d\n", $3}' > "$SPOOL_FILE"
	v_x=$(cat "$SPOOL_FILE" | head -n 1 | tr -d [:cntrl:])
	v_y=$(cat "$SPOOL_FILE" | tail -n 1 | tr -d [:cntrl:])
	log_info "${LAYER_NAME}.${v_module} ${i} contains info from ${v_x} to ${v_y}"
	if [ "$v_y" -gt "$v_last_snapid_inhdfs" ]
	then
		log_info "${LAYER_NAME}.${v_module} ${i} should be uploaded to hdfs, it's contains data upto ts: ${v_y}, and most-recent uploaded to hdfs atop-data relates to ${v_last_snapid_inhdfs};"
		v_value=$(basename "$i")
		v_key=${v_path}${v_value}
		log_info "${LAYER_NAME}.${v_module} hdfs-name for ${i} is ${v_key}"
		createfile -n "$v_key" -l "$i" -m "$UPLOADTIME_LIMIT" -o "true" 1>/dev/null 2>&1; rc="$?"
		[ "$rc" -ne "0" ] && {
			log_info "${LAYER_NAME}.${v_module} can not upload ${i} to hdfs as ${v_key}, error: ${rc}"
			return "$rc"
		}
		v_value=$(getxattr -n "${v_attrset["hdfs_flagfile"]}" -a "user.max_keyvalue" | egrep "^user.max_keyvalue.*" | awk '{printf "%s", $2;}')
		v_value=${v_value#\"}; v_value=${v_value%\"}
		[ "$v_value" == "none" ] && v_value="0"
		if [ "$v_y" -gt "$v_value" ]
		then
			log_info "${LAYER_NAME}.${v_module} ${v_y} ${v_value} updating user.max_keyvalue to ${v_y}"
			setxattr -n ${v_attrset["hdfs_flagfile"]} -a "user.max_keyvalue" -v "$v_y" -f "replace"; rc="$?"
			[ "$rc" -ne "0" ] && {
				log_info "${LAYER_NAME}.${v_module} can not set user.max_keyvalue to ${v_y} in flag-file ${v_attrset["hdfs_flagfile"]}; error: ${rc}"
				return "$rc"
			}
		fi
	else
		log_info "${LAYER_NAME}.${v_module} no need to upload ${i} to hdfs"
	fi
done

#end of upload_atopto_storage
}

upload_to_storage() {
local v_module="upload_to_storage"
local v_dcname=""
local v_x v_y v_z v_k v_hdfsfileis
local v_minkey v_maxkey v_fsize v_hdfsminkey v_hdfsmaxkey

while [ "$1" != "" ]
do
        case "$1" in
                "-h"|"--help")
                printf "%s\n" "
-h|--help	- This help
-d|--dcname	- Name of data-collection, by which this program shuld have to obtain various necessary attribytes for data processing'
		  Such as: path to local data-file with data, it's structure, it's key column, name of hdfs-file and etc;"
		return 0
                ;;
		"-d"|"--dcname")
		if [ -z "$2" ]
		then
			log_info "${LAYER_NAME}.${v_module} you have to set value for -d|--dcname option; See -h|--help"; return 1
		fi
		v_dcname="$2"
		shift 2
		;;
                *)
                log_info "${v_module} ${1} is not an option; Use -h|--help"
                return 1
                ;;
        esac
done

if [ -z "$v_dcname" ]
then
        log_info "${LAYER_NAME}.${v_module} you have to set value for -d|--dcname option; See -h|--help"; return 1
fi

log_info "${LAYER_NAME}.${v_module} ok, data-collection name is: ${v_dcname}"

check_attributes -d "$v_dcname"
if [ "$?" -ne "0" ]
then
	log_info "${LAYER_NAME}.${v_module} something wrong with work with attributes and|or with attrs itself, of data-collection with name: ${v_dcname};"
	return 1
fi

get_attribute -d "$v_dcname">"$SPOOL_FILE"

declare -A v_attrset
while read line
do
	v_key=$(echo ${line} | awk '{printf "%s", $1}')
	v_value=$(echo ${line} | awk '{printf "%s", $2}')
	#printf "%s\t%s\n" "$v_key" "$v_value"
	v_attrset["$v_key"]="$v_value"
done<"$SPOOL_FILE"

v_minkey=$(cat ${v_attrset["datafile_path"]} | awk -F ${v_attrset["column_separator"]} -v kcn=${v_attrset["key_column_number"]} '{print $kcn;}' | sort -n | head -n 1 | tr -d [:cntrl:])
v_maxkey=$(cat ${v_attrset["datafile_path"]} | awk -F ${v_attrset["column_separator"]} -v kcn=${v_attrset["key_column_number"]} '{print $kcn;}' | sort -n -r | head -n 1 | tr -d [:cntrl:])
if [[ ! "$v_x" =~ [0-9]+ && "$v_y" =~ [0-9]+ ]]
then
	log_info "${LAYER_NAME}.${v_module} can not obtain min/max values of key-column from ${v_attrset["datafile_path"]}"
	return 1
else
	log_info "${LAYER_NAME}.${v_module} min-key: ${v_minkey}; max-key ${v_maxkey}"
fi

#check is hdfs flag-file exist
cat /dev/null > "$SPOOL_FILE"
v_hdfsfileis="" #it means: no, hdfs-file - doesn't exist
v_y="${v_attrset["hdfs_flagfile"]}"
itemstatus -n "$v_y" 1>"$SPOOL_FILE" 2>&1
rc="$?"
if [ "$rc" -eq "0" ]
then
        #Ok we defenetly get info about hdfs-item from hdfs; Let's see: what it is
        v_x=$(cat "$SPOOL_FILE" | egrep "^type:" | awk '{printf "%s", $2;}' | tr [:lower:] [:upper:])
        if [ "$v_x" == "FILE" ]
        then
                log_info "${LAYER_NAME}.${v_module} hdfs-side available and flag-file ${v_y} is there"
                v_hdfsfileis="y"
        else
                #Well, it's not a file; Directory, symlinc, something else but not a file; Well nothing else but erroro-ending
                log_info "${LAYER_NAME}.${v_module} hdfs-side is available but ${v_y} is not a file, it's: ${v_x};"
                return 1
        fi
else
        #well may be hdfs is not reachable; Or may be it said something about error; Let us see: what is it
        v_x=$(cat "$SPOOL_FILE" | wc -l)
        if [ "$v_x" -gt "0" ]
        then
                #cat "$SPOOL_FILE"
                cat "$SPOOL_FILE" | grep -q "FileNotFoundExceptionFile" 1>/dev/null 2>&1
                if [ "$?" -eq "0"  ]
                then
                        v_hdfsfileis="n" #So it means that hdfs is reachable and it said that asked file: is absent;
                else
                        log_info "${LAYER_NAME}.${v_module} hdfs returned error on itemstatus req for: ${v_y}"
                        log_info "$SPOOL_FILE" "logfile"
                        return 1
                fi
        else
                log_info "${LAYER_NAME}.${v_module} hdfs is not reachable right now; error-exiting"; return 1
        fi
fi

[ -z "$v_hdfsfileis" ] && v_hdfsfileis="n"
if [ "$v_hdfsfileis" == "n" ]
then
        log_info "${LAYER_NAME}.${v_module} try to create flag-file ${v_attrset["hdfs_flagfile"]}"
        cat /dev/null > $SPOOL_FILE
        createfile -n "${v_attrset["hdfs_flagfile"]}" -l "$SPOOL_FILE" -m 10 1>/dev/null 2>&1; rc="$?"
        if [ "$rc" -ne "0" ]
        then
                log_info "${LAYER_NAME}.${v_module} can not create flag file ${v_attrset["hdfs_flagfile"]}"
                return 1
        fi
        setxattr -n ${v_attrset["hdfs_flagfile"]} -a "user.current_file" -v "none" -f "create"; v_z="$?"
        [ "$v_z" -ne "0" ] && {
                log_info "${LAYER_NAME}.${v_module} can not set xattr user.current_file for ${v_attrset["hdfs_flagfile"]}"
                return 1
        }
	v_y="none"
        log_info "${LAYER_NAME}.${v_module} flag-file ${v_attrset["hdfs_flagfile"]} created, it's xattrs setted"
else
        log_info "${LAYER_NAME}.${v_module} flag-file already is"
fi

if [ "$v_hdfsfileis" == "n" ]
then
	v_x=${v_attrset["hdfs_folder"]}; v_x=${v_x%\/}; v_x=${v_x}"/"$(date +%s)"_"${v_attrset["hdfs_filename"]}
	log_info "${LAYER_NAME}.${v_module} try to create hdfs-file ${v_x}; local-file: ${v_attrset["datafile_path"]}; upload-time limiet: ${UPLOADTIME_LIMIT}"
	createfile -n "$v_x" -l "${v_attrset["datafile_path"]}" -m "$UPLOADTIME_LIMIT" 1>/dev/null 2>&1; rc="$?"
	[ "$rc" -ne "0" ] && {
		log_info "${LAYER_NAME}.${v_module} cannot create hdfs-file ${v_x}, error:  ${rc}"
		return 1
	}
	log_info "${LAYER_NAME}.${v_module} ok, hdfs-file created;"
	setxattr -n ${v_attrset["hdfs_flagfile"]} -a "user.current_file" -v "$v_x" -f "replace"; rc="$?"
	setxattr -n ${v_x} -a "user.min_keyval" -v "$v_minkey" -f "create"; rc="$?"
	setxattr -n ${v_x} -a "user.max_keyval" -v "$v_maxkey" -f "create"; rc="$?"
	setxattr -n ${v_x} -a "user.next_file" -v "none" -f "create"; rc="$?"
	setxattr -n ${v_x} -a "user.prev_file" -v "none" -f "create"; rc="$?"
else
	v_x=$(getxattr -n "${v_attrset["hdfs_flagfile"]}" -a "user.current_file" | egrep "^user.current_file.*" | awk '{printf "%s", $2;}')
        v_x=${v_x#\"}; v_x=${v_x%\"}
        log_info "${LAYER_NAME}.${v_module} user.current_file found as: ${v_x}"
	cat /dev/null > "$SPOOL_FILE"
	itemstatus -n "${v_x}" 1>"$SPOOL_FILE" 2>&1
	rc="$?"
	if [ "$rc" -ne "0" ]
	then
		v_y=$(cat "$SPOOL_FILE" | wc -l)
		if [ "$v_y" -gt "0" ]
		then
			cat "$SPOOL_FILE" | grep -q "FileNotFoundExceptionFile" 1>/dev/null 2>&1
			if [ "$?" -eq "0"  ]
			then
				log_info "${LAYER_NAME}.${v_module} hdfs-file ${v_x} is registered, in flag-file ${v_attrset["hdfs_flagfile"]} as hdfs-file with the most recent data"
				log_info "${LAYER_NAME}.${v_module} but file ${v_x} is not found in hdfs"
			else
				log_info "${LAYER_NAME}.${v_module} some unknown error in hdfs-answer:"
				log_info "$SPOOL_FILE" "logfile"
			fi
		else
			log_info "${LAYER_NAME}.${v_module} can not obtain any answer from hdfs, at this moment"
		fi
		[ -f "$SPOOL_FILE" ] && rm -f "$SPOOL_FILE"
		find $HOME -type f -name '.cookie_*' -delete
		return 1
	else
		v_fsize=$(cat "$SPOOL_FILE" | egrep "^length:" | awk '{printf "%s", $2;}')
		v_hdfsminkey=$(getxattr -n "$v_x" -a "user.min_keyval" | egrep "^user.min_keyval*" | awk '{printf "%s", $2;}')
		v_hdfsminkey=${v_hdfsminkey#\"}; v_hdfsminkey=${v_hdfsminkey%\"}
		v_hdfsmaxkey=$(getxattr -n "$v_x" -a "user.max_keyval" | egrep "^user.max_keyval*" | awk '{printf "%s", $2;}')
		v_hdfsmaxkey=${v_hdfsmaxkey#\"}; v_hdfsmaxkey=${v_hdfsmaxkey%\"}
		log_info "${LAYER_NAME}.${v_module} hdfs-file ${v_x} has user.max_keyval=${v_hdfsmaxkey}; user.min_keyval=${v_hdfsminkey}"
		cat /dev/null > "$SPOOL_FILE"
		#well here I suppose that key value of dataset - is constantly growing;
		cat "${v_attrset["datafile_path"]}" | awk -F "${v_attrset["column_separator"]}" -v cn="${v_attrset["key_column_number"]}" -v mv="$v_hdfsmaxkey" '{ if ( $cn >  mv ) {print $0;}}' > "$SPOOL_FILE"
		rc=$(cat "$SPOOL_FILE" | wc -l)
		[ "$rc" -eq "0" ] && {
			log_info "${LAYER_NAME}.${v_module} we had no new data to save it to hdfs"
			rm -f "$SPOOL_FILE" 1>/dev/null 2>&1; return 1
		}
		v_minkey=$(cat "$SPOOL_FILE" | awk -F ${v_attrset["column_separator"]} -v kcn=${v_attrset["key_column_number"]} '{print $kcn;}' | sort -n | head -n 1 | tr -d [:cntrl:])
		v_maxkey=$(cat "$SPOOL_FILE" | awk -F ${v_attrset["column_separator"]} -v kcn=${v_attrset["key_column_number"]} '{print $kcn;}' | sort -n -r | head -n 1 | tr -d [:cntrl:])
		log_info "${LAYER_NAME}.${v_module} delta: new rows count: ${rc}, minkey: ${v_minkey}, maxkey: ${v_maxkey}"
		if [ "$v_fsize" -le "${v_attrset["hdfs_filesize_limit"]}" ]
		then
			v_maxkey=$((v_maxkey>v_hdfsmaxkey?v_maxkey:v_hdfsmaxkey))
			v_minkey=$((v_minkey<v_hdfsminkey?v_minkey:v_hdfsminkey))
			log_info "${LAYER_NAME}.${v_module} under size limit, will try to append new data to current hdfs-file"
			log_info "${LAYER_NAME}.${v_module} try to append data to current hdfs-file"
			appendtofile -n "$v_x" -l "$SPOOL_FILE" -m "$UPLOADTIME_LIMIT" 1>/dev/null 2>&1; rc="$?"
			[ "$rc" -ne "0" ] && {
				log_info "${LAYER_NAME}.${v_module} cannot append new data from ${SPOOL_FILE} to ${v_x}, err: ${rc}"
				rm -f "$SPOOL_FILE" 1>/dev/null 2>&1; return 1
			}
			log_info "${LAYER_NAME}.${v_module} try to update info about min|max row keys in hdfs-file, ${v_minkey}|${v_maxkey}"
			setxattr -n ${v_x} -a "user.min_keyval" -v "$v_minkey" -f "replace"; rc="$?"
			setxattr -n ${v_x} -a "user.max_keyval" -v "$v_maxkey" -f "replace"; rc="$?"
		else
			v_y=${v_attrset["hdfs_folder"]}; v_y=${v_y%\/}; v_y=${v_y}"/"$(date +%s)"_"${v_attrset["hdfs_filename"]}
			log_info "${LAYER_NAME}.${v_module} size limit exceeded; we have to create new hdfs-file: ${v_y}"
			createfile -n "$v_y" -l "$SPOOL_FILE" -m "$UPLOADTIME_LIMIT" 1>/dev/null 2>&1; rc="$?"
			[ "$rc" -ne "0" ] && {
			        log_info "${LAYER_NAME}.${v_module} cannot create hdfs-file ${v_x}, error:  ${rc}"
			        return 1
			}
			log_info "${LAYER_NAME}.${v_module} ok, hdfs-file created;"
			setxattr -n "$v_y" -a "user.min_keyval" -v "$v_minkey" -f "create"; rc="$?"
			setxattr -n "$v_y" -a "user.max_keyval" -v "$v_maxkey" -f "create"; rc="$?"
			setxattr -n "$v_y" -a "user.next_file" -v "none" -f "create"; rc="$?"
			setxattr -n "$v_y" -a "user.prev_file" -v "$v_x" -f "create"; rc="$?"
			setxattr -n "$v_x" -a "user.next_file" -v "$v_y" -f "replace"; rc="$?"
		fi
		[ -f "$SPOOL_FILE" ] && rm -f "$SPOOL_FILE"
		find $HOME -type f -name '.cookie_*' -delete
	fi
fi

return 0
# cat /tmp/158*_ping.dat | awk -F ";" '{if ( x[$1] == "" ){x[$1]=1; print $0;}}' | sort -t ";" -n -k 1 > /tmp/ping.dat
}




initdb() {
local v_module="initdb"
local rc

"$SQLITE" "$SQLITEDB" << __EOF__ 1>"$TEMP_FILE" 2>&1
create table datasets(id integer primary key on conflict abort autoincrement, dataset_name text);
create table datasets_attributes(id integer primary key on conflict abort autoincrement, attr_name text, attr_value text, ds_id integer, constraint to_ds_id_fk foreign key (ds_id) references datasets(id));
create index datasets_attributes_fk_idx on datasets_attributes(ds_id);
insert into datasets(dataset_name) values('ping');
insert into datasets_attributes(attr_name, attr_value, ds_id) values('datafile_path', '/tmp/pinglog.log', 1);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('column_count', '11', 1);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('column_separator', ';', 1);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('key_column_number', '1', 1);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('hdfs_folder', 'perm-dev2_dev/ping/', 1);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('hdfs_filename', 'ping.dat', 1);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('hdfs_filesize_limit', '20480', 1);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('hdfs_flagfile', 'perm-dev2_dev/ping/flag.txt', 1);

insert into datasets(dataset_name) values('awr');
insert into datasets_attributes(attr_name, attr_value, ds_id) values('hdfs_flagfile', 'perm-dev2_dev/awr/flag.txt', 2);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('hdfs_folder', 'perm-dev2_dev/awr/', 2);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('oradir', 'AWR_DUMP_DIR', 2);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('dump_name', 'awr', 2);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('free_space', '1073741824', 2);

insert into datasets(dataset_name) values('atop');
insert into datasets_attributes(attr_name, attr_value, ds_id) values('hdfs_flagfile', 'perm-dev2_dev/atop/flag.txt', 3);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('hdfs_folder', 'perm-dev2_dev/atop/', 3);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('atoplog_dir', '/var/log/atop/', 3);
insert into datasets_attributes(attr_name, attr_value, ds_id) values('atoplog_name', '.*atop_[0-9]+', 3);
.exit
__EOF__
rc="$?"
if [ "$rc" -eq "0" ]
then
        log_info "${LAYER_NAME}.${v_module} ok"; return 0
else
        log_info "${LAYER_NAME}.${v_module} ok"
        log_info "$TEMP_FILE" "logfile"
        return "$rc"
fi
}

