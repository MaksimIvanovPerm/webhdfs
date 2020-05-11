# webhdfs
bash interface to HDFS through webhdfs api

python 2.6, with default set of python-modules, is enough
```
 . ./webhdfs/webhdfs_lib.sh; usage
HDFS-interface's subprograms brief desc:
mk_dir          - Make hdfs-directory with given name and permission mode.
                  Default permission is 644 for files, 755 for directories; Valid Values 0 - 1777
ls_dir          - Listing items in given hdfs-directory, or show some info about given hdfs-file;
itemstatus      - Show info about given at hdfs-item
                  In case of success it returns block of new-line delimited lines, with hdfs-item attributes, in key-valu form;
                  see FileStatus JSON object in webhdfs doc;

createfile      - Make file at hdfs, by uploading there given local file;
delete          - Delete file|directory at hdfs, optionally - recursively;
getfile         - Get given file from hdfs
appendtofile    - Append content of given local-file to given hdfs-file;
renamefile      - Renaming
setmod          - Set permission of a file/directory;

getxattr        - Get out from hdfs extended attribute(s), of the given file|folder
setxattr        - Set an extended-attribute to the given file|folder in hdfs
rmxattr         - Remove given extended-attribute of the given file|folder in hdfs

All subprograms have -h|--help call option
In all subprograms value for option: -n|--name - has to be an absolute path
--------------------------------------------------------------------------------------
ENV:
SILENT          - In case it non-zero: turns off output of library-subprogram messages to stdout;
                  Messages still will be made by the subproc, but will be written to logfile only;
---------------------------------------------------------------------------------------
Logdile:        - /tmp/webhdfs_lib.log
Config:         - /home/oracle/webhdfs/settings.conf
```

# mrails
app-level procedures for uploading data to hdfs;

Also I recently added mrails_lib.sh and mrails.conf: shell-resources, which intended to be used as application level program;
mrails_lib.sh provides you with routines for uploading to hdfs oracle-awr, atop (upload_awrto_storage & upload_atopto_storage routines) and some csv-file (upload_to_storage);
In case of uploadign to hdfs some csv-file: it is supposed that given csv-file: is mainteined by some other program or task or utility ao something like it;
upolad_to_storade-routine: does not anything with given csv-file, except reading data from it;

mriails_lib.sh uses routines from webhdfs_lib.sh;
Also mrails_lib.sh uses sqlite, version >=3.6.20 of sqlite: is enough
An example of using mrails-routine:
```
cat ./atop.sh
#!/bin/bash
sudo su -l oracle << __EOFF__
. /etc/profile.d/ora_env.sh
cd /home/oracle/webhdfs
env | sort > /tmp/awrcron.log
pwd>>/tmp/awrcron.log
export SILENT=""
. ./mrails_lib.sh; upload_atopto_storage -d "atop" 1>>/tmp/atopcron.log 2>&1
delete_aux_files
__EOFF__
```
