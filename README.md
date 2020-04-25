# webhdfs
bash interface to HDFS through webhdfs api
```
 . ./webhdfs/webhdfs_lib.sh; usage
Summary of library subprograms:
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

getxattr        - Get out from hdfs extended attribute(s), of the given file|folder
setxattr        - Set an extended-attribute to the given file|folder in hdfs
rmxattr         - Remove given extended-attribute of the given file|folder in hdfs

All subprograms have -h|--help call option
--------------------------------------------------------------------------------------
ENV:
SILENT          - In case it non-zero: turns off output of library-subprogram messages to stdout;
                  Messages still will be made by the subproc, but will be written to logfile only;
---------------------------------------------------------------------------------------
Logdile:        - /tmp/webhdfs_lib.log
Config:         - /home/oracle/webhdfs/settings.conf
```
