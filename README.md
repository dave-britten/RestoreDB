# RestoreDB

## About

RestoreDB is a stored procedure designed to simplify multi-step database restores from full, differential, and/or transaction log backups. This procedure allows you to specify one or more paths containing backup files (with optional recursive searching), choose a point-in-time to try to restore to, automatically move and rename the target database files, and provide any other "WITH" options necessary. There are other subtle features described in more detail below.

## Installation

Run RestoreDB.sql using SQL Server Management Studio or any other suitable SQL Server client that can run a typical multi-batch (i.e. containing "GO") script. By default, RestoreDB will be created in the master database's dbo schema.

## Usage

RestoreDB can be executed from any database by calling `EXEC master.dbo.RestoreDB @Source...`. The minimum set of parameters that you must provide are *@Source* and *@Database*. This will search for backup files in the provided *@Source* path, and restore the database named in *@Database* to the most recent time available in the discovered backups.

RestoreDB supports restoring from backup files containing multiple backups (e.g. multiple backups written to the same file without `FORMAT` or `INIT`).

## Parameters

- **@Source** nvarchar(4000) (required) - Source paths to search for backup files.  
You may separate multiple paths with commas. Prefix a path with "+" to recursively search subdirectories in that path.
- **@Database** sysname (required) - Name of the database to restore from. Backups of databases not matching this name will be ignored.
- **@RestoreAs** sysname - Name of the database to restore to. Database will be restored to *@Database* if *@RestoreAs* is not specified.
- **@StopAt** datetime - By default, RestoreDB will attempt to restore the database to the latest available time. If you specify *@StopAt*, then RestoreDB will attempt to use the available full, differential, and transaction log backups to get as close to *@StopAt* as possible.  
*You do not have to have transaction log backups or full recovery mode to use this option*, but you will see a warning if there are no log backups that can be used to restore to that exact time. In such cases, the database will be restored to the latest possible time earlier than *@StopAt* using whatever full/differential/log backups *are* available.
- **@AutoMove** bit (default: 0) - If set to 1, then all data and transaction log files will automatically be moved to the instance default data and log directories using `WITH MOVE`.
- **@AutoRename** bit (default:0) - If set to 1, then RestoreDB will automatically rename the restored database files (using `WITH MOVE`) by replacing any occurence of the database name in *@Database* with the database name in *@RestoreAs*.  
This option has no effect if *@RestoreAs* has not been specified (or does not differ from *@Database*), or if at least one of *@AutoMove*, *@MoveDataFilesTo*, or *@MoveLogFilesTo* has not been specified.
- **@MoveDataFilesTo**, **@MoveLogFilesTo** nvarchar(max) - Move all data files or transaction log files to the provided path. The specified paths should *not* be supplied with a backslash ('\\') at the end.
- **@RestoreFullWith**, **@RestoreDiffWith**, **@RestoreLogWith** nvarchar(max) - Additional `WITH` options that will be used for any full, differential, or transaction log restores, respectively. These will be appended directly to the `WITH` clause built by RestoreDB, and thus should *not* include the initial `WITH` keyword.  
Defaults to `STATS = 5` for full and differential backups.
- **@NoRecovery** bit (default: 0) - Leave the database in a recovering state if 1 is specified. By default, RestoreDB will attempt to recover the database.
- **@Standby** nvarchar(max) - Standby filename to use to leave the database in standby mode, instead of running recovery. Standby mode is not used if this is not provided.
- **@Replace** bit (default: 0) - Restore the full database backup `WITH REPLACE` to allow replacing an existing database. This option is ignored if the target database does not exist, i.e. it will *not* cause an error.
- **@Debug** bit (default: 0) - Return some intermediate result sets to the client for debugging purposes.
- **@WhatIf** bit (default: 0) - If this parameter is set to 1, no actual `RESTORE DATABASE` statements will be executed, they will only be printed.

## Path Token Replacements: [datadir] and [logdir]

In the cases listed below, the tokens [datadir] and [logdir] will automatically be replaced with the instance default data and log directories respectively.

- The path specified in *@MoveDataFilesTo*
- The path specified in *@MoveLogFilesTo*
- Any options specified in *@RestoreFullWith*

You may thus specify paths such as `@MoveDataFilesTo = '[datadir]\TestCopy'`.

## Examples

Restore an existing database to Nov. 10, 2018 at 3:00 AM, using any backups underneath 'K:\SQLBackups':

    EXEC master.dbo.RestoreDB @Source='+K:\SQLBackups', @Database='ERPDatabase', @Replace=1, @StopAt='20181110 03:00:00.00'

Restore a database where backups may be located in multiple paths, e.g. an AlwaysOn Availability Group where the backups are written to local storage on the current secondary server:

    EXEC master.dbo.RestoreDB @Source='\\Server1\Backups\ERPDatabase,\\Server2\Backups\ERPDatabase', @Database='ERPDatabase', @Replace=1

Restore a test copy of a database to the most recent point in time, using a new database name:

    EXEC master.dbo.RestoreDB @Source='+K:\SQLBackups', @Database='ERPDatabase_TestEnvironment', @AutoMove=1, @AutoRename=1

## Known Issues

- Error handling is, so far, rudimentary at best.
- RestoreDB will attempt to run `RESTORE HEADERONLY` on *all* files found in the provided source paths, i.e. there is no filename wildcard matching ('\*.bak', '\*.trn', etc.) yet.
- RestoreDB does not yet handle filestream data automatically. It may still be possible to restore filestream-enabled databases by specifying *@RestoreFullWith* options manually.
- This has been coded for SQL Server 2016/2017, and will fail on older versions due to differences in `RESTORE HEADERONLY` or `RESTORE FILELISTONLY`, and usage of the `STRING_SPLIT` function introduced in SQL Server 2016. Support for at least SQL Server 2012 is planned for future relases.
- RestoreDB is not designed to work with backup schemes that make use of filegroup backups. There is currently no plan to change this.

## License Terms and Conditions

**Copyright 2018 Dave Britten**

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
