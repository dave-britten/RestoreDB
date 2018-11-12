USE master
GO

IF object_id('dbo.RestoreDB') IS NULL
	EXEC sp_executesql N'CREATE PROCEDURE dbo.RestoreDB AS SELECT ''Stub'' AS Stub;'
GO

ALTER PROCEDURE dbo.RestoreDB
	@Source nvarchar(4000),
	@Database sysname,
	@RestoreAs sysname = NULL,
	@StopAt datetime = NULL,
	@AutoMove bit = 0,
	@AutoRename bit = 0,
	@MoveDataFilesTo nvarchar(max) = NULL,
	@MoveLogFilesTo nvarchar(max) = NULL,
	@RestoreFullWith nvarchar(max) = 'STATS = 5',
	@RestoreDiffWith nvarchar(max) = 'STATS = 5',
	@RestoreLogWith nvarchar(max) = NULL,
	@NoRecovery bit = 0,
	@Standby nvarchar(max) = NULL,
	@Replace bit = 0,
	@Debug bit = 0,
	@WhatIf bit = 0
AS

SET NOCOUNT ON

--Restoring to the original database name?
IF @RestoreAs IS NULL
	SET @RestoreAs = @Database

DECLARE @backupfile nvarchar(4000)
DECLARE @backupsfound int
DECLARE @backupschecked int
DECLARE @restoringto varchar(50)
DECLARE @sql nvarchar(max)

--Check if the database being restored as exists. Needed for determining whether to use the WITH REPLACE option.
DECLARE @dbexists bit = 0
IF EXISTS (SELECT name FROM master.sys.databases WHERE name = @RestoreAs)
	SET @dbexists = 1

--Cursor-reading variables.
DECLARE @fullfile nvarchar(4000)
DECLARE @fullpos int
DECLARE @fullfirstlsn decimal(25,0)
DECLARE @fulllastlsn decimal(25,0)
DECLARE @fullfinish datetime
DECLARE @difffile nvarchar(4000)
DECLARE @diffpos int
DECLARE @difffirstlsn decimal(25,0)
DECLARE @difflastlsn decimal(25,0)
DECLARE @difffinish datetime
DECLARE @logid int
DECLARE @logfile nvarchar(4000)
DECLARE @logpos int
DECLARE @logfirstlsn decimal(25,0)
DECLARE @loglastlsn decimal(25,0)
DECLARE @lastlsn decimal(25,0)
DECLARE @logfinish datetime

--Log files that will be restored.
DECLARE @logfiles TABLE (
	id int NOT NULL IDENTITY(1,1),
	filename nvarchar(4000),
	pos int,
	finish datetime
)

DECLARE @stopped bit

--Get the instance default data and log directories.
DECLARE @datadir varchar(max) = CAST(serverproperty('InstanceDefaultDataPath') AS varchar(max))
DECLARE @logdir varchar(max) = CAST(serverproperty('InstanceDefaultLogPath') AS varchar(max))

SET @datadir = LEFT(@datadir, LEN(@datadir) - 1) -- Remove '\'
SET @logdir = LEFT(@logdir, LEN(@logdir) - 1)

--Parse the user-supplied paths. Separate multiple paths with a comma, and prefix a path with + to search recursively in subdirectories.
DECLARE @paths TABLE (
	dir nvarchar(1000) NOT NULL,
	depth int NOT NULL DEFAULT 1
)
DECLARE @dir nvarchar(1000)
DECLARE @depth int

INSERT INTO @paths (dir) SELECT value FROM STRING_SPLIT(@Source, ',')

UPDATE @paths SET dir = STUFF(dir, 1, 1, ''), depth = 0 WHERE dir LIKE '+%'

--Results from xp_dirtree.
DECLARE @dirtree TABLE (
	id int NOT NULL IDENTITY(1,1),
    SubDirectory nvarchar(255),
    Depth smallint,
    FileFlag bit,
	Parent int,
	FullPath nvarchar(4000)
)

--Results from RESTORE HEADERONLY.
DECLARE @backups TABLE (
	Filename nvarchar(4000),
	BackupName nvarchar(128),
	BackupDescription nvarchar(255),
	BackupType smallint,
	ExpirationDate datetime,
	Compressed bit,
	Position smallint,
	DeviceType tinyint,
	UserName nvarchar(128),
	ServerName nvarchar(128),
	DatabaseName nvarchar(128),
	DatabaseVersion int,
	DatabaseCreationDate datetime,
	BackupSize numeric(20,0),
	FirstLSN numeric(25,0),
	LastLSN numeric(25,0),
	CheckpointLSN numeric(25,0),
	DatabaseBackupLSN numeric(25,0),
	BackupStartDate datetime,
	BackupFinishDate datetime,
	SortOrder smallint,
	CodePage smallint,
	UnicodeLocaleId int,
	UnicodeComparisonStyle int,
	CompatibilityLevel smallint,
	SoftwareVendorId int,
	SoftwareVersionMajor int,
	SoftwareVersionMinor int,
	SoftwareVersionBuild int,
	MachineName nvarchar(128),
	Flags int,
	BindingID uniqueidentifier,
	RecoveryForkID uniqueidentifier,
	Collation nvarchar(128),
	FamilyGUID uniqueidentifier,
	HasBulkLoggedData bit,
	IsSnapshot bit,
	IsReadOnly bit,
	IsSingleUser bit,
	HasBackupChecksums bit,
	IsDamaged bit,
	BeginsLogChain bit,
	HasIncompleteMetadata bit,
	IsForceOffline bit,
	IsCopyOnly bit,
	FirstRecoveryForkID uniqueidentifier,
	ForkPointLSN numeric(25,0),
	RecoveryModel nvarchar(60),
	DifferentialBaseLSN numeric(25,0),
	DifferentialBaseGUID uniqueidentifier,
	BackupTypeDescription nvarchar(60),
	BackupSetGUID uniqueidentifier,
	CompressedBackupSize bigint,
	containment tinyint,
	KeyAlgorithm nvarchar(32),
	EncryptorThumbprint varbinary(20),
	EncryptorType nvarchar(32)
)

--Results from RESTORE FILELISTONLY.
DECLARE @files TABLE (
	LogicalName nvarchar(128),
	PhysicalName nvarchar(260),
	Type char(1),
	FileGroupName nvarchar(128),
	Size numeric(20,0),
	MaxSize numeric(20,0),
	FileID bigint,
	CreateLSN numeric(25,0),
	DropLSN numeric(25,0),
	UniqueID uniqueidentifier,
	ReadOnlyLSN numeric(25,0),
	ReadWriteLSN numeric(25,0),
	BackupSizeInBytes bigint,
	SourceBlockSize int,
	FileGroupID int,
	LogGroupGUID uniqueidentifier,
	DifferentialBaseLSN numeric(25,0),
	DifferentialBaseGUID uniqueidentifier,
	IsReadOnly bit,
	IsPresent bit,
	TDEThumbprint varbinary(32),
	--For SQL Server 2016+
	SnapshotURL varchar(360)
)

--Find files in the source paths.
RAISERROR('Finding backup files...', 0, 1) WITH NOWAIT

DECLARE dircursor CURSOR FOR
SELECT dir, depth FROM @paths

OPEN dircursor
FETCH NEXT FROM dircursor INTO @dir, @depth

WHILE @@FETCH_STATUS = 0
BEGIN
	IF @Debug = 1
		RAISERROR('%s, %d', 0, 1, @dir, @depth)
	INSERT INTO @dirtree (SubDirectory, Depth, FileFlag)
	EXEC master.sys.xp_dirtree @dir, @depth, 1

	UPDATE @dirtree SET FullPath = @dir + '\' + SubDirectory WHERE FullPath IS NULL
	FETCH NEXT FROM dircursor INTO @dir, @depth
END

CLOSE dircursor
DEALLOCATE dircursor

--Get parent ID for each directory entry.
UPDATE d1
SET Parent = d2.id
FROM @dirtree d1
	CROSS APPLY (SELECT TOP 1 d2.id FROM @dirtree d2 WHERE d2.Depth = d1.Depth - 1 AND d2.id < d1.id ORDER BY d2.id DESC) d2;

--Recursively generate full paths for each directory entry.
WITH dr AS (
	SELECT
		id,
		SubDirectory,
		Depth,
		FileFlag,
		Parent,
		FullPath
	FROM @dirtree d
	WHERE depth = 1
	UNION ALL
	SELECT
		d.id,
		d.SubDirectory,
		d.Depth,
		d.FileFlag,
		d.Parent,
		CAST(dr.FullPath + N'\' + d.SubDirectory AS nvarchar(4000)) AS FullPath
	FROM @dirtree d
		INNER JOIN dr
			ON d.Parent = dr.id
)
UPDATE d
SET FullPath = dr.FullPath
FROM @dirtree d
	INNER JOIN dr
		ON d.id = dr.id

--Can get rid of the directories now; only need the files.
DELETE FROM @dirtree WHERE FileFlag = 0

IF @Debug = 1
	SELECT * FROM @dirtree

--Start reading all the backup files with RESTORE HEADERONLY.
SELECT @backupsfound = COUNT(*) FROM @dirtree
RAISERROR('Found %d backups to examine. Please wait...', 0, 1, @backupsfound) WITH NOWAIT
SET @backupschecked = 0

DECLARE backupfiles CURSOR FOR
SELECT FullPath FROM @dirtree ORDER BY id

OPEN backupfiles
FETCH NEXT FROM backupfiles INTO @backupfile

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @sql = 'RESTORE HEADERONLY FROM DISK = ''' + REPLACE(@backupfile, '''', '''''') + ''''
	INSERT INTO @backups (BackupName, BackupDescription, BackupType, ExpirationDate, Compressed, Position, DeviceType, UserName, ServerName, DatabaseName, DatabaseVersion, DatabaseCreationDate, BackupSize, FirstLSN, LastLSN, CheckpointLSN, DatabaseBackupLSN, BackupStartDate, BackupFinishDate, SortOrder, CodePage, UnicodeLocaleId, UnicodeComparisonStyle, CompatibilityLevel, SoftwareVendorId, SoftwareVersionMajor, SoftwareVersionMinor, SoftwareVersionBuild, MachineName, Flags, BindingID, RecoveryForkID, Collation, FamilyGUID, HasBulkLoggedData, IsSnapshot, IsReadOnly, IsSingleUser, HasBackupChecksums, IsDamaged, BeginsLogChain, HasIncompleteMetadata, IsForceOffline, IsCopyOnly, FirstRecoveryForkID, ForkPointLSN, RecoveryModel, DifferentialBaseLSN, DifferentialBaseGUID, BackupTypeDescription, BackupSetGUID, CompressedBackupSize, containment, KeyAlgorithm, EncryptorThumbprint, EncryptorType)
	EXEC (@sql)
	UPDATE @backups SET Filename = @backupfile WHERE Filename IS NULL

	SET @backupschecked = @backupschecked + 1

	IF @backupschecked % 100 = 0
		RAISERROR('%d/%d', 0, 1, @backupschecked, @backupsfound) WITH NOWAIT

	FETCH NEXT FROM backupfiles INTO @backupfile
END

CLOSE backupfiles
DEALLOCATE backupfiles

RAISERROR('Done examining backups.', 0, 1) WITH NOWAIT

IF @Debug = 1
	SELECT * FROM @backups

--If the user supplied a STOPAT time, then ignore any full/differential backups that finished after that time.
IF @StopAt IS NOT NULL
DELETE FROM @backups WHERE BackupFinishDate > @StopAt AND BackupTypeDescription <> 'Transaction Log'

--Find the full backup to start from.
SELECT TOP 1 @fullfile = Filename, @fullpos = Position, @fullfirstlsn = FirstLSN, @fulllastlsn = LastLSN, @fullfinish = BackupFinishDate
FROM @backups
WHERE BackupTypeDescription = 'Database'
ORDER BY BackupFinishDate DESC

IF @fullfile IS NULL
BEGIN
	RAISERROR('Could not find a full backup that completed before the supplied @StopAt time.', 16, 1)
	RETURN 1
END

--Find a differential backup.
SELECT TOP 1 @difffile = Filename, @diffpos = Position, @difffirstlsn = FirstLSN, @difflastlsn = LastLSN, @difffinish = BackupFinishDate
FROM @backups
WHERE DifferentialBaseLSN = @fullfirstlsn
	AND BackupTypeDescription = 'Database Differential'
ORDER BY BackupFinishDate DESC

IF @Debug = 1
	SELECT @fullfile, @difffile

--Start looking for the log chain, starting from the full or differential backup.
SET @lastlsn = ISNULL(@difflastlsn, @fulllastlsn)

SET @logfinish = NULL
WHILE 1 = 1
BEGIN
	SET @logfile = NULL

	SELECT TOP 1 @logfile = Filename, @logpos = Position, @logfirstlsn = FirstLSN, @loglastlsn = LastLSN, @lastlsn = LastLSN, @logfinish = BackupFinishDate
	FROM @backups
	WHERE FirstLSN <= @lastlsn
		AND LastLSN > @lastlsn
		AND BackupTypeDescription = 'Transaction Log'
	ORDER BY FirstLSN ASC

	--No more logs, all done.
	IF @logfile IS NULL
		BREAK

	INSERT INTO @logfiles (Filename, pos, finish)
	VALUES (@logfile, @logpos, @logfinish)

	IF @logfinish >= @StopAt --Don't need any more, this backup covers the STOPAT time.
		BREAK
END

IF @logfinish < @StopAt OR (@logfinish IS NULL AND @StopAt IS NOT NULL)
BEGIN
	SET @restoringto = CONVERT(varchar(50), ISNULL(@logfinish, ISNULL(@difffinish, @fullfinish)), 121)
	RAISERROR('Warning: Restoring to %s, as no transaction log backups are available past that point in time.', 8, 1, @restoringto) WITH NOWAIT
END

IF @Debug = 1
	SELECT * FROM @logfiles ORDER BY id

--We now have enough info to start restoring.

--Replace [datadir] and [logdir] tokens with instance default directories in the user-supplied WITH options.
IF @RestoreFullWith IS NOT NULL
BEGIN
	SET @RestoreFullWith = REPLACE(@RestoreFullWith, '[datadir]', @datadir)
	SET @RestoreFullWith = REPLACE(@RestoreFullWith, '[logdir]', @logdir)
END

--If files are going to be moved/renamed automatically, then we need to get the list of database files from the full backup first.
IF @AutoMove = 1 OR @MoveDataFilesTo IS NOT NULL OR @MoveLogFilesTo IS NOT NULL OR (@AutoRename = 1 AND @Database <> @RestoreAs)
BEGIN
	SET @sql = 'RESTORE FILELISTONLY FROM DISK = ''' + REPLACE(@fullfile, '''', '''''') + ''''
	INSERT INTO @files
	EXEC (@sql)

	--Extract the filename without path.
	UPDATE @files SET PhysicalName = RIGHT(PhysicalName, CHARINDEX('\', ISNULL(REVERSE(PhysicalName), '') + '\') - 1)

	--Change any occurrences of the original database name to the @RestoreAs database name within the database filenames.
	IF (@AutoRename = 1 AND @Database <> @RestoreAs)
		UPDATE @files SET PhysicalName = REPLACE(PhysicalName, @Database, @RestoreAs)
END

--Generate the full restore statement.
SET @sql = 'RESTORE DATABASE ' + QUOTENAME(@RestoreAs) + ' FROM DISK = ''' + REPLACE(@fullfile, '''', '''''') + ''' WITH FILE = ' + CAST(@fullpos AS varchar(10))

IF @Replace = 1 AND @dbexists = 1
	SET @sql = @sql + ', REPLACE'

IF @AutoMove = 1
	SELECT @sql = @sql + ', MOVE ''' + REPLACE(LogicalName, '''', '''''') + ''' TO ''' + REPLACE(CASE WHEN Type = 'L' THEN @logdir ELSE @datadir END + '\' + PhysicalName, '''', '''''') + ''''
	FROM @files

IF @MoveDataFilesTo IS NOT NULL
	SELECT @sql = @sql + ', MOVE ''' + REPLACE(LogicalName, '''', '''''') + ''' TO ''' + REPLACE(REPLACE(REPLACE(@MoveDataFilesTo, '[datadir]', @datadir), '[logdir]', @logdir) + '\' + PhysicalName, '''', '''''') + ''''
	FROM @files
	WHERE Type <> 'L'

IF @MoveLogFilesTo IS NOT NULL
	SELECT @sql = @sql + ', MOVE ''' + REPLACE(LogicalName, '''', '''''') + ''' TO ''' + REPLACE(REPLACE(REPLACE(@MoveLogFilesTo, '[datadir]', @datadir), '[logdir]', @logdir) + '\' + PhysicalName, '''', '''''') + ''''
	FROM @files
	WHERE Type = 'L'

IF @difffile IS NOT NULL OR EXISTS (SELECT id FROM @logfiles) OR @NoRecovery = 1 --Will we be restoring differential or transaction log backups, or has the user requested WITH NORECOVERY?
	SET @sql = @sql + ', NORECOVERY'
ELSE IF @Standby IS NOT NULL --No differentials or logs, but has the user requested WITH STANDBY?
	SET @sql = @sql + ', STANDBY = ''' + REPLACE(@Standby, '''', '''''') + ''''
ELSE --Nope, run recovery.
	SET @sql = @sql + ', RECOVERY'

--Add user-supplied WITH options.
IF @RestoreFullWith IS NOT NULL
	SET @sql = @sql + ', ' + @RestoreFullWith

RAISERROR('%s', 0, 1, @sql) WITH NOWAIT
IF @WhatIf = 0
BEGIN
	EXEC (@sql)
	IF @@ERROR <> 0
	BEGIN
		RAISERROR('Failed to restore full backup. The preceding error messages, if any, may indicate the cause of failure.', 16, 2) WITH NOWAIT
		RETURN 2
	END
END

--Restore the differential backup, if available.
IF @difffile IS NOT NULL
BEGIN
	SET @sql = 'RESTORE DATABASE ' + QUOTENAME(@RestoreAs) + ' FROM DISK = ''' + REPLACE(@difffile, '''', '''''') + ''' WITH FILE = ' + CAST(@diffpos AS varchar(10))

	IF EXISTS (SELECT id FROM @logfiles) OR @NoRecovery = 1 --Will we be restoring transaction log backups, or has the user requested WITH NORECOVERY?
		SET @sql = @sql + ', NORECOVERY'
	ELSE IF @Standby IS NOT NULL --No logs, but has the user requested WITH STANDBY?
		SET @sql = @sql + ', STANDBY = ''' + REPLACE(@Standby, '''', '''''') + ''''
	ELSE --Nope, run recovery.
		SET @sql = @sql + ', RECOVERY'

	--Add user-supplied WITH options.
	IF @RestoreDiffWith IS NOT NULL
		SET @sql = @sql + ', ' + @RestoreDiffWith

	RAISERROR('%s', 0, 1, @sql) WITH NOWAIT
	IF @WhatIf = 0
	BEGIN
		EXEC (@sql)
		IF @@ERROR <> 0
		BEGIN
			RAISERROR('Failed to restore differential backup. The preceding error messages, if any, may indicate the cause of failure.', 16, 3) WITH NOWAIT
			RETURN 3
		END
	END
END

--Iterate through the log backups that will be restored.
DECLARE logfiles CURSOR FOR
SELECT id, filename, pos, finish
FROM @logfiles
ORDER BY id

SET @stopped = 0

OPEN logfiles
FETCH NEXT FROM logfiles INTO @logid, @logfile, @logpos, @logfinish

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @sql = 'RESTORE LOG ' + QUOTENAME(@RestoreAs) + ' FROM DISK = ''' + @logfile + ''' WITH FILE = ' + CAST(@logpos AS varchar(10))

	IF EXISTS (SELECT id FROM @logfiles WHERE id > @logid) OR @NoRecovery = 1 --Are there more log files, or has the user requested WITH NORECOVERY?
		SET @sql = @sql + ', NORECOVERY'
	ELSE IF @Standby IS NOT NULL --No more logs, but has the user requested WITH STANDBY?
		SET @sql = @sql + ', STANDBY = ''' + REPLACE(@Standby, '''', '''''') + ''''
	ELSE --Nope, run recovery.
		SET @sql = @sql + ', RECOVERY'

	--Check if this log file covers the target @StopAt time, and add STOPAT.
	IF @StopAt <= @logfinish
	BEGIN
		SET @sql = @sql + ', STOPAT = ''' + REPLACE(CONVERT(varchar(50), @StopAt, 121), '-', '') + ''''
		SET @stopped = 1
	END

	--Add user-supplied WITH options.
	IF @RestoreLogWith IS NOT NULL
		SET @sql = @sql + ', ' + @RestoreLogWith

	RAISERROR('%s', 0, 1, @sql) WITH NOWAIT
	IF @WhatIf = 0
	BEGIN
		EXEC (@sql)
		IF @@ERROR <> 0
		BEGIN
			RAISERROR('Failed to restore transaction log backup. The preceding error messages, if any, may indicate the cause of failure.', 16, 3) WITH NOWAIT
			CLOSE logfiles
			DEALLOCATE logfiles
			RETURN 3
		END
	END

	FETCH NEXT FROM logfiles INTO @logid, @logfile, @logpos, @logfinish
END

CLOSE logfiles
DEALLOCATE logfiles

