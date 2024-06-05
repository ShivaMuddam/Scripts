USE master
GO

CREATE TABLE #numprocs
(
[Index] INT,
[Name] VARCHAR(200),
Internal_Value VARCHAR(50),
Character_Value VARCHAR(200)
)

DECLARE @BASEPATH VARCHAR(200)
DECLARE @PATH VARCHAR(200)
DECLARE @SQL_SCRIPT VARCHAR(500)
DECLARE @CORES INT
DECLARE @FILECOUNT INT
DECLARE @SIZE INT
DECLARE @GROWTH INT
DECLARE @ISPERCENT INT

INSERT INTO #numprocs
EXEC xp_msver

SELECT @CORES = Internal_Value FROM #numprocs WHERE [Index] = 16
PRINT @CORES

SET @BASEPATH = (select SUBSTRING(physical_name, 1, CHARINDEX(N'tempdev.mdf', LOWER(physical_name)) - 1) DataFileLocation
FROM master.sys.master_files
WHERE database_id = 2 and FILE_ID = 1)
PRINT @BASEPATH

SET @FILECOUNT = (SELECT COUNT(*)
FROM master.sys.master_files
WHERE database_id = 2 AND TYPE_DESC = 'ROWS')

SELECT @SIZE = size FROM master.sys.master_files WHERE database_id = 2 AND FILE_ID = 1
SET @SIZE = @SIZE / 128

SELECT @GROWTH = growth FROM master.sys.master_files WHERE database_id = 2 AND FILE_ID = 1
SELECT @ISPERCENT = is_percent_growth FROM master.sys.master_files WHERE database_id = 2 AND FILE_ID = 1

WHILE @CORES > @FILECOUNT
BEGIN
SET @SQL_SCRIPT = 'ALTER DATABASE tempdb
ADD FILE
(
FILENAME = ''' + @BASEPATH + 'tempdev' + RTRIM(CAST(@CORES as CHAR)) + '.ndf'',
NAME = tempdev' + RTRIM(CAST(@CORES as CHAR)) + ',
SIZE = ' + RTRIM(CAST(@SIZE as CHAR)) + 'MB,
FILEGROWTH = ' + RTRIM(CAST(@GROWTH as CHAR))
IF @ISPERCENT > 0
SET @SQL_SCRIPT = @SQL_SCRIPT + '%'
SET @SQL_SCRIPT = @SQL_SCRIPT + ')'

EXEC(@SQL_SCRIPT)
SET @CORES = @CORES - 1
END
GO
DROP TABLE #numprocs


--USE tempdb
--DBCC SHRINKFILE( 'tempdev32' , EMPTYFILE);

--USE Master
--ALTER DATABASE tempdb REMOVE FILE tempdev32

