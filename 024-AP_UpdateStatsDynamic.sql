USE [DBA]
GO

/****** Object:  StoredProcedure [dbo].[AP_UpdateStatsDynamic]    Script Date: 12/16/2011 12:00:30 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*
-- ignore date
EXEC SP_MSForEachDB
'
IF(''?'' NOT IN (''master'',''model'',''tempdb'',''msdb''))
	EXEC dbo.AP_UpdateStatsDynamic
		@DBName = ''?'', -- VARCHAR(255)
		@PercentChange = .2, -- DECIMAL (5,5)
		@MaxAgeInDays = 999, -- INT
		@VerboseLogging = 0, -- BIT
		@ColumnStatsOnly = 0 -- BIT
'

select name,is_auto_update_stats_on,is_auto_update_stats_async_on from master.sys.databases 

*/

/*
This will disable auto-update stats each time its run



This SP will do a fullscan on any statistics that have:
	 - Percent changed is greater then what you specify 
	 - Age is greater then what you specify



3/10/2008 Created by Ben Sala to dynamically update stats as needed.
3/18/2009 fixed divide by 0 bug when stats has 0 rows.
3/23/2009 CLC altered this so it can handle db names with SPACES in them
3/30/2009 Added the use of a logging table
06/22/2010 AJM - Added logic to account for multiple schemas
06/29/2010 BS - Removed DB name from SP to allow for differant database names.
*/


CREATE PROC [dbo].[AP_UpdateStatsDynamic]
	(
	@DBName VARCHAR(255)
	, @PercentChange DECIMAL (5,5) = .2
	, @MaxAgeInDays INT = 9999
	, @VerboseLogging BIT = 0
	, @ColumnStatsOnly BIT = 0
	, @SessionDT DATETIME = NULL
	)
AS

SET NOCOUNT ON

IF OBJECT_ID('dbo.StatisticsLog') IS NULL
BEGIN
	CREATE TABLE [dbo].[StatisticsLog](
		[StatisticsLogID] [int] IDENTITY(1,1) NOT NULL,
		[DatabaseName] [varchar](200) NOT NULL,
		[TableName] [varchar](255) NOT NULL,
		[StatsList] [varchar](MAX) NOT NULL,
		[StartDate] [datetime] NOT NULL,
		[EndDate] [datetime] NULL,
		[SessionDT] [datetime] NOT NULL,
		ErrorDescription VARCHAR(MAX) NULL,
	 CONSTRAINT [PK_StatisticsLog] PRIMARY KEY CLUSTERED 
		([StatisticsLogID] ASC)
	)
	ALTER TABLE [dbo].[StatisticsLog] ADD  DEFAULT (getdate()) FOR [StartDate]
	CREATE INDEX IX_SessionDT ON dbo.StatisticsLog (SessionDT)
END


	

DECLARE @SQL NVARCHAR(MAX)
	, @WorkingID INT
	, @LogID INT
	, @ErrMessage VARCHAR(MAX)


IF(@SessionDT IS NULL)
	SELECT @SessionDT = GETDATE()


CREATE TABLE #Stats(
	ID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED
	, Tablename VARCHAR(255) NOT NULL 
	, StatsName VARCHAR(255) NOT NULL 
	, LastUpdated VARCHAR(255) NULL
	, RowsModified INT NOT NULL
	, IsIndex BIT NOT NULL
	, Row_Count BIGINT NOT NULL
	)

CREATE TABLE #StatsToBeProccessed (
	ID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED
	, TableName VARCHAR(255)
	, StatsList VARCHAR(MAX)
	)

SELECT @SQL = '
USE [@DBName]
-- Disable auto-stats
IF(SELECT is_auto_update_stats_on 
	FROM sys.databases
		WHERE NAME = ''@DBName''
	) = 1
BEGIN
	RAISERROR(''Auto stats being disabled for DB: @DBName'',0,1)
	ALTER DATABASE [@DBName] SET AUTO_UPDATE_STATISTICS OFF WITH NO_WAIT
END


-- get stats and row counts
INSERT INTO #Stats (
	Tablename,
	StatsName,
	LastUpdated,
	RowsModified,
	IsIndex,
	Row_Count
) 
SELECT Tablename = OBJECT_SCHEMA_NAME(SI.ID)+''].[''+OBJECT_NAME(SI.ID)
  , SI.NAME
  , statsdate = STATS_DATE(SI.ID, SI.IndID)
  , SI.rowmodctr
  --, PercentChange = SI.RowModCtr / SI.Rows
  , IsIndex = 
	CASE 
	  WHEN I.NAME IS NOT NULL THEN 1
	  ELSE 0
	END
  , Row_Count = SUM(PS.Row_count)
	FROM sys.sysindexes SI
	INNER JOIN sys.dm_db_partition_stats PS
	  ON SI.ID = PS.OBJECT_ID
	  AND PS.Index_ID < 2
	INNER JOIN sys.sysobjects SO
	  ON SI.ID = SO.ID
	  AND SO.Xtype <> ''S''
	LEFT JOIN sys.indexes I
	  ON SI.IndID = I.Index_ID
	  AND SI.ID = I.OBJECT_ID
	  	WHERE SI.IndID <> 0
	  	  AND SI.rowmodctr > 0
			GROUP BY OBJECT_SCHEMA_NAME(SI.ID)+''].[''+OBJECT_NAME(SI.ID)
			  , SI.NAME
			  , STATS_DATE(SI.ID, SI.IndID)
			  , Si.Rowmodctr
			  , CASE 
				  WHEN I.NAME IS NOT NULL THEN 1
				  ELSE 0
				END
'
SELECT @SQL = REPLACE(@SQL,'@DBName',@DBName)

EXEC SP_executesql @SQL



IF(@VerboseLogging = 1)
	SELECT * 
	, RowsModified / CAST(Row_Count AS NUMERIC(18,5))
	, DATEADD(DAY, -@MaxAgeInDays, GETDATE())
		FROM #Stats
			ORDER BY Tablename, StatsName


DELETE #Stats
	WHERE row_count = 0

DELETE #Stats
	WHERE RowsModified / CAST(Row_Count AS NUMERIC(18,5)) < @PercentChange
	  AND LastUpdated > DATEADD(DAY, -@MaxAgeInDays, GETDATE()) 



IF(@ColumnStatsOnly = 1)		  
	DELETE #Stats
		WHERE IsIndex = 1

IF(@VerboseLogging = 1)
	SELECT *
	, RowsModified / CAST(Row_Count AS NUMERIC(18,5))
	, DATEADD(DAY, -@MaxAgeInDays, GETDATE()) 
		FROM #Stats
			ORDER BY Tablename, StatsName




INSERT INTO #StatsToBeProccessed (Tablename, StatsList)
SELECT DISTINCT TableName
, StatsList = STUFF((SELECT ',[' + SI.StatsName + ']'
			FROM #Stats SI
				WHERE SI.TableName= SO.TableName
			FOR XML PATH('')
			), 1,1,'')
	FROM #Stats SO
		ORDER BY TableName



SELECT TOP 1 @WorkingID = ID
	FROM #StatsToBeProccessed

WHILE EXISTS(SELECT * FROM #StatsToBeProccessed)
BEGIN
	SELECT @SQL = 'USE [@DBName] UPDATE STATISTICS [' + TableName + '] (' + StatsList + ') WITH FULLSCAN'
		FROM #StatsToBeProccessed
			WHERE ID = @WorkingID
	SELECT @SQL = REPLACE(@SQL,'@DBName',@DBName)
	
	IF(@VerboseLogging > 0)
		RAISERROR('%s',0,1,@SQL) WITH NOWAIT

	INSERT INTO dbo.StatisticsLog (
		DatabaseName,
		TableName,
		StatsList,
		StartDate,
		SessionDT
	) 
	SELECT DatabaseName = @DBName
	, TableName
	, StatsList
	, StartDate = GETDATE()
	, SessionDT = @SessionDT
		FROM #StatsToBeProccessed
			WHERE ID = @WorkingID
	SELECT @LogID = SCOPE_IDENTITY()


	BEGIN TRY
		EXEC SP_ExecuteSQL @SQL

		UPDATE dbo.StatisticsLog
			SET EndDate = GETDATE()
				WHERE StatisticsLogID = @LogID

		
	END TRY
	BEGIN CATCH
		SELECT @ErrMessage = ERROR_MESSAGE()
		-- failed to rebuild an index online or offline, aborting index operation and going to the next. (email will be sent)
		UPDATE dbo.StatisticsLog
			SET 
			ErrorDescription = ' ErrorNumber: ' + CAST(ERROR_NUMBER() AS VARCHAR(50))
							 + ' ErrorSeverity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(50))
							 + ' Error: ' + @ErrMessage
							 + ' @command: ' + @SQL
				WHERE StatisticsLogID = @LogID	
				
		RAISERROR('Error in command.  Error: %s.  Command: %s',0,1,@ErrMessage,@SQL)
	END CATCH
	
	DELETE #StatsToBeProccessed
			WHERE ID = @WorkingID

	SELECT TOP 1 @WorkingID = ID
		FROM #StatsToBeProccessed
END

DROP TABLE #Stats
DROP TABLE #StatsToBeProccessed

DELETE dbo.StatisticsLog
	WHERE SessionDT < DATEADD(YEAR,-1,GETDATE())





GO


