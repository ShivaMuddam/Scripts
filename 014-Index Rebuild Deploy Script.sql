USE [DBA]

IF ((OBJECT_ID('dbo.ParseStringToTable')) > 0)
	DROP FUNCTION dbo.ParseStringToTable

IF ((OBJECT_ID('IndexRebuild.Status')) > 0)
	DROP TABLE IndexRebuild.Status

IF ((OBJECT_ID('IndexRebuild.StatusHistory')) > 0)
	DROP TABLE IndexRebuild.StatusHistory

IF ((OBJECT_ID('IndexRebuild.Log')) > 0)
	DROP TABLE IndexRebuild.Log

IF ((OBJECT_ID('IndexRebuild.LogHistory')) > 0)
	DROP TABLE IndexRebuild.LogHistory

IF ((OBJECT_ID('IndexRebuild.LogMessage')) > 0)
	DROP PROCEDURE IndexRebuild.LogMessage

IF ((OBJECT_ID('IndexRebuild.GetIndexesContainingLOBData')) > 0)
	DROP PROCEDURE IndexRebuild.GetIndexesContainingLOBData

IF ((OBJECT_ID('IndexRebuild.GetIndexFragmentation')) > 0)
	DROP PROCEDURE IndexRebuild.GetIndexFragmentation

IF ((OBJECT_ID('IndexRebuild.RebuildIndexes')) > 0)
	DROP PROCEDURE IndexRebuild.RebuildIndexes

IF SCHEMA_ID('IndexRebuild') IS NOT NULL
	DROP SCHEMA IndexRebuild

IF EXISTS(SELECT job_id FROM msdb.dbo.sysjobs WHERE name = 'DBA Job: ADDCFF08-ECF2-4739-AB04-0854A462E13B')
	EXECUTE msdb.dbo.sp_delete_job @job_name = N'DBA Job: ADDCFF08-ECF2-4739-AB04-0854A462E13B' 

IF EXISTS(SELECT job_id FROM msdb.dbo.sysjobs WHERE name = 'DBA Job: Rebuild Indexes')
	EXECUTE msdb.dbo.sp_delete_job @job_name = N'DBA Job: Rebuild Indexes' 
USE [DBA]
GO

/****** Object:  Schema [IndexRebuild]    Script Date: 11/30/2011 10:42:12 ******/
CREATE SCHEMA [IndexRebuild] AUTHORIZATION [dbo]
GO
USE [DBA]
GO

/****** Object:  UserDefinedFunction [dbo].[ParseStringToTable]    Script Date: 11/30/2011 10:17:36 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE FUNCTION [dbo].[ParseStringToTable](@DelimitedString VARCHAR(MAX))
RETURNS @Values TABLE
(
	Value VARCHAR(MAX)
)
AS 
BEGIN
 
	IF(REPLACE(@DelimitedString, ' ', '') != '' AND @DelimitedString IS NOT NULL)
	BEGIN

		--Replace commas with semi-colons; remove spaces; add semi-colon to end
		SET @DelimitedString = REPLACE(@DelimitedString, ',', ';')
		SET @DelimitedString = REPLACE(@DelimitedString, ' ', '')
		IF(RIGHT(@DelimitedString, 1) != ';')
			SET @DelimitedString = @DelimitedString + ';'
	
		WHILE(LEN(@DelimitedString) > 0)
		BEGIN
			--Parse out the database
			INSERT INTO @Values
				SELECT 
					SUBSTRING(@DelimitedString, 0, CHARINDEX(';', @DelimitedString, 0))
		
			--Remove the database from string	
			SET @DelimitedString = REPLACE(@DelimitedString, SUBSTRING(@DelimitedString, 0, CHARINDEX(';', @DelimitedString, 0)) + ';', '')
		END

	END
 
RETURN
END





GO

EXEC sys.sp_addextendedproperty @name=N'Version', @value=N'1.0' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'FUNCTION',@level1name=N'ParseStringToTable'
GO


USE [DBA]
GO

/****** Object:  Table [IndexRebuild].[Status]    Script Date: 11/30/2011 10:21:09 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [IndexRebuild].[Status](
	[DatabaseName] [varchar](100) NOT NULL,
	[SchemaName] [varchar](100) NOT NULL,
	[TableName] [varchar](100) NOT NULL,
	[IndexName] [varchar](200) NOT NULL,
	[ProcedureRunDate] [datetime] NOT NULL,
	[IndexType] [varchar](50) NOT NULL,
	[AllocUnitTypeDescription] [varchar](50) NOT NULL,
	[ContainsLOBData] [bit] NOT NULL,
	[ContainsOverflowData] [bit] NOT NULL,
	[Before_AvgFragmentationInPercent] [decimal](18, 2) NOT NULL,
	[After_AvgFragmentationInPercent] [decimal](18, 2) NULL,
	[Before_FillFactor] [int] NOT NULL,
	[After_FillFactor] [int] NULL,
	[Before_IndexPageCount] [int] NOT NULL,
	[After_IndexPageCount] [nchar](10) NULL,
	[Before_IndexDepth] [int] NOT NULL,
	[After_IndexDepth] [nchar](10) NULL,
	[Before_FragmentCount] [int] NOT NULL,
	[After_FragmentCount] [nchar](10) NULL,
	[WasRebuilt] [bit] NOT NULL,
	[WasReorganized] [bit] NOT NULL,
	[LastRebuildReorgDate] [datetime] NULL,
	[LastRebuildReorgError] [varchar](max) NULL,
	[NotRebuiltReorgReason] [varchar](max) NULL,
	[LastRebuildReorgDurationSeconds] [int] NULL,
 CONSTRAINT [PK_IndexRebuildStatus] PRIMARY KEY CLUSTERED 
(
	[DatabaseName] ASC,
	[SchemaName] ASC,
	[TableName] ASC,
	[IndexName] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

ALTER TABLE [IndexRebuild].[Status] ADD  CONSTRAINT [DF_IndexRebuildStatus_WasRebuilt]  DEFAULT ((0)) FOR [WasRebuilt]
GO

ALTER TABLE [IndexRebuild].[Status] ADD  CONSTRAINT [DF_IndexRebuildStatus_WasReorganized]  DEFAULT ((0)) FOR [WasReorganized]
GO


USE [DBA]
GO

/****** Object:  Table [IndexRebuild].[StatusHistory]    Script Date: 11/30/2011 10:22:00 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [IndexRebuild].[StatusHistory](
	[DatabaseName] [varchar](100) NOT NULL,
	[SchemaName] [varchar](100) NOT NULL,
	[TableName] [varchar](100) NOT NULL,
	[IndexName] [varchar](200) NOT NULL,
	[ProcedureRunDate] [datetime] NOT NULL,
	[IndexType] [varchar](50) NOT NULL,
	[AllocUnitTypeDescription] [varchar](50) NOT NULL,
	[ContainsLOBData] [bit] NOT NULL,
	[ContainsOverflowData] [bit] NOT NULL,
	[Before_AvgFragmentationInPercent] [decimal](18, 2) NOT NULL,
	[After_AvgFragmentationInPercent] [decimal](18, 2) NULL,
	[Before_FillFactor] [int] NOT NULL,
	[After_FillFactor] [int] NULL,
	[Before_IndexPageCount] [int] NOT NULL,
	[After_IndexPageCount] [nchar](10) NULL,
	[Before_IndexDepth] [int] NOT NULL,
	[After_IndexDepth] [nchar](10) NULL,
	[Before_FragmentCount] [int] NOT NULL,
	[After_FragmentCount] [nchar](10) NULL,
	[WasRebuilt] [bit] NOT NULL,
	[WasReorganized] [bit] NOT NULL,
	[LastRebuildReorgDate] [datetime] NULL,
	[LastRebuildReorgError] [varchar](max) NULL,
	[NotRebuiltReorgReason] [varchar](max) NULL,
	[LastRebuildReorgDurationSeconds] [int] NULL,
 CONSTRAINT [PK_IndexRebuildStatus_History] PRIMARY KEY CLUSTERED 
(
	[DatabaseName] ASC,
	[SchemaName] ASC,
	[TableName] ASC,
	[IndexName] ASC,
	[ProcedureRunDate] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO


USE [DBA]
GO

/****** Object:  Table [IndexRebuild].[Log]    Script Date: 11/30/2011 10:20:25 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [IndexRebuild].[Log](
	[LogID] [int] IDENTITY(1,1) NOT NULL,
	[Message] [varchar](3000) NOT NULL,
	[DateLogged] [datetime] NOT NULL,
	[ElapsedSecondsSinceLastMessage] [int] NULL,
	[ElapsedMillisecondsSinceLastMessage] [int] NULL,
 CONSTRAINT [PK_IndexRebuildLog] PRIMARY KEY CLUSTERED 
(
	[LogID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO


USE [DBA]
GO

/****** Object:  Table [IndexRebuild].[LogHistory]    Script Date: 11/30/2011 10:20:59 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [IndexRebuild].[LogHistory](
	[LogID] [int] NOT NULL,
	[ProcedureRunDate] [datetime] NOT NULL,
	[Message] [varchar](3000) NOT NULL,
	[DateLogged] [datetime] NOT NULL,
	[ElapsedSecondsSinceLastMessage] [int] NULL,
	[ElapsedMillisecondsSinceLastMessage] [int] NULL,
 CONSTRAINT [PK_IndexRebuildLog_History] PRIMARY KEY CLUSTERED 
(
	[LogID] ASC,
	[ProcedureRunDate] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO


USE [DBA]
GO

/****** Object:  StoredProcedure [IndexRebuild].[LogMessage]    Script Date: 11/30/2011 10:18:26 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [IndexRebuild].[LogMessage]
	@LoggingEnabled BIT,
	@Message VARCHAR(3000)
AS
BEGIN
	
	/*===========================================
	-- Create variables
	===========================================*/

	DECLARE @MaxLogDate DATETIME

	/*===========================================
	-- Ensure that logging is enabled
	===========================================*/
	IF(@LoggingEnabled = 1)
	BEGIN
		
		/*===========================================
		-- Print the message
		===========================================*/
		PRINT @Message

		/*===========================================
		-- If there is existing data
		===========================================*/
		IF(SELECT COUNT(*) FROM IndexRebuild.Log) > 0
		BEGIN
			
			SELECT @MaxLogDate = MAX(DateLogged) FROM IndexRebuild.Log
			
			INSERT INTO IndexRebuild.Log(Message, DateLogged, ElapsedSecondsSinceLastMessage, ElapsedMillisecondsSinceLastMessage)
			VALUES(@Message, GETDATE(), DATEDIFF(SECOND, @MaxLogDate, GETDATE()), DATEDIFF(MILLISECOND, @MaxLogDate, GETDATE()))

		END
		
		/*===========================================
		-- Else no existing data
		===========================================*/
		ELSE
		BEGIN
			INSERT INTO IndexRebuild.Log(Message, DateLogged)
			VALUES(@Message, GETDATE())
		END
		
	END

END


GO

EXEC sys.sp_addextendedproperty @name=N'Version', @value=N'2.0' , @level0type=N'SCHEMA',@level0name=N'IndexRebuild', @level1type=N'PROCEDURE',@level1name=N'LogMessage'
GO


USE [DBA]
GO

/****** Object:  StoredProcedure [IndexRebuild].[GetIndexesContainingLOBData]    Script Date: 11/30/2011 10:19:54 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [IndexRebuild].[GetIndexesContainingLOBData]
(@DatabaseName VARCHAR(500))
AS 
BEGIN
	
	DECLARE @DynamicSQL VARCHAR(MAX)
	
	SET @DynamicSQL = 
	'USE [' + @DatabaseName + ']' + '
	
	SELECT
		DISTINCT DB_Name() AS DatabaseName, SchemaName, TableName, IndexName
	FROM
	(
		SELECT
			DISTINCT
			ta.name AS TableName,
			s.name AS SchemaName,
			i.name AS IndexName,
			CASE WHEN cic.name IS NULL THEN c.name ELSE cic.name END AS ColumnName,
			CASE WHEN cic.name IS NULL THEN ctype.name ELSE cictype.name END AS ColumnType,
			CASE WHEN cic.name IS NULL THEN c.max_length ELSE cic.max_length END AS ColumnMaxLength
		FROM sys.indexes i
		JOIN sys.index_columns ic
			ON i.index_id = ic.index_id
			AND ic.object_id = i.object_id
		JOIN sys.tables ta
			ON i.object_id = ta.object_id
		JOIN sys.schemas s
			ON ta.schema_id = s.schema_id
		JOIN sys.columns c
			ON ic.column_id = c.column_id
			AND c.object_id = ta.object_id
		LEFT JOIN sys.columns cic --(Clustered Index Columns) sys.index_columns does not include all columns for clustered indexes
			ON i.object_id = cic.object_id
			AND i.type = 1 --Clustered Index type
		LEFT JOIN sys.types ctype
			ON c.system_type_id = ctype.system_type_id
		LEFT JOIN sys.types cictype
			ON cic.system_type_id = cictype.system_type_id
	)IndexColumns 
	WHERE 
	(
		(
			IndexColumns.ColumnType IN
			(
				''VARCHAR'',
				''NVARCHAR''
			)
			AND IndexColumns.ColumnMaxLength = -1
		)
		
		OR
		(
			IndexColumns.ColumnType IN
			(
				''Text'',
				''NText'',
				''Image'',
				''XML'',
				''Varbinary''
			)
		)
	)'
	
	EXEC (@DynamicSQL)
	
END


GO

EXEC sys.sp_addextendedproperty @name=N'Version', @value=N'2.0' , @level0type=N'SCHEMA',@level0name=N'IndexRebuild', @level1type=N'PROCEDURE',@level1name=N'GetIndexesContainingLOBData'
GO


USE [DBA]
GO

/****** Object:  StoredProcedure [IndexRebuild].[GetIndexFragmentation]    Script Date: 11/30/2011 10:18:53 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [IndexRebuild].[GetIndexFragmentation]
	@DatabaseName VARCHAR(500)
AS
BEGIN
	
/*===========================================
-- Create temp tables and variables
===========================================*/ 
	
	DECLARE @DynamicSQL VARCHAR(MAX)
	
	IF(OBJECT_ID('tempdb..#IndexesContainingLOBData')>0)
		DROP TABLE #IndexesContainingLOBData
	CREATE TABLE #IndexesContainingLOBData
	(
		DatabaseName VARCHAR(500),
		SchemaName VARCHAR(500),
		TableName VARCHAR(500),
		IndexName VARCHAR(500)
	)

/*===========================================
-- Query fragmentation
===========================================*/ 

	SET @DynamicSQL = 
	'
		USE [' + @DatabaseName + ']
		
		SELECT 
			DB_NAME(PS.database_id) AS DBName,
			S.name  AS SchemaName,
			O.name AS TableName,
			b.name AS IndexName,
			ps.index_type_desc AS IndexType,
			ps.alloc_unit_type_desc AllocUnitTypeDescription,
			ps.avg_fragmentation_in_percent AS FragmentationPercent,
			b.fill_factor AS CurrentFillFactor,
			ps.page_count AS IndexPageCount,
			ps.index_depth AS IndexDepth,
			ps.fragment_count AS FragmentCount
		FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, NULL) AS ps
		INNER JOIN sys.indexes AS b ON ps.OBJECT_ID = b.OBJECT_ID AND ps.index_id = b.index_id
		INNER JOIN sys.objects O  ON PS.object_id = O.object_id
		INNER JOIN sys.schemas S ON S.schema_id = O.schema_id 
		WHERE PS.index_type_desc IN (''CLUSTERED INDEX'',''NONCLUSTERED INDEX'') -- Only get clustered and nonclustered indexes
		AND b.is_hypothetical = 0 -- Only real indexes
		AND O.type_desc = ''USER_TABLE'' -- Restrict to user tables
		ORDER BY ps.avg_fragmentation_in_percent DESC
	'
	
	INSERT INTO ##IndexFragmentation (DatabaseName, SchemaName, TableName, IndexName, IndexType,AllocUnitTypeDescription, 
	AvgFragmentationInPercent, IndexFillFactor, IndexPageCount, IndexDepth, FragmentCount)
		EXEC (@DynamicSQL)
		
/*===========================================
-- Determine if LOB/Overflow Data Exists
===========================================*/

	--Find indexes with LOB data
	INSERT INTO #IndexesContainingLOBData
		EXEC IndexRebuild.GetIndexesContainingLOBData @DatabaseName
	UPDATE i
		SET ContainsLOBData = 1
	FROM ##IndexFragmentation i
	JOIN #IndexesContainingLOBData lob
		ON i.DatabaseName = lob.DatabaseName
		AND i.SchemaName = lob.SchemaName
		AND i.TableName = lob.TableName
		AND i.IndexName = lob.IndexName
	
	--Find indexes with overflow data
	UPDATE i
		SET ContainsLOBData = 1
	FROM ##IndexFragmentation i
	JOIN
	(
		SELECT
			*
		FROM ##IndexFragmentation
		WHERE AllocUnitTypeDescription = 'ROW_OVERFLOW_DATA'
	)lobd 
		ON i.DatabaseName = lobd.DatabaseName
			AND i.SchemaName = lobd.SchemaName
			AND i.TableName = lobd.TableName
			AND i.IndexName = lobd.IndexName
				
/*===========================================
-- Remove Fragmentation Duplicates
===========================================*/ 

	/*Delete any rows that have LOB_DATA & ROW_OVERFLOW_DATA that also have IN_ROW_DATA.
	(The thought behind this, is that it is possible for an index to just have an LOB Data row in 
		sys.dm_db_index_physical_stats, and not a IN_ROW_DATA row. So obviously, you dont want to
		delete an index from this list that needs to be rebuilt, even though it is composed of only
		LOB data)*/
	DELETE FROM ##IndexFragmentation
	FROM ##IndexFragmentation i
	JOIN
	(
		SELECT
			DatabaseName, SchemaName, TableName, IndexName
		FROM ##IndexFragmentation
		WHERE AllocUnitTypeDescription = 'IN_ROW_DATA'
	) ird ON i.DatabaseName = ird.DatabaseName
		AND i.SchemaName = ird.SchemaName
		AND i.TableName = ird.TableName
		AND i.IndexName = ird.IndexName
	WHERE i.AllocUnitTypeDescription <> 'IN_ROW_DATA'

	/*Delete any remaining duplicate rows 
	("...one row is returned for each level of the B-tree in each partition.")*/
	DELETE FROM i
	FROM ##IndexFragmentation i
	WHERE i.ID IN
	(
		SELECT
			ID
		FROM
		(
			SELECT
				ROW_NUMBER() OVER(PARTITION BY DatabaseName, SchemaName, TableName, IndexName ORDER BY IndexName) AS RowNumber,
				*
			FROM ##IndexFragmentation
		)rn
		WHERE rn.RowNumber > 1
	)
	
/*===========================================
-- Cleanup
===========================================*/ 

	DROP TABLE #IndexesContainingLOBData

END


GO

EXEC sys.sp_addextendedproperty @name=N'Version', @value=N'2.0' , @level0type=N'SCHEMA',@level0name=N'IndexRebuild', @level1type=N'PROCEDURE',@level1name=N'GetIndexFragmentation'
GO


USE [DBA]
GO

/****** Object:  StoredProcedure [IndexRebuild].[RebuildIndexes]    Script Date: 11/30/2011 10:18:00 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [IndexRebuild].[RebuildIndexes] 
	@FragmentationPercentThreshold INT = 10,
	@FillFactor INT = 80,
	@PriorityDatabases VARCHAR(MAX) = NULL,
	@ExcludedDatabases VARCHAR(MAX) = NULL,
	@LoggingEnabled BIT = 1,
	@ReorganizeOfflineIndexes BIT = 1,
	@ReQueryFragmentationAfterRebuild BIT = 1
AS
BEGIN

/*===========================================
-- Transaction settings
===========================================*/ 

	SET NOCOUNT ON
	SET DEADLOCK_PRIORITY LOW
	
/*===========================================
-- Ensure proper objects exist, and the
-- parameters are set correctly
===========================================*/ 

	IF ((SELECT OBJECT_ID('DBA.IndexRebuild.Status')) IS NULL)
	BEGIN
		RAISERROR('Missing table DBA.IndexRebuild.Status.  This table needs to exist for rebuilds to function.',16,1)
		RETURN -1
	END

	IF ((SELECT OBJECT_ID('DBA.dbo.ParseStringToTable')) IS NULL)
	BEGIN
		RAISERROR('Missing function DBA.dboParseStringToTable.  This function needs to exist for rebuilds to function.',16,1)
		RETURN -1
	END
	
	IF ((SELECT OBJECT_ID('DBA.IndexRebuild.GetIndexesContainingLOBData')) IS NULL)
	BEGIN
		RAISERROR('Missing stored procedure DBA.IndexRebuild.GetIndexesContainingLOBData.  This stored procedure needs to exist for rebuilds to function.',16,1)
		RETURN -1
	END
	
	IF ((SELECT OBJECT_ID('DBA.IndexRebuild.GetIndexFragmentation')) IS NULL)
	BEGIN
		RAISERROR('Missing stored procedure DBA.IndexRebuild.GetIndexFragmentation.  This stored procedure needs to exist for rebuilds to function.',16,1)
		RETURN -1
	END

	IF @LoggingEnabled = 1
	BEGIN

		IF ((SELECT OBJECT_ID('DBA.IndexRebuild.Log')) IS NULL)
		BEGIN
			RAISERROR('Missing table DBA.IndexRebuild.Log.  This table needs to exist for logging to function.',16,1)
			RETURN -1
		END
		
		IF ((SELECT OBJECT_ID('DBA.IndexRebuild.LogHistory')) IS NULL)
		BEGIN
			RAISERROR('Missing table DBA.IndexRebuild.LogHistory.  This table needs to exist for logging to function.',16,1)
			RETURN -1
		END
		
		IF ((SELECT OBJECT_ID('DBA.IndexRebuild.LogMessage')) IS NULL)
		BEGIN
			RAISERROR('Missing stored procedure DBA.IndexRebuild.LogMessage.  This stored procedure needs to exist for logging to function.',16,1)
			RETURN -1
		END
	END

/*===========================================
-- Create temp tables and variables
===========================================*/ 
	
	--Variables
	DECLARE 
		@UserDatabase VARCHAR(500),
		@DynamicSQL VARCHAR(MAX),
		@TableName VARCHAR(500),
		@TableSchemaName VARCHAR(50),
		@IndexName VARCHAR(500),
		@ContainsLOBData BIT,
		@Timer1 DATETIME,
		@Timer2 DATETIME,
		@MessageToLog VARCHAR(MAX),
		@ProcedureRunDate DATETIME

	SET @ProcedureRunDate = GETDATE()

	/*This global temp table is required due to the limitation 
	of calling insert/exec in a nested fashion. All stored 
	procedures (except IndexRebuild.GetIndexFragmentation)
	are global temp table agnostic.*/
	IF(OBJECT_ID('tempdb..##IndexFragmentation')>0)
		DROP TABLE ##IndexFragmentation
	CREATE TABLE ##IndexFragmentation
	(
		ID INT PRIMARY KEY IDENTITY,
		DatabaseName VARCHAR(500),
		SchemaName VARCHAR(500),
		TableName VARCHAR(500),
		IndexName VARCHAR(500),
		IndexType VARCHAR(50),
		AllocUnitTypeDescription VARCHAR(50),
		ContainsLOBData BIT DEFAULT 0,
		ContainsOverflowData BIT DEFAULT 0,
		AvgFragmentationInPercent DECIMAL(18,2),
		IndexFillFactor INT,
		IndexPageCount INT,
		IndexDepth INT,
		FragmentCount INT
	)

/*===========================================
-- Truncate IndexRebuildLog table
===========================================*/ 

	SET @MessageToLog = 'Truncating table DBA.IndexRebuild.Log'
	EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog

	--Insert into the history table before truncating current table
	INSERT INTO IndexRebuild.LogHistory 
		SELECT 
			LogID,
			(SELECT MIN(DateLogged) FROM IndexRebuild.Log) AS ProcedureRunDate,
			Message,
			DateLogged,
			ElapsedSecondsSinceLastMessage,
			ElapsedMillisecondsSinceLastMessage
		FROM IndexRebuild.Log

	TRUNCATE TABLE IndexRebuild.Log
	DBCC CHECKIDENT ('IndexRebuild.Log', RESEED, 1)

/*===========================================
-- Truncate IndexRebuild.Status table
===========================================*/ 
	
	SET @MessageToLog = 'Truncating table DBA.IndexRebuild.Status'
	EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog

	--Insert into the history table before truncating current table
	INSERT INTO IndexRebuild.StatusHistory
		SELECT * FROM IndexRebuild.Status

	TRUNCATE TABLE IndexRebuild.Status
	
/*===========================================
-- Get database cursor ready
===========================================*/ 
	IF CURSOR_STATUS('global','UserDatabasesCursor')>=-1
		DEALLOCATE UserDatabasesCursor
	DECLARE UserDatabasesCursor CURSOR FOR
		
		--Select the priority databases first
		SELECT
			Value
		FROM dbo.ParseStringToTable(@PriorityDatabases)
		
		UNION ALL
		
		SELECT
			name
		FROM sys.databases
		WHERE database_id > 4
		--WHERE name = 'dba'
		AND name NOT IN
		(
			--Already selected the priority databases
			SELECT
				Value
			FROM dbo.ParseStringToTable(@PriorityDatabases)
			
			UNION ALL
			
			--Do not select
			SELECT
				Value
			FROM dbo.ParseStringToTable(@ExcludedDatabases)
				
		)
		AND source_database_id IS NULL --Make sure the DB is not a snapshot

/*===========================================
-- Loop through databases
===========================================*/ 
	
	--Prepare cursor
	OPEN UserDatabasesCursor
	FETCH NEXT FROM UserDatabasesCursor INTO @UserDatabase

	SET @MessageToLog = 'Beginning to loop through user databases.'
	EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog

	--Loop through user databases
	WHILE @@FETCH_STATUS = 0
	BEGIN

		SET @MessageToLog = '==============Working on user database [' + @UserDatabase + ']=============='
		EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog

		/*===========================================
		-- Ensure database compatibility level is 
		-- correct
		===========================================*/ 
		
			IF((SELECT [compatibility_level] FROM sys.databases 
				WHERE name=@UserDatabase) < 90)
			BEGIN
			
				SET @MessageToLog = 'The compatability level is set to SQL 2000. Cannot rebuild/reorganize indexes.'
				EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog
				
				FETCH NEXT FROM UserDatabasesCursor INTO @UserDatabase
				CONTINUE
				
			END

		/*===========================================
		-- Get Index Fragmentation for database
		===========================================*/ 
			
			SET @MessageToLog = 'Querying index fragmentation for database.'
			EXEC IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog
			
			--Make sure global temp table is empty
			TRUNCATE TABLE ##IndexFragmentation
			
			--Populate ##IndexFragmentation
			EXEC IndexRebuild.GetIndexFragmentation @UserDatabase

		/*===========================================
		-- Remove indexes under threshold
		===========================================*/ 

			DELETE FROM ##IndexFragmentation
			WHERE AvgFragmentationInPercent < @FragmentationPercentThreshold

		/*===========================================
		-- Persist index list
		===========================================*/ 
			
			INSERT INTO IndexRebuild.Status 
			(DatabaseName, SchemaName, TableName, IndexName, ProcedureRunDate, IndexType, AllocUnitTypeDescription, ContainsLOBData,
			ContainsOverflowData,Before_AvgFragmentationInPercent, Before_FillFactor, Before_IndexPageCount,
			Before_IndexDepth, Before_FragmentCount) 
				SELECT DatabaseName, SchemaName, TableName, IndexName, @ProcedureRunDate, IndexType, 
					AllocUnitTypeDescription, ContainsLOBData,ContainsOverflowData,AvgFragmentationInPercent,
					IndexFillFactor, IndexPageCount,IndexDepth, FragmentCount 
				FROM ##IndexFragmentation

		/*===========================================
		-- Loop through indexes
		===========================================*/ 
		
			IF CURSOR_STATUS('global','IndexCursor')>=-1
				DEALLOCATE IndexCursor
			DECLARE IndexCursor CURSOR FOR
				SELECT
					TableName, SchemaName, IndexName, ContainsLOBData
				FROM IndexRebuild.Status
				WHERE DatabaseName = @UserDatabase
			
			OPEN IndexCursor	
			FETCH NEXT FROM IndexCursor INTO @TableName, @TableSchemaName, @IndexName, @ContainsLOBData
			
			SET @MessageToLog = 'Beginning to loop through indexes.'
			EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog

			WHILE @@FETCH_STATUS = 0
			BEGIN
		
				/*===========================================
				-- Can rebuild online
				===========================================*/ 
		
					IF(@ContainsLOBData = 0)
					BEGIN

						SET @DynamicSQL = 'ALTER INDEX [<IndexName>] ON [<DatabaseName>].[<SchemaName>].[<TableName>] REBUILD WITH (ONLINE=ON, SORT_IN_TEMPDB=ON, FILLFACTOR=<FillFactor>)'
						SET @DynamicSQL = REPLACE(@DynamicSQL, '<IndexName>', @IndexName)
						SET @DynamicSQL = REPLACE(@DynamicSQL, '<DatabaseName>', @UserDatabase)
						SET @DynamicSQL = REPLACE(@DynamicSQL, '<SchemaName>', @TableSchemaName)
						SET @DynamicSQL = REPLACE(@DynamicSQL, '<TableName>', @TableName)
						SET @DynamicSQL = REPLACE(@DynamicSQL, '<FillFactor>', CONVERT(VARCHAR(3), @FillFactor))
		
						BEGIN TRY
						
							SET @MessageToLog = 'Rebuilding index '+ @IndexName +'. (Syntax: ' + @DynamicSQL + ').'
							EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog

							SET @Timer1 = GETDATE()
							EXEC (@DynamicSQL)						
							SET @Timer2 = GETDATE()
						
							UPDATE IndexRebuild.Status
								SET 
									WasRebuilt = 1,
									LastRebuildReorgDurationSeconds = DATEDIFF(SECOND, @Timer1, @Timer2),
									LastRebuildReorgDate = GETDATE()
							WHERE DatabaseName = @UserDatabase
							AND SchemaName = @TableSchemaName
							AND TableName = @TableName
							AND IndexName = @IndexName
						
						END TRY

						BEGIN CATCH
					
							SET @MessageToLog = 'Error during rebuild ( ' + ERROR_MESSAGE() + ').'
							EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog

							UPDATE IndexRebuild.Status
								SET LastRebuildReorgError = ERROR_MESSAGE()
							WHERE DatabaseName = @UserDatabase
							AND SchemaName = @TableSchemaName
							AND TableName = @TableName
							AND IndexName = @IndexName
					
						END CATCH

					END

				/*================================================
				-- Can't rebuild online / Reorg for Offline is ON
				==================================================*/ 

					ELSE IF(@ContainsLOBData = 1 AND @ReorganizeOfflineIndexes = 1)
					BEGIN

						SET @DynamicSQL = 'ALTER INDEX [<IndexName>] ON [<DatabaseName>].[<SchemaName>].[<TableName>] REORGANIZE'
						SET @DynamicSQL = REPLACE(@DynamicSQL, '<IndexName>', @IndexName)
						SET @DynamicSQL = REPLACE(@DynamicSQL, '<DatabaseName>', @UserDatabase)
						SET @DynamicSQL = REPLACE(@DynamicSQL, '<SchemaName>', @TableSchemaName)
						SET @DynamicSQL = REPLACE(@DynamicSQL, '<TableName>', @TableName)
		
						BEGIN TRY
						
							SET @MessageToLog = 'Reorganizing index '+ @IndexName +'. (Syntax: ' + @DynamicSQL + ').'
							EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog

							SET @Timer1 = GETDATE()
							EXEC (@DynamicSQL)						
							SET @Timer2 = GETDATE()
						
							UPDATE IndexRebuild.Status
								SET 
									WasReorganized = 1,
									LastRebuildReorgDurationSeconds = DATEDIFF(SECOND, @Timer1, @Timer2),
									LastRebuildReorgDate = GETDATE()
							WHERE DatabaseName = @UserDatabase
							AND SchemaName = @TableSchemaName
							AND TableName = @TableName
							AND IndexName = @IndexName
						
						END TRY

						BEGIN CATCH
					
							SET @MessageToLog = 'Error during reorganization ( ' + ERROR_MESSAGE() + ').'
							EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog

							UPDATE IndexRebuild.Status
								SET LastRebuildReorgError = ERROR_MESSAGE()
							WHERE DatabaseName = @UserDatabase
							AND SchemaName = @TableSchemaName
							AND TableName = @TableName
							AND IndexName = @IndexName
					
						END CATCH

					END

				/*================================================
				-- Can't rebuild online / Reorg for Offline is OFF
				==================================================*/ 

					ELSE IF(@ContainsLOBData = 1 AND @ReorganizeOfflineIndexes = 0)
					BEGIN
					
						SET @MessageToLog = @IndexName + ' ON [' + @UserDatabase + '].[' + @TableSchemaName + '].[' + @TableName + '] contains LOB Data, and procedure has been set to NOT reorganize indexes requiring offline rebuild.'
						EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog
					
						UPDATE IndexRebuild.Status
							SET NotRebuiltReorgReason = 'Index contains LOB Data, and procedure has been set to NOT reorganize indexes requiring offline rebuild.'
						WHERE DatabaseName = @UserDatabase
						AND SchemaName = @TableSchemaName
						AND TableName = @TableName
						AND IndexName = @IndexName

					END
					
				/*===========================================
				-- Fetch next index to rebuild
				===========================================*/ 

					FETCH NEXT FROM IndexCursor INTO @TableName, @TableSchemaName, @IndexName, @ContainsLOBData

		/*===========================================
		-- Finish looping through indexes
		===========================================*/ 
			END
			
			SET @MessageToLog = 'Finished looping through indexes.'
			EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog

			CLOSE IndexCursor
			DEALLOCATE IndexCursor
		
		/*===========================================
		-- Requery Index Fragmenation
		===========================================*/ 
		
			IF(@ReQueryFragmentationAfterRebuild = 1)
			BEGIN
			
				SET @MessageToLog = 'Re-Querying index fragmentation for database.'
				EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog
				
				--Make sure global temp table is empty
				TRUNCATE TABLE ##IndexFragmentation
				
				--Populate ##IndexFragmentation
				EXEC IndexRebuild.GetIndexFragmentation @UserDatabase
					
				UPDATE IndexRebuild.Status
					SET 
					After_AvgFragmentationInPercent = f.AvgFragmentationInPercent,
					After_FillFactor = f.IndexFillFactor,
					After_IndexPageCount = f.IndexPageCount,
					After_IndexDepth = f.IndexDepth,
					After_FragmentCount = f.FragmentCount
				FROM IndexRebuild.Status irs
				JOIN ##IndexFragmentation f
					ON irs.DatabaseName = f.DatabaseName
					AND irs.SchemaName = f.SchemaName
					AND irs.TableName = f.TableName
					AND irs.IndexName = f.IndexName

			END
		
		/*===========================================
		-- Get next user database
		===========================================*/
		
		FETCH NEXT FROM UserDatabasesCursor INTO @UserDatabase

/*===========================================
-- Finish looping through databases
===========================================*/ 
	
	END

	SET @MessageToLog = 'Finished Rebuilding Indexes.'
	EXEC DBA.IndexRebuild.LogMessage @LoggingEnabled, @MessageToLog

	CLOSE UserDatabasesCursor
	DEALLOCATE UserDatabasesCursor

/*===========================================
-- Cleanup
===========================================*/ 

	DROP TABLE ##IndexFragmentation

END

GO

EXEC sys.sp_addextendedproperty @name=N'Version', @value=N'2.0' , @level0type=N'SCHEMA',@level0name=N'IndexRebuild', @level1type=N'PROCEDURE',@level1name=N'RebuildIndexes'
GO


USE [msdb]
GO

/****** Object:  Job [DBA Job: Rebuild Indexes]    Script Date: 11/30/2011 10:22:45 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[AdminJobs]]]    Script Date: 11/30/2011 10:22:45 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[AdminJobs]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[AdminJobs]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA Job: Rebuild Indexes', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'This job is a container for "DBA Job: ADDCFF08-ECF2-4739-AB04-0854A462E13B" .  In other words, it starts and stops the Rebuild Index job. This is done to ensure that if an offline index is being rebuilt, and the rebuild exceeds the time allocated for it to run, that it immidiately stops.', 
		@category_name=N'[AdminJobs]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Call Index Rebuild Job]    Script Date: 11/30/2011 10:22:45 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Call Index Rebuild Job', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE [msdb]

EXEC sp_start_job @job_name = ''DBA Job: ADDCFF08-ECF2-4739-AB04-0854A462E13B''

WAITFOR DELAY ''06:00:00'' --The time the rebuild job is allowed to run

EXEC sp_stop_job @job_name = ''DBA Job: ADDCFF08-ECF2-4739-AB04-0854A462E13B''
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Daily at 10:00 PM (Exclude Sundays for Backups)', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=126, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20111123, 
		@active_end_date=99991231, 
		@active_start_time=220000, 
		@active_end_time=235959, 
		@schedule_uid=N'8f9729b6-23dd-4739-a454-1f08c37d7e7d'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO


USE [msdb]
GO

/****** Object:  Job [DBA Job: ADDCFF08-ECF2-4739-AB04-0854A462E13B]    Script Date: 11/30/2011 10:22:31 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[AdminJobs]]]    Script Date: 11/30/2011 10:22:32 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[AdminJobs]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[AdminJobs]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA Job: ADDCFF08-ECF2-4739-AB04-0854A462E13B', 
		@enabled=0, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Never schedule this job to run.  It should run via a container job that starts and stops it.  This is done to ensure that if an offline index is being rebuilt, and the rebuild exceeds the time allocated for it to run, that it immidiately stops.', 
		@category_name=N'[AdminJobs]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Rebuild Indexes]    Script Date: 11/30/2011 10:22:32 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Rebuild Indexes', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC DBA.IndexRebuild.RebuildIndexes
	@FragmentationPercentThreshold = 10,
	@FillFactor = 80,
	@PriorityDatabases = '''',
	@ExcludedDatabases = '''',
	@LoggingEnabled = 1,
	@ReorganizeOfflineIndexes = 1,
	@RequeryFragmentationAfterRebuild = 1', 
		@database_name=N'DBA', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO


