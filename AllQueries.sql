-- Create Servers table
CREATE TABLE Servers (
    ServerID INT PRIMARY KEY IDENTITY(1,1),
    ServerName NVARCHAR(255) NOT NULL,
    ServerType NVARCHAR(50) NOT NULL, -- e.g., 'SQL Server', 'Azure SQL'
    Location NVARCHAR(255), -- e.g., 'On-premises', 'Azure'
    IPAddress NVARCHAR(50),
    CreatedDate DATETIME DEFAULT GETDATE()
);
-----------------------------------------
CREATE TABLE DatabaseServers (
    ServerID INT PRIMARY KEY IDENTITY,
    ServerName VARCHAR(100) NOT NULL,
    ServerType VARCHAR(50), -- e.g., SQL Server, Oracle, PostgreSQL
    Version VARCHAR(50),
    OperatingSystem VARCHAR(100),
    Description TEXT
);

------------------------------------------------------------------
-- Create Databases table
CREATE TABLE Databases (
    DatabaseID INT PRIMARY KEY IDENTITY(1,1),
    ServerID INT NOT NULL,
    DatabaseName NVARCHAR(255) NOT NULL,
    RecoveryModel NVARCHAR(50), -- e.g., 'FULL', 'SIMPLE', 'BULK_LOGGED'
	 Collation VARCHAR(50),
      CompatibilityLevel VARCHAR(50),
    SizeMB DECIMAL(10, 2),
    Status NVARCHAR(50), -- e.g., 'Online', 'Offline'
    CreatedDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (ServerID) REFERENCES Servers(ServerID) ON DELETE CASCADE
);

-- Create DatabaseProperties table
CREATE TABLE DatabaseProperties (
    PropertyID INT PRIMARY KEY IDENTITY(1,1),
    DatabaseID INT NOT NULL,
    PropertyName NVARCHAR(255) NOT NULL,
    PropertyValue NVARCHAR(255),
    FOREIGN KEY (DatabaseID) REFERENCES Databases(DatabaseID) ON DELETE CASCADE
);
----------------------
CREATE TABLE Roles (
    RoleID INT PRIMARY KEY IDENTITY,
    DatabaseID INT,
    RoleName VARCHAR(100) NOT NULL,
    Description TEXT,
    FOREIGN KEY (DatabaseID) REFERENCES Databases(DatabaseID)
);

DBCC TRACESTATUS(-1);
DBCC TRACESTATUS(trace_flag_number);
SELECT * 
FROM sys.dm_os_sys_info 
--WHERE trace_flag_id IS NOT NULL;
DBCC TRACEON(1204, -1);
GO
DBCC TRACESTATUS(-1);
DBCC TRACEOFF(1204, -1);
GO
USE tempdb;
GO

SELECT name AS [File Name], 
       type_desc AS [File Type], 
       size * 8 / 1024 AS [Size (MB)], 
       growth * 8 / 1024 AS [Growth (MB)], 
       physical_name AS [Physical Name]
FROM sys.master_files
WHERE database_id = DB_ID(N'tempdb');
GO

-- Checking File Growth Settings
USE master;
GO

ALTER DATABASE tempdb 
ADD FILE (
    NAME = tempdev2, 
    FILENAME = 'C:\TempDB\tempdb2.ndf', 
    SIZE = 1000MB, 
    FILEGROWTH = 100MB
);
GO
USE master;
GO

ALTER DATABASE tempdb 
REMOVE FILE tempdev2;
GO
USE master;
GO

ALTER DATABASE tempdb 
MODIFY FILE (
    NAME = tempdev, 
    FILEGROWTH = 100MB
);
GO
USE [YourDatabaseName];
GO

SELECT 
    name AS [File Name],
    size * 8 / 1024 AS [Size (MB)],
    max_size,
    growth,
    physical_name AS [Physical Name]
FROM sys.master_files
WHERE type_desc = 'LOG' AND database_id = DB_ID(N'YourDatabaseName');
GO
USE master;
GO

ALTER DATABASE [YourDatabaseName] 
MODIFY FILE (
    NAME = logical_log_file_name, 
    SIZE = 1024MB
);
GO
Replace logical_log_file_name with the logical name of your transaction log file and adjust the size as needed.

2. Setting Autogrowth
USE distribution;
GO

SELECT 
    agent_id,
    name AS AgentName,
    profile_id,
    subscriber_id,
    subscriber_db AS SubscriberDB,
    status,
    last_action AS LastAction,
    last_start_time AS LastStartTime,
    last_end_time AS LastEndTime,
    job_id
FROM MSsnapshot_agents
UNION ALL
SELECT 
    agent_id,
    name AS AgentName,
    profile_id,
    0 AS subscriber_id,
    publisher_db AS SubscriberDB,
    status,
    last_action AS LastAction,
    last_start_time AS LastStartTime,
    last_end_time AS LastEndTime,
    job_id
FROM MSlogreader_agents
UNION ALL
SELECT 
    agent_id,
    name AS AgentName,
    profile_id,
    subscriber_id,
    subscriber_db AS SubscriberDB,
    status,
    last_action AS LastAction,
    last_start_time AS LastStartTime,
    last_end_time AS LastEndTime,
    job_id
FROM MSdistribution_agents;
GO
--2. Checking Latency and Throughput
USE distribution;
GO

SELECT 
    publication AS Publication,
    subscriber_db AS SubscriberDB,
    avg(xact_seqno - entry_time) AS AvgLatency,
    COUNT(xact_seqno) AS TransactionCount
FROM MSdistribution_history
GROUP BY publication, subscriber_db;
GO
-- Query to check for publications, subscriptions, and replication agents
SET @sql = '
USE [YourDatabaseName];

SELECT 
    @@SERVERNAME AS ServerName, 
    ISNULL(p.PublicationCount, 0) AS PublicationCount, 
    ISNULL(s.SubscriptionCount, 0) AS SubscriptionCount,
    ISNULL(a.ReplicationAgentCount, 0) AS ReplicationAgentCount
FROM
(
    SELECT COUNT(*) AS PublicationCount
    FROM sys.publications
) AS p
CROSS JOIN
(
    SELECT COUNT(*) AS SubscriptionCount
    FROM sys.subscriptions
) AS s
CROSS JOIN
(
    SELECT COUNT(*) AS ReplicationAgentCount
    FROM MSdistribution_agents
) AS a;
';

-- Execute the query across all registered servers in the CMS group
EXEC sp_MSforeachdb @command1 = @sql;
-- Create a temporary table to store results
CREATE TABLE #ReplicationCheckResults (
    ServerName NVARCHAR(128),
    PublicationCount INT,
    SubscriptionCount INT,
    ReplicationAgentCount INT
);

-- Define the query to check for replication
DECLARE @sql NVARCHAR(MAX);

SET @sql = '
USE [YourDatabaseName];

INSERT INTO #ReplicationCheckResults (ServerName, PublicationCount, SubscriptionCount, ReplicationAgentCount)
SELECT 
    @@SERVERNAME AS ServerName, 
    ISNULL(p.PublicationCount, 0) AS PublicationCount, 
    ISNULL(s.SubscriptionCount, 0) AS SubscriptionCount,
    ISNULL(a.ReplicationAgentCount, 0) AS ReplicationAgentCount
FROM
(
    SELECT COUNT(*) AS PublicationCount
    FROM sys.publications
) AS p
CROSS JOIN
(
    SELECT COUNT(*) AS SubscriptionCount
    FROM sys.subscriptions
) AS s
CROSS JOIN
(
    SELECT COUNT(*) AS ReplicationAgentCount
    FROM MSdistribution_agents
) AS a;
';

-- Execute the query across all registered servers in the CMS group
EXEC sp_MSforeachdb @command1 = @sql;

-- Retrieve the results
SELECT * FROM #ReplicationCheckResults;

-- Clean up
DROP TABLE #ReplicationCheckResults;

-- Temporary table to store results
CREATE TABLE #ReplicationTablesCheck (
    ServerName NVARCHAR(128),
    TableName NVARCHAR(128),
    TableCount INT
);

-- Define the query to check for replication tables
DECLARE @sql NVARCHAR(MAX);

SET @sql = '
USE distribution;

IF OBJECT_ID(''MSpublications'') IS NOT NULL
BEGIN
    INSERT INTO #ReplicationTablesCheck (ServerName, TableName, TableCount)
    SELECT @@SERVERNAME AS ServerName, ''MSpublications'' AS TableName, COUNT(*) AS TableCount
    FROM MSpublications;
END

IF OBJECT_ID(''MSsubscriptions'') IS NOT NULL
BEGIN
    INSERT INTO #ReplicationTablesCheck (ServerName, TableName, TableCount)
    SELECT @@SERVERNAME AS ServerName, ''MSsubscriptions'' AS TableName, COUNT(*) AS TableCount
    FROM MSsubscriptions;
END

IF OBJECT_ID(''MSdistribution_agents'') IS NOT NULL
BEGIN
    INSERT INTO #ReplicationTablesCheck (ServerName, TableName, TableCount)
    SELECT @@SERVERNAME AS ServerName, ''MSdistribution_agents'' AS TableName, COUNT(*) AS TableCount
    FROM MSdistribution_agents;
END

IF OBJECT_ID(''MSlogreader_agents'') IS NOT NULL
BEGIN
    INSERT INTO #ReplicationTablesCheck (ServerName, TableName, TableCount)
    SELECT @@SERVERNAME AS ServerName, ''MSlogreader_agents'' AS TableName, COUNT(*) AS TableCount
    FROM MSlogreader_agents;
END

IF OBJECT_ID(''MSmerge_agents'') IS NOT NULL
BEGIN
    INSERT INTO #ReplicationTablesCheck (ServerName, TableName, TableCount)
    SELECT @@SERVERNAME AS ServerName, ''MSmerge_agents'' AS TableName, COUNT(*) AS TableCount
    FROM MSmerge_agents;
END
';

-- Execute the query across all registered servers in the CMS group
EXEC sp_MSforeachdb @command1 = @sql;

-- Retrieve the results
SELECT * FROM #ReplicationTablesCheck;

-- Clean up
DROP TABLE #ReplicationTablesCheck;
-- Temporary table to store the results
CREATE TABLE #DbOwnerMembers (
    DatabaseName NVARCHAR(128),
    MemberName NVARCHAR(128)
);

-- Cursor to iterate through all databases
DECLARE @DatabaseName NVARCHAR(128);
DECLARE @SQL NVARCHAR(MAX);

DECLARE db_cursor CURSOR FOR
SELECT name
FROM sys.databases
WHERE database_id > 4 -- Exclude system databases
  AND state_desc = 'ONLINE'; -- Ensure the database is online

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @DatabaseName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Generate dynamic SQL for each database
    SET @SQL = N'
    USE [' + @DatabaseName + N'];
    INSERT INTO #DbOwnerMembers (DatabaseName, MemberName)
    SELECT 
        ''' + @DatabaseName + N''' AS DatabaseName,
        dp.name AS MemberName
    FROM sys.database_principals dp
    JOIN sys.database_role_members drm ON dp.principal_id = drm.member_principal_id
    JOIN sys.database_principals rp ON rp.principal_id = drm.role_principal_id
    WHERE rp.name = ''db_owner'';';

    -- Execute the dynamic SQL
    EXEC sp_executesql @SQL;

    FETCH NEXT FROM db_cursor INTO @DatabaseName;
END;

CLOSE db_cursor;
DEALLOCATE db_cursor;

-- Select results
SELECT * FROM #DbOwnerMembers;

-- Clean up
DROP TABLE #DbOwnerMembers;

-- This script should be executed from the Central Management Server

DECLARE @sql NVARCHAR(MAX);
SET @sql = '
SELECT 
    @@SERVERNAME AS ServerName,
    name AS DatabaseName,
    recovery_model_desc AS RecoveryModel
FROM sys.databases;
';

EXEC sp_MSforeachdb @command1 = @sql;
CREATE TABLE Servers (
    ServerID INT PRIMARY KEY IDENTITY(1,1),
    ServerName NVARCHAR(255) NOT NULL,
    ServerType NVARCHAR(50) NOT NULL, -- e.g., 'SQL Server', 'Azure SQL'
    Location NVARCHAR(255), -- e.g., 'On-premises', 'Azure'
    IPAddress NVARCHAR(50),
    CreatedDate DATETIME DEFAULT GETDATE()
);
CREATE TABLE Databases (
    DatabaseID INT PRIMARY KEY IDENTITY(1,1),
    ServerID INT NOT NULL,
    DatabaseName NVARCHAR(255) NOT NULL,
    RecoveryModel NVARCHAR(50), -- e.g., 'FULL', 'SIMPLE', 'BULK_LOGGED'
    Status NVARCHAR(50), -- e.g., 'Online', 'Offline'
    CreatedDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (ServerID) REFERENCES Servers(ServerID) ON DELETE CASCADE
);
CREATE TABLE DatabaseProperties (
    PropertyID INT PRIMARY KEY IDENTITY(1,1),
    DatabaseID INT NOT NULL,
    PropertyName NVARCHAR(255) NOT NULL,
    PropertyValue NVARCHAR(255),
    FOREIGN KEY (DatabaseID) REFERENCES Databases(DatabaseID) ON DELETE CASCADE
);
CREATE TABLE Servers (
    ServerID INT PRIMARY KEY IDENTITY(1,1),
    ServerName NVARCHAR(255) NOT NULL,
    ServerType NVARCHAR(50) NOT NULL, -- e.g., 'SQL Server', 'Azure SQL'
    Location NVARCHAR(255), -- e.g., 'On-premises', 'Azure'
    IPAddress NVARCHAR(50),
    CreatedDate DATETIME DEFAULT GETDATE()
);

-- Create Databases table
CREATE TABLE Databases (
    DatabaseID INT PRIMARY KEY IDENTITY(1,1),
    ServerID INT NOT NULL,
    DatabaseName NVARCHAR(255) NOT NULL,
    RecoveryModel NVARCHAR(50), -- e.g., 'FULL', 'SIMPLE', 'BULK_LOGGED'
    Status NVARCHAR(50), -- e.g., 'Online', 'Offline'
    CreatedDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (ServerID) REFERENCES Servers(ServerID) ON DELETE CASCADE
);

-- Create DatabaseProperties table
CREATE TABLE DatabaseProperties (
    PropertyID INT PRIMARY KEY IDENTITY(1,1),
    DatabaseID INT NOT NULL,
    PropertyName NVARCHAR(255) NOT NULL,
    PropertyValue NVARCHAR(255),
    FOREIGN KEY (DatabaseID) REFERENCES Databases(DatabaseID) ON DELETE CASCADE
);
CREATE TABLE ServerMetadata (
    ServerID INT PRIMARY KEY IDENTITY(1,1),
    ServerName NVARCHAR(255) NOT NULL,
    FullyQualifiedDomainName NVARCHAR(255) NOT NULL,
    ResourceGroup NVARCHAR(255) NOT NULL,
    PublicIPAddress NVARCHAR(50) NULL
);
INSERT INTO ServerMetadata (ServerName, FullyQualifiedDomainName, ResourceGroup)
VALUES 
('MySqlServer1', 'myserver1.database.windows.net', 'MyResourceGroup1'),
('MySqlServer2', 'myserver2.database.windows.net', 'MyResourceGroup2');
SELECT 
    ServerName,
    FullyQualifiedDomainName,
    ResourceGroup,
    PublicIPAddress
FROM ServerMetadata;
SELECT 
    ServerName,
    FullyQualifiedDomainName,
    ResourceGroup,
    PublicIPAddress
FROM ServerMetadata;
CREATE SERVER AUDIT [UserAndLoginAudit]
TO FILE 
( 
   FILEPATH = N'C:\SQLAudit\' -- specify your path
)
WITH
( 
   QUEUE_DELAY = 1000,
   ON_FAILURE = CONTINUE
);
GO
--Create Server Audit Specification:

---Create a server audit specification to track login creation and user alterations.
CREATE SERVER AUDIT SPECIFICATION [UserAndLoginAuditSpec]
FOR SERVER AUDIT [UserAndLoginAudit]
ADD (SERVER_ROLE_MEMBER_CHANGE_GROUP),
ADD (SERVER_PERMISSION_CHANGE_GROUP),
ADD (DATABASE_ROLE_MEMBER_CHANGE_GROUP),
ADD (DATABASE_PERMISSION_CHANGE_GROUP),
ADD (SERVER_OBJECT_CHANGE_GROUP),
ADD (DATABASE_OBJECT_CHANGE_GROUP),
ADD (SUCCESSFUL_LOGIN_GROUP),
ADD (FAILED_LOGIN_GROUP);
GO
SELECT 
    event_time,
    action_id,
    succeeded,
    session_server_principal_name,
    server_principal_name,
    database_principal_name,
    object_name,
    statement
FROM sys.fn_get_audit_file ('C:\SQLAudit\*.sqlaudit', DEFAULT, DEFAULT);
-- Step 4: Update website information
SELECT dp.name AS OrphanedUser
FROM sys.database_principals dp
LEFT JOIN sys.server_principals sp
    ON dp.sid = sp.sid
WHERE dp.type IN ('S', 'U', 'G')
  AND sp.sid IS NULL;
GO

-- Check for expired logins
USE master;
GO

SELECT name AS ExpiredLogin
FROM sys.sql_logins
WHERE is_expiration_checked = 1
  AND LOGINPROPERTY(name, 'IsExpired') = 1;
GO
-----------------------------------------------------------------
DECLARE @dbName NVARCHAR(255);

DECLARE db_cursor CURSOR FOR
SELECT name
FROM sys.databases
WHERE state_desc = 'ONLINE'
  AND database_id > 4; -- Skip system databases

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @dbName;

WHILE @@FETCH_STATUS = 0
BEGIN
    DECLARE @sql NVARCHAR(MAX);

    SET @sql = '
    USE [' + @dbName + '];
    SELECT ''' + @dbName + ''' AS DatabaseName, dp.name AS OrphanedUser
    FROM sys.database_principals dp
    LEFT JOIN sys.server_principals sp
        ON dp.sid = sp.sid
    WHERE dp.type IN (''S'', ''U'', ''G'')
      AND sp.sid IS NULL;
    ';

    EXEC sp_executesql @sql;

    FETCH NEXT FROM db_cursor INTO @dbName;
END;

CLOSE db_cursor;
DEALLOCATE db_cursor;
-------------------------------------------------------------------------------------
--# Example for cloning Ola Hallengren's SQL Server Maintenance Solution
--git clone https://github.com/olahallengren/sql-server-maintenance-solution.git

--# Example for cloning Brent Ozar's SQL Server First Responder Kit
--git clone https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit.git

--# Example for cloning Microsoft's SQL Server Samples and Scripts
--git clone https://github.com/microsoft/sql-server-samples.git
---------------------------------------------------------------------------------------
-- Identify slow queries using sys.dm_exec_query_stats
SELECT TOP 10
    qs.total_logical_reads AS [TotalLogicalReads],
    qs.execution_count,
    qs.total_worker_time / qs.execution_count AS [AvgCPUTime],
    SUBSTRING(qt.text, (qs.statement_start_offset/2) + 1,
    ((CASE qs.statement_end_offset
        WHEN -1 THEN DATALENGTH(qt.text)
        ELSE qs.statement_end_offset
    END - qs.statement_start_offset)/2) + 1) AS [QueryText]
FROM
    sys.dm_exec_query_stats qs
CROSS APPLY
    sys.dm_exec_sql_text(qs.sql_handle) AS qt
ORDER BY
    qs.total_logical_reads DESC;
	--------------------------------------------------------------------
	-- Get the actual execution plan for a specific query
SET STATISTICS XML ON;

-- Your slow query here
SELECT * FROM LargeTable WHERE SomeColumn = 'SomeValue';

SET STATISTICS XML OFF;
-----------------------------------------------------------------------------------
-- Find unused indexes
SELECT 
    o.name AS TableName,
    i.name AS IndexName,
    i.index_id
FROM 
    sys.indexes AS i
    INNER JOIN sys.objects AS o ON i.object_id = o.object_id
    LEFT OUTER JOIN sys.dm_db_index_usage_stats AS s ON i.object_id = s.object_id AND i.index_id = s.index_id
WHERE 
    o.type = 'U'
    AND i.name IS NOT NULL
    AND s.index_id IS NULL
ORDER BY 
    o.name, i.name;
----------------------------------
CREATE TABLE Roles (
    RoleID INT PRIMARY KEY IDENTITY,
    DatabaseID INT,
    RoleName VARCHAR(100) NOT NULL,
    Description TEXT,
    FOREIGN KEY (DatabaseID) REFERENCES Databases(DatabaseID)
);
CREATE TABLE Users (
    UserID INT PRIMARY KEY IDENTITY,
    DatabaseID INT,
    UserName VARCHAR(100) NOT NULL,
    LoginName VARCHAR(100),
    DefaultSchema VARCHAR(100),
    Description TEXT,
    FOREIGN KEY (DatabaseID) REFERENCES Databases(DatabaseID)
);
CREATE TABLE Permissions (
    PermissionID INT PRIMARY KEY IDENTITY,
    DatabaseID INT,
    GranteeType VARCHAR(50), -- 'User' or 'Role'
    GranteeID INT, -- UserID or RoleID
    ObjectName VARCHAR(100),
    ObjectType VARCHAR(50), -- e.g., Table, View, Procedure
    PermissionType VARCHAR(50), -- e.g., SELECT, INSERT
    PermissionState VARCHAR(50), -- e.g., GRANT, DENY
    FOREIGN KEY (DatabaseID) REFERENCES Databases(DatabaseID)
);
CREATE TABLE Tables (
    TableID INT PRIMARY KEY IDENTITY,
    DatabaseID INT,
    TableName VARCHAR(100) NOT NULL,
    SchemaName VARCHAR(100),
    [RowCount] INT,
    Description TEXT,
    FOREIGN KEY (DatabaseID) REFERENCES Databases(DatabaseID)
);
CREATE TABLE Columns (
    ColumnID INT PRIMARY KEY IDENTITY,
    TableID INT,
    ColumnName VARCHAR(100) NOT NULL,
    DataType VARCHAR(100),
    IsNullable BIT,
    DefaultValue VARCHAR(100),
    Description TEXT,
    FOREIGN KEY (TableID) REFERENCES Tables(TableID)
);
CREATE TABLE Indexes (
    IndexID INT PRIMARY KEY IDENTITY,
    TableID INT,
    IndexName VARCHAR(100) NOT NULL,
    IndexType VARCHAR(50), -- e.g., Clustered, Non-Clustered
    ColumnsIncluded VARCHAR(500),
    IsUnique BIT,
    IsPrimaryKey BIT,
    FOREIGN KEY (TableID) REFERENCES Tables(TableID)
);
CREATE TABLE Relationships (
    RelationshipID INT PRIMARY KEY IDENTITY,
    ParentTableID INT,
    ChildTableID INT,
    ForeignKeyName VARCHAR(100),
    ParentColumns VARCHAR(500),
    ChildColumns VARCHAR(500),
    FOREIGN KEY (ParentTableID) REFERENCES Tables(TableID),
    FOREIGN KEY (ChildTableID) REFERENCES Tables(TableID)
);
CREATE TABLE Backups (
    BackupID INT PRIMARY KEY IDENTITY,
    DatabaseID INT,
    BackupType VARCHAR(50), -- e.g., Full, Differential, Transaction Log
    BackupFrequency VARCHAR(50), -- e.g., Daily, Weekly
    BackupLocation VARCHAR(200),
    LastBackupDate DATETIME,
    FOREIGN KEY (DatabaseID) REFERENCES Databases(DatabaseID)
);
CREATE TABLE SecuritySettings (
    SecuritySettingID INT PRIMARY KEY IDENTITY,
    DatabaseID INT,
    AuthenticationMode VARCHAR(50), -- e.g., Windows, Mixed
    EncryptionEnabled BIT,
    EncryptionType VARCHAR(50), -- e.g., TDE, Column-Level
    KeyManagement VARCHAR(100),
    AuditPolicy VARCHAR(200),
    FOREIGN KEY (DatabaseID) REFERENCES Databases(DatabaseID)
);
-- Server Information
SELECT 
    SERVERPROPERTY('MachineName') AS ServerName,
    SERVERPROPERTY('ProductVersion') AS Version,
    SERVERPROPERTY('Edition') AS Edition,
    SERVERPROPERTY('ProductLevel') AS ProductLevel,
    SERVERPROPERTY('EngineEdition') AS EngineEdition,
    SERVERPROPERTY('Collation') AS Collation
-- Databases Information
SELECT 
    db.database_id AS DatabaseID,
    db.name AS DatabaseName,
    db.collation_name AS Collation,
    db.recovery_model_desc AS RecoveryModel,
    db.compatibility_level AS CompatibilityLevel,
    CAST(SUM(mf.size) * 8.0 / 1024 AS DECIMAL(10, 2)) AS SizeMB
FROM 
    sys.databases db
JOIN 
    sys.master_files mf ON db.database_id = mf.database_id
GROUP BY 
    db.database_id, db.name, db.collation_name, db.recovery_model_desc, db.compatibility_level;
	-- Users Information
SELECT 
    dp.principal_id AS UserID,
    dp.name AS UserName,
    dp.type_desc AS UserType,
    dp.default_schema_name AS DefaultSchema,
    dp.create_date AS CreateDate,
    dp.modify_date AS ModifyDate
FROM 
    sys.database_principals dp
WHERE 
    dp.type IN ('S', 'U', 'G'); -- S = SQL User, U = Windows User, G = Windows Group
-- Roles Information
SELECT 
    rp.principal_id AS RoleID,
    rp.name AS RoleName,
    rp.type_desc AS RoleType,
    rp.create_date AS CreateDate,
    rp.modify_date AS ModifyDate
FROM 
    sys.database_principals rp
WHERE 
    rp.type IN ('R', 'A'); -- R = Database Role, A = Application Role
-- Permissions Information
SELECT 
    dp.permission_id AS PermissionID,
    dp.class_desc AS ObjectType,
    dp.permission_name AS PermissionType,
    dp.state_desc AS PermissionState,
    OBJECT_NAME(dp.major_id) AS ObjectName,
    pr.name AS GranteeName,
    pr.type_desc AS GranteeType
FROM 
    sys.database_permissions dp
JOIN 
    sys.database_principals pr ON dp.grantee_principal_id = pr.principal_id;
-- Tables Information
SELECT 
    t.object_id AS TableID,
    t.name AS TableName,
    s.name AS SchemaName,
    p.rows AS RowCount
FROM 
    sys.tables t
JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
JOIN 
    sys.partitions p ON t.object_id = p.object_id
WHERE 
    p.index_id IN (0, 1)
GROUP BY 
    t.object_id, t.name, s.name, p.rows;
-- Columns Information
SELECT 
    c.column_id AS ColumnID,
    t.object_id AS TableID,
    c.name AS ColumnName,
    tp.name AS DataType,
    c.max_length AS MaxLength,
    c.is_nullable AS IsNullable,
    c.is_identity AS IsIdentity,
    c.is_computed AS IsComputed,
    dc.definition AS DefaultValue
FROM 
    sys.columns c
JOIN 
    sys.tables t ON c.object_id = t.object_id
JOIN 
    sys.types tp ON c.user_type_id = tp.user_type_id
LEFT JOIN 
    sys.default_constraints dc ON c.default_object_id = dc.object_id;
-- Indexes Information
SELECT 
    i.index_id AS IndexID,
    t.object_id AS TableID,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique AS IsUnique,
    i.is_primary_key AS IsPrimaryKey,
    STUFF((SELECT ', ' + c.name
           FROM sys.index_columns ic
           JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
           FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS ColumnsIncluded
FROM 
    sys.indexes i
JOIN 
    sys.tables t ON i.object_id = t.object_id
WHERE 
    i.type IN (1, 2); -- Clustered and Non-clustered indexes
-- Relationships (Foreign Keys) Information
SELECT 
    fk.object_id AS RelationshipID,
    fk.name AS ForeignKeyName,
    p.name AS ParentTableName,
    rf.name AS ReferencedTableName,
    STUFF((SELECT ', ' + pc.name
           FROM sys.foreign_key_columns fkc
           JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
           WHERE fkc.constraint_object_id = fk.object_id
           FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS ParentColumns,
    STUFF((SELECT ', ' + rc.name
           FROM sys.foreign_key_columns fkc
           JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
           WHERE fkc.constraint_object_id = fk.object_id
           FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS ReferencedColumns
FROM 
    sys.foreign_keys fk
JOIN 
    sys.tables p ON fk.parent_object_id = p.object_id
JOIN 
    sys.tables rf ON fk.referenced_object_id = rf.object_id;

	---
	-- Backup Information
SELECT 
    bs.database_name AS DatabaseName,
    bs.backup_type_desc AS BackupType,
    bs.backup_start_date AS BackupStartDate,
    bs.backup_finish_date AS BackupFinishDate,
    bmf.physical_device_name AS BackupLocation
FROM 
    msdb.dbo.backupset bs
JOIN 
    msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
WHERE 
    bs.type IN ('D', 'I', 'L'); -- D = Full, I = Differential, L = Log
------
-- Security Settings
SELECT 
    name AS DatabaseName,
    CASE 
        WHEN is_encrypted = 1 THEN 'Enabled'
        ELSE 'Disabled'
    END AS EncryptionEnabled,
    CASE 
        WHEN is_trustworthy_on = 1 THEN 'Enabled'
        ELSE 'Disabled'
    END AS TrustworthySetting,
    CASE 
        WHEN is_db_chaining_on = 1 THEN 'Enabled'
        ELSE 'Disabled'
    END AS DbChaining
FROM 
    sys.databases;
--------------------1. Server Information
sql
Copy code
-- Server Information
SELECT 
    SERVERPROPERTY('MachineName') AS ServerName,
    SERVERPROPERTY('ProductVersion') AS Version,
    SERVERPROPERTY('Edition') AS Edition,
    SERVERPROPERTY('ProductLevel') AS ProductLevel,
    SERVERPROPERTY('EngineEdition') AS EngineEdition,
    SERVERPROPERTY('Collation') AS Collation;
2. Databases Information
sql
Copy code
-- Databases Information
SELECT 
    db.database_id AS DatabaseID,
    db.name AS DatabaseName,
    db.collation_name AS Collation,
    db.recovery_model_desc AS RecoveryModel,
    db.compatibility_level AS CompatibilityLevel,
    SUM(mf.size) * 8.0 / 1024 AS SizeMB
FROM 
    sys.databases db
JOIN 
    sys.master_files mf ON db.database_id = mf.database_id
GROUP BY 
    db.database_id, db.name, db.collation_name, db.recovery_model_desc, db.compatibility_level;
3. Users Information
sql
Copy code
-- Users Information
SELECT 
    DB_NAME() AS DatabaseName,
    dp.principal_id AS UserID,
    dp.name AS UserName,
    dp.type_desc AS UserType,
    dp.default_schema_name AS DefaultSchema,
    dp.create_date AS CreateDate,
    dp.modify_date AS ModifyDate
FROM 
    sys.database_principals dp
WHERE 
    dp.type IN ('S', 'U', 'G'); -- S = SQL User, U = Windows User, G = Windows Group
4. Roles Information
sql
Copy code
-- Roles Information
SELECT 
    DB_NAME() AS DatabaseName,
    rp.principal_id AS RoleID,
    rp.name AS RoleName,
    rp.type_desc AS RoleType,
    rp.create_date AS CreateDate,
    rp.modify_date AS ModifyDate
FROM 
    sys.database_principals rp
WHERE 
    rp.type IN ('R', 'A'); -- R = Database Role, A = Application Role
5. Permissions Information
sql
Copy code
-- Permissions Information
SELECT 
    DB_NAME() AS DatabaseName,
    dp.permission_id AS PermissionID,
    dp.class_desc AS ObjectType,
    dp.permission_name AS PermissionType,
    dp.state_desc AS PermissionState,
    OBJECT_NAME(dp.major_id) AS ObjectName,
    pr.name AS GranteeName,
    pr.type_desc AS GranteeType
FROM 
    sys.database_permissions dp
JOIN 
    sys.database_principals pr ON dp.grantee_principal_id = pr.principal_id;
6. Tables Information
sql
Copy code
-- Tables Information
SELECT 
    DB_NAME() AS DatabaseName,
    t.object_id AS TableID,
    t.name AS TableName,
    s.name AS SchemaName,
    p.rows AS RowCount
FROM 
    sys.tables t
JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
JOIN 
    sys.partitions p ON t.object_id = p.object_id
WHERE 
    p.index_id IN (0, 1)
GROUP BY 
    t.object_id, t.name, s.name, p.rows;
7. Columns Information
sql
Copy code
-- Columns Information
SELECT 
    DB_NAME() AS DatabaseName,
    c.column_id AS ColumnID,
    t.object_id AS TableID,
    c.name AS ColumnName,
    tp.name AS DataType,
    c.max_length AS MaxLength,
    c.is_nullable AS IsNullable,
    c.is_identity AS IsIdentity,
    c.is_computed AS IsComputed,
    dc.definition AS DefaultValue
FROM 
    sys.columns c
JOIN 
    sys.tables t ON c.object_id = t.object_id
JOIN 
    sys.types tp ON c.user_type_id = tp.user_type_id
LEFT JOIN 
    sys.default_constraints dc ON c.default_object_id = dc.object_id;
8. Indexes Information
sql
Copy code
-- Indexes Information
SELECT 
    DB_NAME() AS DatabaseName,
    i.index_id AS IndexID,
    t.object_id AS TableID,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique AS IsUnique,
    i.is_primary_key AS IsPrimaryKey,
    STUFF((SELECT ', ' + c.name
           FROM sys.index_columns ic
           JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
           WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
           FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS ColumnsIncluded
FROM 
    sys.indexes i
JOIN 
    sys.tables t ON i.object_id = t.object_id
WHERE 
    i.type IN (1, 2); -- Clustered and Non-clustered indexes
9. Relationships Information
sql
Copy code
-- Relationships (Foreign Keys) Information
SELECT 
    DB_NAME() AS DatabaseName,
    fk.object_id AS RelationshipID,
    fk.name AS ForeignKeyName,
    p.name AS ParentTableName,
    rf.name AS ReferencedTableName,
    STUFF((SELECT ', ' + pc.name
           FROM sys.foreign_key_columns fkc
           JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
           WHERE fkc.constraint_object_id = fk.object_id
           FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS ParentColumns,
    STUFF((SELECT ', ' + rc.name
           FROM sys.foreign_key_columns fkc
           JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
           WHERE fkc.constraint_object_id = fk.object_id
           FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS ReferencedColumns
FROM 
    sys.foreign_keys fk
JOIN 
    sys.tables p ON fk.parent_object_id = p.object_id
JOIN 
    sys.tables rf ON fk.referenced_object_id = rf.object_id;
10. Backups Information
To retrieve backup information, query the msdb database.

sql
Copy code
-- Backup Information
SELECT 
    bs.database_name AS DatabaseName,
    bs.backup_type_desc AS BackupType,
    bs.backup_start_date AS BackupStartDate,
    bs.backup_finish_date AS BackupFinishDate,
    bmf.physical_device_name AS BackupLocation
FROM 
    msdb.dbo.backupset bs
JOIN 
    msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
WHERE 
    bs.type IN ('D', 'I', 'L'); -- D = Full, I = Differential, L = Log
11. Security Settings
sql
Copy code
-- Security Settings
SELECT 
    name AS DatabaseName,
    CASE 
        WHEN is_encrypted = 1 THEN 'Enabled'
        ELSE 'Disabled'
    END AS EncryptionEnabled,
    CASE 
        WHEN is_trustworthy_on = 1 THEN 'Enabled'
        ELSE 'Disabled'
    END AS TrustworthySetting,
    CASE 
        WHEN is_db_chaining_on = 1 THEN 'Enabled'
        ELSE 'Disabled'
    END AS DbChaining
FROM 
    sys.databases;
Automating Execution
To automate the collection of this information, you can combine these scripts into a stored procedure. Here’s an example:

sql
Copy code
CREATE PROCEDURE sp_CollectDatabaseInfo
AS
BEGIN
    -- Collect Server Information
    INSERT INTO YourMetaDatabase.DatabaseServers (ServerName, Version, Edition, ProductLevel, EngineEdition, Collation)
    SELECT 
        SERVERPROPERTY('MachineName'), 
        SERVERPROPERTY('ProductVersion'), 
        SERVERPROPERTY('Edition'), 
        SERVERPROPERTY('ProductLevel'), 
        SERVERPROPERTY('EngineEdition'), 
        SERVERPROPERTY('Collation');

    -- Collect Databases Information
    INSERT INTO YourMetaDatabase.Databases (DatabaseID, DatabaseName, Collation, RecoveryModel, CompatibilityLevel, SizeMB)
    SELECT 
        db.database_id, 
        db.name, 
        db.collation_name, 
        db.recovery_model_desc, 
        db.compatibility_level, 
        SUM(mf.size) * 8.0 / 1024
    FROM 
        sys.databases db
    JOIN 
        sys.master_files mf ON db.database_id = mf.database_id
    GROUP BY 
        db.database_id, db.name, db.collation_name, db.recovery_model_desc, db.compatibility_level;

    -- Collect Users Information
    INSERT INTO YourMetaDatabase.Users (DatabaseName, UserID, UserName, UserType, DefaultSchema, CreateDate, ModifyDate)
    SELECT 
        DB_NAME(), 
        dp.principal_id, 
        dp.name, 
        dp.type_desc, 
        dp.default_schema_name, 
        dp.create_date, 
        dp.modify_date
    FROM 
        sys.database_principals dp
    WHERE 
        dp.type IN ('S', 'U', 'G'); 

    -- Collect Roles Information
    INSERT INTO YourMetaDatabase.Roles (DatabaseName, RoleID, RoleName, RoleType, CreateDate, ModifyDate)
    SELECT 
        DB_NAME(), 
        rp.principal_id, 
        rp.name, 
        rp.type_desc, 
        rp.create_date, 
        rp.modify_date
    FROM 
        sys.database_principals rp
    WHERE 
        rp.type IN ('R', 'A');

    -- Collect Permissions Information
    INSERT INTO YourMetaDatabase.Permissions (DatabaseName, PermissionID, ObjectType, PermissionType, PermissionState, ObjectName, GranteeName, GranteeType)
    SELECT 
        DB_NAME(), 
        dp.permission_id, 
        dp.class_desc, 
        dp.permission_name, 
        dp.state_desc, 
        OBJECT_NAME(dp.major_id), 
        pr.name, 
        pr.type_desc
    FROM 
        sys.database_permissions dp
    JOIN 
        sys.database_principals pr ON dp.grantee_principal_id = pr.principal_id;

    -- Collect Tables Information
    INSERT INTO YourMetaDatabase.Tables (DatabaseName, TableID, TableName, SchemaName, RowCount)
    SELECT 
        DB_NAME(), 
        t.object_id, 
        t.name, 
        s.name, 
        p.rows
    FROM 
        sys.tables t
    JOIN 
        sys.schemas s ON t.schema_id = s.schema_id
    JOIN 
        sys.partitions p ON t.object_id = p.object_id
    WHERE 
        p.index_id IN (0, 1)
    GROUP BY 
        t.object_id, t.name, s.name, p.rows;

    -- Collect Columns Information
    INSERT INTO YourMetaDatabase.Columns (DatabaseName, ColumnID, TableID, ColumnName, DataType, MaxLength, IsNullable, IsIdentity, IsComputed, DefaultValue)
    SELECT 
        DB_NAME(), 
        c.column_id, 
        t.object_id, 
        c.name, 
        tp.name, 
        c.max_length, 
        c.is_nullable, 
        c.is_identity, 
        c.is_computed, 
        dc.definition
    FROM 
        sys.columns c
    JOIN 
        sys.tables t ON c.object_id = t.object_id
    JOIN 
        sys.types tp ON c.user_type_id = tp.user_type_id
    LEFT JOIN 
        sys.default_constraints dc ON c.default_object_id = dc.object_id;

    -- Collect Indexes Information
    INSERT INTO YourMetaDatabase.Indexes (DatabaseName, IndexID, TableID, IndexName, IndexType, IsUnique, IsPrimaryKey, ColumnsIncluded)
    SELECT 
        DB_NAME(), 
        i.index_id, 
        t.object_id, 
        i.name, 
        i.type_desc, 
        i.is_unique, 
        i.is_primary_key, 
        STUFF((SELECT ', ' + c.name
               FROM sys.index_columns ic
               JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
               WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
               FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
    FROM 
        sys.indexes i
    JOIN 
        sys.tables t ON i.object_id = t.object_id
    WHERE 
        i.type IN (1, 2);

    -- Collect Relationships Information
    INSERT INTO YourMetaDatabase.Relationships (DatabaseName, RelationshipID, ForeignKeyName, ParentTableName, ReferencedTableName, ParentColumns, ReferencedColumns)
    SELECT 
        DB_NAME(), 
        fk.object_id, 
        fk.name, 
        p.name, 
        rf.name, 
        STUFF((SELECT ', ' + pc.name
               FROM sys.foreign_key_columns fkc
               JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
               WHERE fkc.constraint_object_id = fk.object_id
               FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''), 
        STUFF((SELECT ', ' + rc.name
               FROM sys.foreign_key_columns fkc
               JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
               WHERE fkc.constraint_object_id = fk.object_id
               FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
    FROM 
        sys.foreign_keys fk
    JOIN 
        sys.tables p ON fk.parent_object_id = p.object_id
    JOIN 
        sys.tables rf ON fk.referenced_object_id = rf.object_id;

    -- Collect Backups Information
    INSERT INTO YourMetaDatabase.Backups (DatabaseName, BackupType, BackupStartDate, BackupFinishDate, BackupLocation)
    SELECT 
        bs.database_name, 
        bs.backup_type_desc, 
        bs.backup_start_date, 
        bs.backup_finish_date, 
        bmf.physical_device_name
    FROM 
        msdb.dbo.backupset bs
    JOIN 
        msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
    WHERE 
        bs.type IN ('D', 'I', 'L');

    -- Collect Security Settings
    INSERT INTO YourMetaDatabase.SecuritySettings (DatabaseName, EncryptionEnabled, TrustworthySetting, DbChaining)
    SELECT 
        name, 
        CASE 
            WHEN is_encrypted = 1 THEN 'Enabled'
            ELSE 'Disabled'
        END, 
        CASE 
            WHEN is_trustworthy_on = 1 THEN 'Enabled'
            ELSE 'Disabled'
        END, 
        CASE 
            WHEN is_db_chaining_on = 1 THEN 'Enabled'
            ELSE 'Disabled'
        END
    FROM 
        sys.databases;
END;
-----
-- Replication Information
-- Publishers
SELECT 
    srv.srvname AS PublisherName,
    db.name AS PublicationDatabase,
    pub.name AS PublicationName,
    pub.pubid AS PublicationID,
    pub.publisher_db AS PublisherDB,
    pub.status AS PublicationStatus
FROM 
    msdb.dbo.MSpublications pub
JOIN 
    master.dbo.sysservers srv ON pub.publisher_id = srv.srvid
JOIN 
    master.dbo.sysdatabases db ON pub.publisher_db = db.name;

-- Subscriptions
SELECT 
    pub.srvname AS PublisherName,
    pub_db.name AS PublicationDatabase,
    pub.name AS PublicationName,
    sub.srvname AS SubscriberName,
    sub_db.name AS SubscriptionDatabase,
    subs.subscriber_id AS SubscriptionID,
    subs.subscription_type AS SubscriptionType,
    subs.status AS SubscriptionStatus
FROM 
    msdb.dbo.MSsubscriptions subs
JOIN 
    msdb.dbo.MSpublications pub ON subs.publication_id = pub.pubid
JOIN 
    master.dbo.sysservers pub_srv ON pub.publisher_id = pub_srv.srvid
JOIN 
    master.dbo.sysdatabases pub_db ON pub.publisher_db = pub_db.name
JOIN 
    master.dbo.sysservers sub_srv ON subs.subscriber_id = sub_srv.srvid
JOIN 
    master.dbo.sysdatabases sub_db ON subs.subscriber_db = sub_db.name;
-----------------------------------------------------------------------
CREATE TABLE ReplicationPublishers (
    PublisherName NVARCHAR(128),
    PublicationDatabase NVARCHAR(128),
    PublicationName NVARCHAR(128),
    PublicationID INT,
    PublisherDB NVARCHAR(128),
    PublicationStatus INT
);

-- Subscriptions Information
CREATE TABLE ReplicationSubscriptions (
    PublisherName NVARCHAR(128),
    PublicationDatabase NVARCHAR(128),
    PublicationName NVARCHAR(128),
    SubscriberName NVARCHAR(128),
    SubscriptionDatabase NVARCHAR(128),
    SubscriptionID INT,
    SubscriptionType INT,
    SubscriptionStatus INT
);

-------------------------------------
CREATE PROCEDURE sp_CollectDatabaseInfo
AS
BEGIN
    -- Collect Server Information
    INSERT INTO YourMetaDatabase.DatabaseServers (ServerName, Version, Edition, ProductLevel, EngineEdition, Collation)
    SELECT 
        SERVERPROPERTY('MachineName'), 
        SERVERPROPERTY('ProductVersion'), 
        SERVERPROPERTY('Edition'), 
        SERVERPROPERTY('ProductLevel'), 
        SERVERPROPERTY('EngineEdition'), 
        SERVERPROPERTY('Collation');

    -- Collect Databases Information
    INSERT INTO YourMetaDatabase.Databases (DatabaseID, DatabaseName, Collation, RecoveryModel, CompatibilityLevel, SizeMB)
    SELECT 
        db.database_id, 
        db.name, 
        db.collation_name, 
        db.recovery_model_desc, 
        db.compatibility_level, 
        SUM(mf.size) * 8.0 / 1024
    FROM 
        sys.databases db
    JOIN 
        sys.master_files mf ON db.database_id = mf.database_id
    GROUP BY 
        db.database_id, db.name, db.collation_name, db.recovery_model_desc, db.compatibility_level;

    -- Collect Users Information
    INSERT INTO YourMetaDatabase.Users (DatabaseName, UserID, UserName, UserType, DefaultSchema, CreateDate, ModifyDate)
    SELECT 
        DB_NAME(), 
        dp.principal_id, 
        dp.name, 
        dp.type_desc, 
        dp.default_schema_name, 
        dp.create_date, 
        dp.modify_date
    FROM 
        sys.database_principals dp
    WHERE 
        dp.type IN ('S', 'U', 'G'); 

    -- Collect Roles Information
    INSERT INTO YourMetaDatabase.Roles (DatabaseName, RoleID, RoleName, RoleType, CreateDate, ModifyDate)
    SELECT 
        DB_NAME(), 
        rp.principal_id, 
        rp.name, 
        rp.type_desc, 
        rp.create_date, 
        rp.modify_date
    FROM 
        sys.database_principals rp
    WHERE 
        rp.type IN ('R', 'A');

    -- Collect Permissions Information
    INSERT INTO YourMetaDatabase.Permissions (DatabaseName, PermissionID, ObjectType, PermissionType, PermissionState, ObjectName, GranteeName, GranteeType)
    SELECT 
        DB_NAME(), 
        dp.permission_id, 
        dp.class_desc, 
        dp.permission_name, 
        dp.state_desc, 
        OBJECT_NAME(dp.major_id), 
        pr.name, 
        pr.type_desc
    FROM 
        sys.database_permissions dp
    JOIN 
        sys.database_principals pr ON dp.grantee_principal_id = pr.principal_id;

    -- Collect Tables Information
    INSERT INTO YourMetaDatabase.Tables (DatabaseName, TableID, TableName, SchemaName, RowCount)
    SELECT 
        DB_NAME(), 
        t.object_id, 
        t.name, 
        s.name, 
        p.rows
    FROM 
        sys.tables t
    JOIN 
        sys.schemas s ON t.schema_id = s.schema_id
    JOIN 
        sys.partitions p ON t.object_id = p.object_id
    WHERE 
        p.index_id IN (0, 1)
    GROUP BY 
        t.object_id, t.name, s.name, p.rows;

    -- Collect Columns Information
    INSERT INTO YourMetaDatabase.Columns (DatabaseName, ColumnID, TableID, ColumnName, DataType, MaxLength, IsNullable, IsIdentity, IsComputed, DefaultValue)
    SELECT 
        DB_NAME(), 
        c.column_id, 
        t.object_id, 
        c.name, 
        tp.name, 
        c.max_length, 
        c.is_nullable, 
        c.is_identity, 
        c.is_computed, 
        dc.definition
    FROM 
        sys.columns c
    JOIN 
        sys.tables t ON c.object_id = t.object_id
    JOIN 
        sys.types tp ON c.user_type_id = tp.user_type_id
    LEFT JOIN 
        sys.default_constraints dc ON c.default_object_id = dc.object_id;

    -- Collect Indexes Information
    INSERT INTO YourMetaDatabase.Indexes (DatabaseName, IndexID, TableID, IndexName, IndexType, IsUnique, IsPrimaryKey, ColumnsIncluded)
    SELECT 
        DB_NAME(), 
        i.index_id, 
        t.object_id, 
        i.name, 
        i.type_desc, 
        i.is_unique, 
        i.is_primary_key, 
        STUFF((SELECT ', ' + c.name
               FROM sys.index_columns ic
               JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
               WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
               FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
    FROM 
        sys.indexes i
    JOIN 
        sys.tables t ON i.object_id = t.object_id
    WHERE 
        i.type IN (1, 2);

    -- Collect Relationships Information
    INSERT INTO YourMetaDatabase.Relationships (DatabaseName, RelationshipID, ForeignKeyName, ParentTableName, ReferencedTableName, ParentColumns, ReferencedColumns)
    SELECT 
        DB_NAME(), 
        fk.object_id, 
        fk.name, 
        p.name, 
        rf.name, 
        STUFF((SELECT ', ' + pc.name
               FROM sys.foreign_key_columns fkc
               JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
               WHERE fkc.constraint_object_id = fk.object_id
               FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''), 
        STUFF((SELECT ', ' + rc.name
               FROM sys.foreign_key_columns fkc
               JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
               WHERE fkc.constraint_object_id = fk.object_id
               FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
    FROM 
        sys.foreign_keys fk
    JOIN 
        sys.tables p ON fk.parent_object_id = p.object_id
    JOIN 
        sys.tables rf ON fk.referenced_object_id = rf.object_id;

    -- Collect Backups Information
    INSERT INTO YourMetaDatabase.Backups (DatabaseName, BackupType, BackupStartDate, BackupFinishDate, BackupLocation)
    SELECT 
        bs.database_name, 
        bs.backup_type_desc, 
        bs.backup_start_date, 
        bs.backup_finish_date, 
        bmf.physical_device_name
    FROM 
        msdb.dbo.backupset bs
    JOIN 
        msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
    WHERE 
        bs.type IN ('D', 'I', 'L');

    -- Collect Security Settings
    INSERT INTO YourMetaDatabase.SecuritySettings (DatabaseName, EncryptionEnabled, TrustworthySetting, DbChaining)
    SELECT 
        name, 
        CASE 
            WHEN is_encrypted = 1 THEN 'Enabled'
            ELSE 'Disabled'
        END, 
        CASE 
            WHEN is_trustworthy_on = 1 THEN 'Enabled'
            ELSE 'Disabled'
        END, 
        CASE 
            WHEN is_db_chaining_on = 1 THEN 'Enabled'
            ELSE 'Disabled'
        END
    FROM 
        sys.databases;

    -- Collect Error Log Information
    INSERT INTO YourMetaDatabase.ErrorLog (LogDate, ProcessInfo, Text)
    EXEC xp_readerrorlog;

    -- Collect Job Information
    INSERT INTO YourMetaDatabase.JobHistory (JobName, LastRunDate, LastRunDuration, LastRunOutcome)
    SELECT 
        j.name,
        MAX(jh.run_date + jh.run_time * 9e-3) AS LastRunDate,
        MAX((jh.run_duration / 10000 * 3600 +
             jh.run_duration / 100 % 100 * 60 +
             jh.run_duration % 100) * 1e-2) AS LastRunDuration,
        CASE jh.run_status
            WHEN 0 THEN 'Failed'
            WHEN 1 THEN 'Succeeded'
            ELSE 'Unknown'
        END AS LastRunOutcome
    FROM msdb.dbo.sysjobs j
    JOIN msdb.dbo.sysjobhistory jh ON j.job_id = jh.job_id
    WHERE jh.step_id = 0
    GROUP BY j.name;

END;
--------------------------------------------------------------------------------------------------
CREATE PROCEDURE sp_CollectErrorLogAndJobInfo
AS
BEGIN
    SET NOCOUNT ON;

    -- Error Log Information
    INSERT INTO YourMetaDatabase.ErrorLog (LogDate, ProcessInfo, Text)
    EXEC xp_readerrorlog;

    -- Job History Information
    INSERT INTO YourMetaDatabase.JobHistory (JobName, LastRunDate, LastRunDuration, LastRunOutcome, ScheduleName, ScheduleStartTime)
    SELECT 
        j.name AS JobName,
        MAX(jh.run_date + jh.run_time * 9e-3) AS LastRunDate,
        MAX((jh.run_duration / 10000 * 3600 +
             jh.run_duration / 100 % 100 * 60 +
             jh.run_duration % 100) * 1e-2) AS LastRunDuration,
        CASE jh.run_status
            WHEN 0 THEN 'Failed'
            WHEN 1 THEN 'Succeeded'
            ELSE 'Unknown'
        END AS LastRunOutcome,
        s.name AS ScheduleName,
        CONVERT(TIME, s.active_start_time) AS ScheduleStartTime
    FROM msdb.dbo.sysjobs j
    JOIN msdb.dbo.sysjobhistory jh ON j.job_id = jh.job_id
    LEFT JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
    LEFT JOIN msdb.dbo.sysschedules s ON js.schedule_id = s.schedule_id
    WHERE jh.step_id = 0
    GROUP BY j.name, s.name, s.active_start_time;
END;
------------------------------------------
-- Error Log Table
CREATE TABLE ErrorLog (
    LogDate DATETIME,
    ProcessInfo NVARCHAR(MAX),
    Text NVARCHAR(MAX)
);

-- Job History Table
CREATE TABLE JobHistory (
    JobName NVARCHAR(128),
    LastRunDate DATETIME,
    LastRunDuration FLOAT,
    LastRunOutcome NVARCHAR(50)
);
------------------------------------------------------------
CREATE PROCEDURE sp_CollectErrorLogAndJobInfo
AS
BEGIN
    SET NOCOUNT ON;

    -- Error Log Information
    INSERT INTO YourMetaDatabase.ErrorLog (LogDate, ProcessInfo, Text)
    EXEC xp_readerrorlog;

    -- Job History Information
    INSERT INTO YourMetaDatabase.JobHistory (JobName, LastRunDate, LastRunDuration, LastRunOutcome, ScheduleName, ScheduleStartTime)
    SELECT 
        j.name AS JobName,
        MAX(jh.run_date + jh.run_time * 9e-3) AS LastRunDate,
        MAX((jh.run_duration / 10000 * 3600 +
             jh.run_duration / 100 % 100 * 60 +
             jh.run_duration % 100) * 1e-2) AS LastRunDuration,
        CASE jh.run_status
            WHEN 0 THEN 'Failed'
            WHEN 1 THEN 'Succeeded'
            ELSE 'Unknown'
        END AS LastRunOutcome,
        s.name AS ScheduleName,
        CONVERT(TIME, s.active_start_time) AS ScheduleStartTime
    FROM msdb.dbo.sysjobs j
    JOIN msdb.dbo.sysjobhistory jh ON j.job_id = jh.job_id
    LEFT JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
    LEFT JOIN msdb.dbo.sysschedules s ON js.schedule_id = s.schedule_id
    WHERE jh.step_id = 0
    GROUP BY j.name, s.name, s.active_start_time;
END;
---------------------------------------------------------------------
CREATE PROCEDURE sp_CollectErrorLogAndJobInfo
AS
BEGIN
    SET NOCOUNT ON;

    -- Error Log Information
    INSERT INTO YourMetaDatabase.ErrorLog (LogDate, ProcessInfo, Text)
    EXEC xp_readerrorlog;

    -- Job History Information
    INSERT INTO YourMetaDatabase.JobHistory (JobName, LastRunDate, LastRunDuration, LastRunOutcome, ScheduleName, ScheduleStartTime)
    SELECT 
        j.name AS JobName,
        MAX(jh.run_date + jh.run_time * 9e-3) AS LastRunDate,
        MAX((jh.run_duration / 10000 * 3600 +
             jh.run_duration / 100 % 100 * 60 +
             jh.run_duration % 100) * 1e-2) AS LastRunDuration,
        CASE jh.run_status
            WHEN 0 THEN 'Failed'
            WHEN 1 THEN 'Succeeded'
            ELSE 'Unknown'
        END AS LastRunOutcome,
        s.name AS ScheduleName,
        CONVERT(TIME, s.active_start_time) AS ScheduleStartTime
    FROM msdb.dbo.sysjobs j
    JOIN msdb.dbo.sysjobhistory jh ON j.job_id = jh.job_id
    LEFT JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
    LEFT JOIN msdb.dbo.sysschedules s ON js.schedule_id = s.schedule_id
    WHERE jh.step_id = 0
    GROUP BY j.name, s.name, s.active_start_time;
END;
---------------------------------------------------------------
1. Identify Currently Active Queries
You can use sys.dm_exec_requests to identify currently active queries that are consuming TempDB space:

sql
Copy code
SELECT
    r.session_id,
    r.request_id,
    r.command,
    r.database_id,
    r.wait_type,
    r.wait_time,
    r.wait_resource,
    r.total_elapsed_time,
    r.cpu_time,
    r.logical_reads,
    r.reads,
    r.writes,
    r.row_count,
    q.text AS QueryText
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) q
WHERE r.database_id = DB_ID('tempdb')
ORDER BY r.total_elapsed_time DESC;
2. Monitor TempDB Space Usage by Session
To monitor TempDB usage per session, you can query sys.dm_db_session_space_usage:

sql
Copy code
SELECT
    session_id,
    user_objects_alloc_page_count,
    internal_objects_alloc_page_count,
    user_objects_dealloc_page_count,
    internal_objects_dealloc_page_count
FROM sys.dm_db_session_space_usage
WHERE database_id = DB_ID('tempdb');
3. TempDB Task Allocation
You can also monitor TempDB allocation by tasks using sys.dm_db_task_space_usage:

sql
Copy code
SELECT
    session_id,
    request_id,
    task_allocations_count,
    task_deallocations_count,
    task_used_pages_count
FROM sys.dm_db_task_space_usage
WHERE database_id = DB_ID('tempdb');
4. Monitor TempDB File Usage
To check TempDB file usage and growth:

sql
Copy code
SELECT
    file_id,
    type_desc,
    name AS logical_name,
    physical_name,
    size_mb = size * 8 / 1024,
    growth = CASE WHEN is_percent_growth = 1 THEN growth * 8 / 1024 * 100 ELSE growth * 8 / 1024 END,
    max_size_mb = CASE WHEN max_size = -1 THEN 'Unlimited' ELSE max_size * 8 / 1024 END
FROM tempdb.sys.database_files;
5. TempDB Version Store Usage
If your workload heavily uses features like Snapshot Isolation or Read Committed Snapshot Isolation, monitor TempDB version store space usage:

sql
Copy code
SELECT
    version_store_reserved_page_count,
    version_store_used_page_count,
    maximum_version_store_size_kb
FROM sys.dm_tran_version_store_space_usage;
--------------------------------------------------
CREATE PROCEDURE sp_CollectDatabaseInfo
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ServerName NVARCHAR(128);
    SET @ServerName = @@SERVERNAME;

    -- Collect Server Information
    INSERT INTO YourMetaDatabase.DatabaseServers (ServerName, Version, Edition, ProductLevel, EngineEdition, Collation)
    SELECT 
        @ServerName, 
        SERVERPROPERTY('ProductVersion'), 
        SERVERPROPERTY('Edition'), 
        SERVERPROPERTY('ProductLevel'), 
        SERVERPROPERTY('EngineEdition'), 
        SERVERPROPERTY('Collation');

    -- Collect Databases Information
    INSERT INTO YourMetaDatabase.Databases (ServerName, DatabaseID, DatabaseName, Collation, RecoveryModel, CompatibilityLevel, SizeMB)
    SELECT 
        @ServerName,
        db.database_id, 
        db.name, 
        db.collation_name, 
        db.recovery_model_desc, 
        db.compatibility_level, 
        SUM(mf.size) * 8.0 / 1024
    FROM 
        sys.databases db
    JOIN 
        sys.master_files mf ON db.database_id = mf.database_id
    GROUP BY 
        db.database_id, db.name, db.collation_name, db.recovery_model_desc, db.compatibility_level;

    -- Collect Users Information
    INSERT INTO YourMetaDatabase.Users (ServerName, DatabaseName, UserID, UserName, UserType, DefaultSchema, CreateDate, ModifyDate)
    SELECT 
        @ServerName,
        DB_NAME(), 
        dp.principal_id, 
        dp.name, 
        dp.type_desc, 
        dp.default_schema_name, 
        dp.create_date, 
        dp.modify_date
    FROM 
        sys.database_principals dp
    WHERE 
        dp.type IN ('S', 'U', 'G'); 

    -- Collect Roles Information
    INSERT INTO YourMetaDatabase.Roles (ServerName, DatabaseName, RoleID, RoleName, RoleType, CreateDate, ModifyDate)
    SELECT 
        @ServerName,
        DB_NAME(), 
        rp.principal_id, 
        rp.name, 
        rp.type_desc, 
        rp.create_date, 
        rp.modify_date
    FROM 
        sys.database_principals rp
    WHERE 
        rp.type IN ('R', 'A');

    -- Collect Permissions Information
    INSERT INTO YourMetaDatabase.Permissions (ServerName, DatabaseName, PermissionID, ObjectType, PermissionType, PermissionState, ObjectName, GranteeName, GranteeType)
    SELECT 
        @ServerName,
        DB_NAME(), 
        dp.permission_id, 
        dp.class_desc, 
        dp.permission_name, 
        dp.state_desc, 
        OBJECT_NAME(dp.major_id), 
        pr.name, 
        pr.type_desc
    FROM 
        sys.database_permissions dp
    JOIN 
        sys.database_principals pr ON dp.grantee_principal_id = pr.principal_id;

    -- Collect Tables Information
    INSERT INTO YourMetaDatabase.Tables (ServerName, DatabaseName, TableID, TableName, SchemaName, RowCount)
    SELECT 
        @ServerName,
        DB_NAME(), 
        t.object_id, 
        t.name, 
        s.name, 
        p.rows
    FROM 
        sys.tables t
    JOIN 
        sys.schemas s ON t.schema_id = s.schema_id
    JOIN 
        sys.partitions p ON t.object_id = p.object_id
    WHERE 
        p.index_id IN (0, 1)
    GROUP BY 
        t.object_id, t.name, s.name, p.rows;

    -- Collect Columns Information
    INSERT INTO YourMetaDatabase.Columns (ServerName, DatabaseName, ColumnID, TableID, ColumnName, DataType, MaxLength, IsNullable, IsIdentity, IsComputed, DefaultValue)
    SELECT 
        @ServerName,
        DB_NAME(), 
        c.column_id, 
        t.object_id, 
        c.name, 
        tp.name, 
        c.max_length, 
        c.is_nullable, 
        c.is_identity, 
        c.is_computed, 
        dc.definition
    FROM 
        sys.columns c
    JOIN 
        sys.tables t ON c.object_id = t.object_id
    JOIN 
        sys.types tp ON c.user_type_id = tp.user_type_id
    LEFT JOIN 
        sys.default_constraints dc ON c.default_object_id = dc.object_id;

    -- Collect Indexes Information
    INSERT INTO YourMetaDatabase.Indexes (ServerName, DatabaseName, IndexID, TableID, IndexName, IndexType, IsUnique, IsPrimaryKey, ColumnsIncluded)
    SELECT 
        @ServerName,
        DB_NAME(), 
        i.index_id, 
        t.object_id, 
        i.name, 
        i.type_desc, 
        i.is_unique, 
        i.is_primary_key, 
        STUFF((SELECT ', ' + c.name
               FROM sys.index_columns ic
               JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
               WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
               FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
    FROM 
        sys.indexes i
    JOIN 
        sys.tables t ON i.object_id = t.object_id
    WHERE 
        i.type IN (1, 2);

    -- Collect Relationships Information
    INSERT INTO YourMetaDatabase.Relationships (ServerName, DatabaseName, RelationshipID, ForeignKeyName, ParentTableName, ReferencedTableName, ParentColumns, ReferencedColumns)
    SELECT 
        @ServerName,
        DB_NAME(), 
        fk.object_id, 
        fk.name, 
        p.name, 
        rf.name, 
        STUFF((SELECT ', ' + pc.name
               FROM sys.foreign_key_columns fkc
               JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
               WHERE fkc.constraint_object_id = fk.object_id
               FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''), 
        STUFF((SELECT ', ' + rc.name
               FROM sys.foreign_key_columns fkc
               JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
               WHERE fkc.constraint_object_id = fk.object_id
               FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
    FROM 
        sys.foreign_keys fk
    JOIN 
        sys.tables p ON fk.parent_object_id = p.object_id
    JOIN 
        sys.tables rf ON fk.referenced_object_id = rf.object_id;

    -- Collect Replication Information
    INSERT INTO YourMetaDatabase.ReplicationInfo (ServerName, PublicationName, PublicationDB, Publisher, PublicationType)
    SELECT 
        @ServerName,
        publication.name AS PublicationName,
        publication.publisher_db AS PublicationDB,
        publication.publisher AS Publisher,
        publication.publication_type AS PublicationType
    FROM 
        distribution.dbo.MSpublications AS publication
    UNION ALL
    SELECT 
        @ServerName,
        msdb.dbo.syspublications.name AS PublicationName,
        msdb.dbo.syspublications.publisher_db AS PublicationDB,
        msdb.dbo.sysservers.srvname AS Publisher,
        publication.publication_type AS PublicationType
    FROM 
        msdb.dbo.syspublications
    JOIN 
        master.dbo.sysservers ON msdb.dbo.syspublications.publisher_id = msdb.dbo.sysservers.srvid;

    -- Additional Information Collection (Add as needed)

END;
------------------------------------------------------------------
1. Create a Table to Store Long-Running Queries
First, create a table in your monitoring database (let's call it MonitoringDB) to store information about long-running queries:

sql
Copy code
USE MonitoringDB;

CREATE TABLE LongRunningQueries (
    QueryID INT IDENTITY(1,1) PRIMARY KEY,
    SessionID INT,
    StartTime DATETIME,
    DurationSeconds INT,
    QueryText NVARCHAR(MAX)
);
2. Create a SQL Server Agent Job
Create a SQL Server Agent job that runs periodically (e.g., every 5 minutes) to capture queries running for more than 10 minutes. Below is a script you can use:

sql
Copy code
USE msdb;
GO

EXEC dbo.sp_add_job
    @job_name = N'Capture Long-Running Queries',
    @enabled = 1,
    @description = N'Captures queries running more than 10 minutes and stores them in MonitoringDB.LongRunningQueries';

EXEC dbo.sp_add_jobstep
    @job_name = N'Capture Long-Running Queries',
    @step_name = N'Capture Queries',
    @subsystem = N'TSQL',
    @command = N'
INSERT INTO MonitoringDB.LongRunningQueries (SessionID, StartTime, DurationSeconds, QueryText)
SELECT
    session_id,
    start_time,
    DATEDIFF(SECOND, start_time, GETDATE()) AS DurationSeconds,
    (SELECT text
     FROM sys.dm_exec_sql_text(sql_handle)) AS QueryText
FROM sys.dm_exec_requests
WHERE start_time < DATEADD(MINUTE, -10, GETDATE()) -- Queries running more than 10 minutes
AND session_id > 50; -- Exclude system sessions';

EXEC dbo.sp_add_schedule
    @schedule_name = N'RunEvery5Minutes',
    @freq_type = 4, -- Interval
    @freq_interval = 1, -- Every 1 day
    @active_start_time = 0,
    @active_end_time = 235959;

EXEC dbo.sp_attach_schedule
    @job_name = N'Capture Long-Running Queries',
    @schedule_name = N'RunEvery5Minutes';

EXEC dbo.sp_add_jobserver
    @job_name = N'Capture Long-Running Queries';
GO
----------------------------------------------------------------------
CREATE PROCEDURE sp_InsertServerInfo
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ServerName VARCHAR(100);
    DECLARE @Version VARCHAR(50);
    DECLARE @OperatingSystem VARCHAR(100);
    DECLARE @Description TEXT;
    DECLARE @ServerType VARCHAR(50) = 'SQL Server';

    -- Get Server Name
    SET @ServerName = @@SERVERNAME;

    -- Get SQL Server Version
    SET @Version = CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)) + ' - ' + 
                   CAST(SERVERPROPERTY('ProductLevel') AS VARCHAR(50)) + ' - ' + 
                   CAST(SERVERPROPERTY('Edition') AS VARCHAR(50));

    -- Get Operating System
    CREATE TABLE #OSInfo (
        IndexID INT,
        Name NVARCHAR(255),
        Internal_Value INT,
        Character_Value NVARCHAR(255)
    );

    INSERT INTO #OSInfo EXEC xp_msver;

    SET @OperatingSystem = (SELECT Character_Value FROM #OSInfo WHERE Name = 'Platform') + ' ' + 
                           (SELECT Character_Value FROM #OSInfo WHERE Name = 'WindowsVersion') + ' ' +
                           (SELECT Character_Value FROM #OSInfo WHERE Name = 'ProcessorType');

    DROP TABLE #OSInfo;

    -- Insert Server Information
    INSERT INTO ServerInfo (ServerName, ServerType, Version, OperatingSystem, Description)
    VALUES (@ServerName, @ServerType, @Version, @OperatingSystem, @Description);
END;
---------------------------
CREATE PROCEDURE sp_InsertServerInfo
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ServerName VARCHAR(100);
    DECLARE @Version VARCHAR(100);
    DECLARE @OperatingSystem VARCHAR(255);
    DECLARE @Description VARCHAR(MAX);
    DECLARE @ServerType VARCHAR(50) = 'SQL Server';

    -- Get Server Name
    SET @ServerName = @@SERVERNAME;

    -- Get SQL Server Version
    SET @Version = CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)) + ' - ' + 
                   CAST(SERVERPROPERTY('ProductLevel') AS VARCHAR(50)) + ' - ' + 
                   CAST(SERVERPROPERTY('Edition') AS VARCHAR(50));

    -- Get Operating System
    CREATE TABLE #OSInfo (
        IndexID INT,
        Name NVARCHAR(255),
        Internal_Value INT,
        Character_Value NVARCHAR(255)
    );

    INSERT INTO #OSInfo EXEC xp_msver;

    SET @OperatingSystem = (SELECT Character_Value FROM #OSInfo WHERE Name = 'Platform') + ' ' + 
                           (SELECT Character_Value FROM #OSInfo WHERE Name = 'WindowsVersion') + ' ' +
                           (SELECT Character_Value FROM #OSInfo WHERE Name = 'ProcessorType');

    DROP TABLE #OSInfo;

    -- Insert Server Information
    INSERT INTO ServerInfo (ServerName, ServerType, Version, OperatingSystem, Description)
    VALUES (@ServerName, @ServerType, @Version, @OperatingSystem, @Description);
END;
-------------------------------DECLARE @SchemaName VARCHAR(100);
DECLARE @TableName VARCHAR(256);
DECLARE @IndexName VARCHAR(256);
DECLARE @ColumnName VARCHAR(100);
DECLARE @is_unique VARCHAR(100);
DECLARE @IndexTypeDesc VARCHAR(100);
DECLARE @FileGroupName VARCHAR(100);
DECLARE @is_disabled VARCHAR(100);
DECLARE @IndexOptions VARCHAR(MAX);
DECLARE @IndexColumnId INT;
DECLARE @IsDescendingKey INT;
DECLARE @IsIncludedColumn INT;
DECLARE @TSQLScripCreationIndex VARCHAR(MAX);
DECLARE @TSQLScripDisableIndex VARCHAR(MAX);

DECLARE CursorIndex CURSOR FOR
SELECT schema_name(t.schema_id) [schema_name], t.name AS TableName, ix.name AS IndexName,
       CASE WHEN ix.is_unique = 1 THEN 'UNIQUE ' ELSE '' END,
       ix.type_desc AS IndexTypeDesc,
       CASE WHEN ix.is_padded = 1 THEN 'PAD_INDEX = ON, ' ELSE 'PAD_INDEX = OFF, ' END
       + CASE WHEN ix.allow_page_locks = 1 THEN 'ALLOW_PAGE_LOCKS = ON, ' ELSE 'ALLOW_PAGE_LOCKS = OFF, ' END
       + CASE WHEN ix.allow_row_locks = 1 THEN 'ALLOW_ROW_LOCKS = ON, ' ELSE 'ALLOW_ROW_LOCKS = OFF, ' END
       + CASE WHEN INDEXPROPERTY(t.object_id, ix.name, 'IsStatistics') = 1 THEN 'STATISTICS_NORECOMPUTE = ON, ' ELSE 'STATISTICS_NORECOMPUTE = OFF, ' END
       + CASE WHEN ix.ignore_dup_key = 1 THEN 'IGNORE_DUP_KEY = ON, ' ELSE 'IGNORE_DUP_KEY = OFF, ' END
       + 'SORT_IN_TEMPDB = OFF, FILLFACTOR = ' + CAST(ix.fill_factor AS VARCHAR(3)) AS IndexOptions,
       ix.is_disabled, FILEGROUP_NAME(ix.data_space_id) AS FileGroupName
FROM sys.tables t
INNER JOIN sys.indexes ix ON t.object_id = ix.object_id
WHERE ix.type > 0 AND ix.is_primary_key = 0 AND ix.is_unique_constraint = 0
  AND t.is_ms_shipped = 0 AND t.name <> 'sysdiagrams'
ORDER BY schema_name(t.schema_id), t.name, ix.name;

OPEN CursorIndex;
FETCH NEXT FROM CursorIndex INTO @SchemaName, @TableName, @IndexName, @is_unique, @IndexTypeDesc, @IndexOptions, @is_disabled, @FileGroupName;

WHILE (@@FETCH_STATUS = 0)
BEGIN
    DECLARE @IndexColumns VARCHAR(MAX);
    DECLARE @IncludedColumns VARCHAR(MAX);
    
    SET @IndexColumns = '';
    SET @IncludedColumns = '';
    
    DECLARE CursorIndexColumn CURSOR FOR
    SELECT col.name, ixc.is_descending_key, ixc.is_included_column
    FROM sys.tables tb
    INNER JOIN sys.indexes ix ON tb.object_id = ix.object_id
    INNER JOIN sys.index_columns ixc ON ix.object_id = ixc.object_id AND ix.index_id = ixc.index_id
    INNER JOIN sys.columns col ON ixc.object_id = col.object_id AND ixc.column_id = col.column_id
    WHERE ix.type > 0 AND (ix.is_primary_key = 0 OR ix.is_unique_constraint = 0)
      AND schema_name(tb.schema_id) = @SchemaName AND tb.name = @TableName AND ix.name = @IndexName
    ORDER BY ixc.index_column_id;
    
    OPEN CursorIndexColumn;
    FETCH NEXT FROM CursorIndexColumn INTO @ColumnName, @IsDescendingKey, @IsIncludedColumn;
    
    WHILE (@@FETCH_STATUS = 0)
    BEGIN
        IF @IsIncludedColumn = 0
            SET @IndexColumns = @IndexColumns + @ColumnName + CASE WHEN @IsDescendingKey = 1 THEN ' DESC, ' ELSE ' ASC, ' END;
        ELSE
            SET @IncludedColumns = @IncludedColumns + @ColumnName + ', ';
        
        FETCH NEXT FROM CursorIndexColumn INTO @ColumnName, @IsDescendingKey, @IsIncludedColumn;
    END;
    
    CLOSE CursorIndexColumn;
    DEALLOCATE CursorIndexColumn;
    
    SET @IndexColumns = SUBSTRING(@IndexColumns, 1, LEN(@IndexColumns) - 1);
    SET @IncludedColumns = CASE WHEN LEN(@IncludedColumns) > 0 THEN SUBSTRING(@IncludedColumns, 1, LEN(@IncludedColumns) - 1) ELSE '' END;
    
    SET @TSQLScripCreationIndex = '';
    SET @TSQLScripDisableIndex = '';
    
    SET @TSQLScripCreationIndex = 'IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = ''' + @IndexName + ''' AND object_id = OBJECT_ID(''' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + '''))' + CHAR(13)
        + 'BEGIN' + CHAR(13)
        + '    ALTER INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' REBUILD;' + CHAR(13)
        + 'END' + CHAR(13)
        + 'ELSE' + CHAR(13)
        + 'BEGIN' + CHAR(13)
        + '    CREATE ' + @is_unique + @IndexTypeDesc + ' INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' (' + @IndexColumns + ')' + CHAR(13)
        + CASE WHEN LEN(@IncludedColumns) > 0 THEN 'INCLUDE (' + @IncludedColumns + ')' ELSE '' END + CHAR(13)
        + '    WITH (' + @IndexOptions + ') ON ' + QUOTENAME(@FileGroupName) + ';' + CHAR(13)
        + 'END' + CHAR(13)
        + 'GO';
    
    IF @is_disabled = 1
        SET @TSQLScripDisableIndex = 'ALTER INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' DISABLE;' + CHAR(13)
            + 'GO';
    
    PRINT @TSQLScripCreationIndex;
    PRINT @TSQLScripDisableIndex;
    
    FETCH NEXT FROM CursorIndex INTO @SchemaName, @TableName, @IndexName, @is_unique, @IndexTypeDesc, @IndexOptions, @is_disabled, @FileGroupName;
END;

CLOSE CursorIndex;-- Check the status of the SQL Server Agent service
EXEC xp_servicecontrol 'QUERYSTATE', 'SQLServerAgent';

-- Check job history for errors
SELECT 
    job.name AS JobName,
    job.job_id AS JobID,
    history.run_date AS RunDate,
    history.run_time AS RunTime,
    history.run_duration AS RunDuration,
    history.message AS Message,
    CASE 
        WHEN history.run_status = 0 THEN 'Failed'
        WHEN history.run_status = 1 THEN 'Succeeded'
        WHEN history.run_status = 2 THEN 'Retry'
        WHEN history.run_status = 3 THEN 'Canceled'
        WHEN history.run_status = 4 THEN 'In Progress'
        ELSE 'Unknown'
    END AS RunStatus
FROM msdb.dbo.sysjobs AS job
JOIN msdb.dbo.sysjobhistory AS history ON job.job_id = history.job_id
ORDER BY history.run_date DESC, history.run_time DESC;

-- Check job schedule
SELECT 
    jobs.name AS JobName,
    schedules.name AS ScheduleName,
    schedules.freq_type AS FrequencyType,
    schedules.freq_interval AS FrequencyInterval,
    schedules.freq_subday_type AS SubdayType,
    schedules.freq_subday_interval AS SubdayInterval,
    schedules.active_start_date AS StartDate,
    schedules.active_end_date AS EndDate,
    schedules.active_start_time AS StartTime,
    schedules.active_end_time AS EndTime
FROM msdb.dbo.sysschedules AS schedules
JOIN msdb.dbo.sysjobschedules AS jobschedules ON schedules.schedule_id = jobschedules.schedule_id
JOIN msdb.dbo.sysjobs AS jobs ON jobschedules.job_id = jobs.job_id
WHERE jobs.enabled = 1
ORDER BY jobs.name;

-- Check for blocking or long-running jobs
SELECT 
    blocking_session_id AS BlockingSessionID,
    session_id AS SessionID,
    wait_type AS WaitType,
    wait_time AS WaitTime,
    wait_resource AS WaitResource,
    blocking_session_id
FROM sys.dm_exec_requests
WHERE blocking_session_id <> 0;

-- Check SQL Server Agent operators
SELECT 
    name AS OperatorName,
    enabled AS Enabled,
    email_address AS EmailAddress,
    pager_address AS PagerAddress
FROM msdb.dbo.sysoperators;
------------------------------
-- Declare a table variable to store server list
DECLARE @ServerList TABLE (ServerName VARCHAR(255));

-- Insert your server names into the @ServerList table variable
INSERT INTO @ServerList (ServerName)
SELECT server_name 
FROM msdb.dbo.sysmanagement_shared_registered_servers_internal
WHERE server_name IS NOT NULL;

-- Declare variables to store server name and dynamic SQL
DECLARE @ServerName VARCHAR(255);
DECLARE @SQL NVARCHAR(MAX);

-- Create a table to store the results
IF OBJECT_ID('tempdb..#AgentLogHistoryRetention') IS NOT NULL
    DROP TABLE #AgentLogHistoryRetention;

CREATE TABLE #AgentLogHistoryRetention (
    ServerName VARCHAR(255),
    LogRetentionPeriod INT,
    LogFileSize INT,
    LogFileCount INT
);

-- Cursor to iterate over each server
DECLARE ServerCursor CURSOR FOR
SELECT ServerName
FROM @ServerList;

OPEN ServerCursor;
FETCH NEXT FROM ServerCursor INTO @ServerName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Construct dynamic SQL to execute on each server
    SET @SQL = 'INSERT INTO #AgentLogHistoryRetention (ServerName, LogRetentionPeriod, LogFileSize, LogFileCount)
                SELECT ''' + @ServerName + ''',
                       sagentjobhistoryretention.period, 
                       sagentjobhistoryretention.max_log_file_size, 
                       sagentjobhistoryretention.max_log_file_count
                FROM [' + @ServerName + '].msdb.dbo.syssubsystems AS s
                INNER JOIN [' + @ServerName + '].msdb.dbo.msdb_sysjobs AS j
                ON s.subsystem_id = j.subsystem
                INNER JOIN [' + @ServerName + '].msdb.dbo.sysjobschedules AS js
                ON j.job_id = js.job_id
                INNER JOIN [' + @ServerName + '].msdb.dbo.sysjobs AS sj
                ON j.job_id = sj.job_id
                INNER JOIN [' + @ServerName + '].msdb.dbo.sagentjobhistoryretention
                ON s.subsystem_id = sagentjobhistoryretention.subsystem_id
                WHERE s.subsystem = ''SQL Agent'';';
                
    -- Execute the dynamic SQL
    EXEC sp_executesql @SQL;

    FETCH NEXT FROM ServerCursor INTO @ServerName;
END;

CLOSE ServerCursor;
DEALLOCATE ServerCursor;

-- Select the results
SELECT *
FROM #AgentLogHistoryRetention;

-- Drop the temporary table
DROP TABLE #AgentLogHistoryRetention;
USE msdb;

-- Query to check SQL Server Agent log history retention settings
SELECT 
    s.server_name AS ServerName,
    sagentjobhistoryretention.period AS LogRetentionPeriod, 
    sagentjobhistoryretention.max_log_file_size AS LogFileSize, 
    sagentjobhistoryretention.max_log_file_count AS LogFileCount
FROM msdb.dbo.syssubsystems AS s
INNER JOIN msdb.dbo.msdb_sysjobs AS j ON s.subsystem_id = j.subsystem
INNER JOIN msdb.dbo.sysjobschedules AS js ON j.job_id = js.job_id
INNER JOIN msdb.dbo.sysjobs AS sj ON j.job_id = sj.job_id
INNER JOIN msdb.dbo.sagentjobhistoryretention ON s.subsystem_id = sagentjobhistoryretention.subsystem_id
WHERE s.subsystem = 'SQL Agent';
DECLARE @ServerName VARCHAR(255);

-- List of servers
DECLARE ServerCursor CURSOR FOR
SELECT server_name 
FROM msdb.dbo.sysmanagement_shared_registered_servers_internal
WHERE server_name IS NOT NULL;

OPEN ServerCursor;
FETCH NEXT FROM ServerCursor INTO @ServerName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Print the query for each server
    PRINT 'USE [' + @ServerName + '];'
    PRINT 'SELECT '
        + '''' + @ServerName + ''' AS ServerName,'
        + 'sagentjobhistoryretention.period AS LogRetentionPeriod,'
        + 'sagentjobhistoryretention.max_log_file_size AS LogFileSize,'
        + 'sagentjobhistoryretention.max_log_file_count AS LogFileCount'
        + ' FROM [' + @ServerName + '].msdb.dbo.syssubsystems AS s'
        + ' INNER JOIN [' + @ServerName + '].msdb.dbo.msdb_sysjobs AS j ON s.subsystem_id = j.subsystem'
        + ' INNER JOIN [' + @ServerName + '].msdb.dbo.sysjobschedules AS js ON j.job_id = js.job_id'
        + ' INNER JOIN [' + @ServerName + '].msdb.dbo.sysjobs AS sj ON j.job_id = sj.job_id'
        + ' INNER JOIN [' + @ServerName + '].msdb.dbo.sagentjobhistoryretention ON s.subsystem_id = sagentjobhistoryretention.subsystem_id'
        + ' WHERE s.subsystem = ''SQL Agent'';';

    FETCH NEXT FROM ServerCursor INTO @ServerName;
END;

CLOSE ServerCursor;
DEALLOCATE ServerCursor;
------------------------------------------------
USE msdb;

-- Query to check SQL Server Agent log history retention settings
SELECT 
    @@SERVERNAME AS ServerName,
    CASE 
        WHEN value_in_use = 1 THEN 'Retain' 
        ELSE 'Remove' 
    END AS LogRetentionPeriod,
    'Not applicable' AS LogFileSize,
    'Not applicable' AS LogFileCount
FROM msdb.dbo.sysjobschedules AS js
INNER JOIN msdb.dbo.sysjobs AS j ON js.job_id = j.job_id
WHERE j.enabled = 1;
-----------------------------------------------
DECLARE @ServerName VARCHAR(255);

-- List of servers
DECLARE ServerCursor CURSOR FOR
SELECT server_name 
FROM msdb.dbo.sysmanagement_shared_registered_servers_internal
WHERE server_name IS NOT NULL;

OPEN ServerCursor;
FETCH NEXT FROM ServerCursor INTO @ServerName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Print the query for each server
    PRINT 'USE [' + @ServerName + '];'
    PRINT 'SELECT '
        + '''' + @ServerName + ''' AS ServerName,'
        + 'CASE '
        + 'WHEN value_in_use = 1 THEN ''Retain'' '
        + 'ELSE ''Remove'' '
        + 'END AS LogRetentionPeriod,'
        + '''Not applicable'' AS LogFileSize,'
        + '''Not applicable'' AS LogFileCount'
        + ' FROM [' + @ServerName + '].msdb.dbo.sysjobschedules AS js'
        + ' INNER JOIN [' + @ServerName + '].msdb.dbo.sysjobs AS j ON js.job_id = j.job_id'
        + ' WHERE j.enabled = 1;';

    FETCH NEXT FROM ServerCursor INTO @ServerName;
END;

CLOSE ServerCursor;
DEALLOCATE ServerCursor;
--------------------------------
DECLARE @ServerName VARCHAR(255);

-- List of servers
DECLARE ServerCursor CURSOR FOR
SELECT server_name 
FROM msdb.dbo.sysmanagement_shared_registered_servers_internal
WHERE server_name IS NOT NULL;

OPEN ServerCursor;
FETCH NEXT FROM ServerCursor INTO @ServerName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Generate the query for each server
    DECLARE @SQL NVARCHAR(MAX) = N'
    EXEC(''USE [' + @ServerName + '];
    
    -- Query to check SQL Server Agent log history retention settings
    SELECT 
        @@SERVERNAME AS ServerName,
        (SELECT value 
         FROM msdb.dbo.sysjobs AS j
         INNER JOIN msdb.dbo.sysjobschedules AS js ON j.job_id = js.job_id
         WHERE j.enabled = 1
           AND js.enabled = 1
           AND name = ''HistoryRetentionDays'') AS LogRetentionPeriod
    UNION ALL
    -- Query to check SQL Server Agent job schedule information
    SELECT 
        @@SERVERNAME AS ServerName,
        s.name AS ScheduleName,
        j.name AS JobName,
        CASE 
            WHEN s.enabled = 1 THEN ''Enabled''
            ELSE ''Disabled''
        END AS ScheduleStatus,
        sch.next_run_date AS NextRunDate,
        sch.next_run_time AS NextRunTime
    FROM msdb.dbo.sysjobs AS j
    INNER JOIN msdb.dbo.sysjobschedules AS js ON j.job_id = js.job_id
    INNER JOIN msdb.dbo.sysschedules AS s ON js.schedule_id = s.schedule_id
    INNER JOIN msdb.dbo.sysjobs_view AS sch ON sch.job_id = j.job_id
    WHERE j.enabled = 1;
    '')';

    -- Execute the generated query for each server
    EXEC sp_executesql @SQL;

    FETCH NEXT FROM ServerCursor INTO @ServerName;
END;

CLOSE ServerCursor;
DEALLOCATE ServerCursor;
Step 1: List Registered Servers in the CMS
First, verify that we have the correct list of registered servers. The CMS setup might store the server information differently. Let's try another query to list the registered servers:

sql
Copy code
SELECT srv.name AS server_name
FROM msdb.dbo.sysmanagement_shared_server_groups_internal grp
JOIN msdb.dbo.sysmanagement_shared_registered_servers_internal srv
    ON grp.server_group_id = srv.server_group_id
ORDER BY srv.name;
Step 2: Check SQL Server Agent Log History Retention Settings and Job Schedule Information
After obtaining the list of registered servers, we'll use the following script to query each server:

sql
Copy code
DECLARE @ServerName NVARCHAR(128);

-- Cursor to iterate through the list of registered servers
DECLARE ServerCursor CURSOR FOR
SELECT srv.name AS server_name
FROM msdb.dbo.sysmanagement_shared_server_groups_internal grp
JOIN msdb.dbo.sysmanagement_shared_registered_servers_internal srv
    ON grp.server_group_id = srv.server_group_id
ORDER BY srv.name;

OPEN ServerCursor;
FETCH NEXT FROM ServerCursor INTO @ServerName;

WHILE @@FETCH_STATUS = 0
BEGIN
    DECLARE @SQL NVARCHAR(MAX);
    
    -- Construct dynamic SQL to query each server
    SET @SQL = N'
    EXEC(''USE [' + @ServerName + '];
    SELECT 
        @@SERVERNAME AS ServerName,
        (SELECT value_in_use 
         FROM sys.configurations 
         WHERE name = ''agent_log_history_retention'') AS LogRetentionPeriod
    UNION ALL
    SELECT 
        @@SERVERNAME AS ServerName,
        s.name AS ScheduleName,
        j.name AS JobName,
        CASE 
            WHEN s.enabled = 1 THEN ''Enabled''
            ELSE ''Disabled''
        END AS ScheduleStatus,
        CAST(sch.next_run_date AS VARCHAR(8)) + '' '' + STUFF(STUFF(RIGHT(REPLICATE(''0'', 6) + CAST(sch.next_run_time AS VARCHAR(6)), 6), 3, 0, '':'') , 6, 0, '':'') AS NextRunDateTime
    FROM msdb.dbo.sysjobs AS j
    INNER JOIN msdb.dbo.sysjobschedules AS js ON j.job_id = js.job_id
    INNER JOIN msdb.dbo.sysschedules AS s ON js.schedule_id = s.schedule_id
    INNER JOIN msdb.dbo.sysjobs_view AS sch ON sch.job_id = j.job_id
    WHERE j.enabled = 1;
    '')';

    -- Execute the dynamic SQL for each server
    EXEC sp_executesql @SQL;

    FETCH NEXT FROM ServerCursor INTO @ServerName;
END;

CLOSE ServerCursor;
DEALLOCATE ServerCursor;
USE msdb;
GO

-- Query to check SQL Server Agent log history retention settings
SELECT 
    instance_id AS InstanceID,
    CASE
        WHEN history_retention = -1 THEN 'Retain indefinitely'
        ELSE CAST(history_retention AS VARCHAR) + ' days'
    END AS LogHistoryRetention
FROM dbo.sysjobs_view
WHERE job_id = '00000000-0000-0000-0000-000000000000'; -- The system job for log history retention
GO
------------------------------------
USE msdb;
GO

-- Query to check SQL Server Agent job history retention settings
SELECT 
    name AS JobName,
    CASE
        WHEN date_created < DATEADD(DAY, -CAST(CAST(properties.value AS VARCHAR) AS INT), GETDATE()) THEN 'To be deleted'
        ELSE 'To be retained'
    END AS JobRetentionStatus,
    properties.value AS RetentionDays
FROM dbo.sysjobs AS jobs
CROSS APPLY
(
    SELECT CAST(spro.value AS INT) AS value
    FROM msdb.dbo.sysjobsteps AS sjs
    JOIN msdb.dbo.sysjobschedules AS sjsch
    ON sjs.job_id = sjsch.job_id
    CROSS APPLY msdb.dbo.syspolicy_conditions AS spc
    CROSS APPLY msdb.dbo.syspolicy_policies AS spp
    CROSS APPLY msdb.dbo.syspolicy_system_health_conditions AS sshc
    WHERE jobs.job_id = sjs.job_id
) AS properties
WHERE jobs.name = 'SQLAgent - Job History Retention';
GO
DECLARE @ServerName NVARCHAR(128);

-- Cursor to iterate through the list of servers
DECLARE ServerCursor CURSOR FOR
SELECT ServerName FROM #ServerList;

OPEN ServerCursor;
FETCH NEXT FROM ServerCursor INTO @ServerName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Dynamic SQL to execute on each server
    DECLARE @SQL NVARCHAR(MAX);

    -- Query for SQL Server Agent log history retention settings
    SET @SQL = N'
    USE msdb;
    SELECT
        ''' + @ServerName + ''' AS ServerName,
        CASE 
            WHEN EXISTS (SELECT 1 FROM msdb.dbo.sysjobhistory WHERE run_status = 1) THEN ''Log history retention is enabled''
            ELSE ''Log history retention is disabled''
        END AS LogHistoryRetentionStatus,
        CASE 
            WHEN category_id = 0 THEN ''Default retention period''
            ELSE CAST(category_id AS VARCHAR) + '' days''
        END AS RetentionPeriod
    FROM msdb.dbo.sysjobs
    WHERE name = ''Agent history clean up: distribution'';

    -- Query for job schedule information
    SELECT 
        ''' + @ServerName + ''' AS ServerName,
        s.name AS ScheduleName,
        j.name AS JobName,
        CASE 
            WHEN s.enabled = 1 THEN ''Enabled''
            ELSE ''Disabled''
        END AS ScheduleStatus,
        CAST(sch.next_run_date AS VARCHAR(8)) + '' '' + 
        STUFF(STUFF(RIGHT(REPLICATE(''0'', 6) + CAST(sch.next_run_time AS VARCHAR(6)), 6), 3, 0, '':'') , 6, 0, '':'') AS NextRunDateTime
    FROM msdb.dbo.sysjobs AS j
    INNER JOIN msdb.dbo.sysjobschedules AS js ON j.job_id = js.job_id
    INNER JOIN msdb.dbo.sysschedules AS s ON js.schedule_id = s.schedule_id
    INNER JOIN msdb.dbo.sysjobs_view AS sch ON sch.job_id = j.job_id
    WHERE j.enabled = 1;';

    -- Execute the dynamic SQL on each server
    EXEC (@SQL) AT @ServerName;

    FETCH NEXT FROM ServerCursor INTO @ServerName;
END;

CLOSE ServerCursor;
DEALLOCATE ServerCursor;

-- Drop the temporary table
DROP TABLE #ServerList;
CREATE CLUSTERED INDEX IX_SGEI_PK1 ON #SGEI (PK1);

-- Non-clustered index example for the ROW_NUMBER() partitioning
CREATE NONCLUSTERED INDEX IX_SGEI_Partitioning
    ON #SGEI (UserPK1, CoursePK1, AssignmentPK1, MembershipPK1, GradePK1);
	-----------------------------------------------------
	CREATE NONCLUSTERED INDEX IX_GradeExtractImport_User_Course_Assignment_Membership_Grade
ON [stage].[GradeExtractImport] ([UserPK1], [CoursePK1], [AssignmentPK1], [MembershipPK1], [GradePK1]);

CREATE NONCLUSTERED INDEX IX_GradeExtractImport_Covering
ON [stage].[GradeExtractImport] (
    [UserPK1], [CoursePK1], [AssignmentPK1], [MembershipPK1], [GradePK1]
DEALLOCATE CursorIndex;
-----------------
ALTER PROCEDURE YourStoredProcedure
AS
BEGIN
    DECLARE @StartTime DATETIME;
    DECLARE @EndTime DATETIME;
    DECLARE @LogID INT;
    DECLARE @Step NVARCHAR(128);

    -- Insert start log for the entire procedure
    SET @StartTime = GETDATE();
    INSERT INTO dbo.ProcedureExecutionLog (ProcedureName, Step, StartTime, Status)
    VALUES (OBJECT_NAME(@@PROCID), 'Procedure Start', @StartTime, 'Started');

    SET @LogID = SCOPE_IDENTITY();

    BEGIN TRY
        -- Step 1: Log before SELECT INTO
        SET @Step = 'SELECT INTO #SGEI';
        SET @StartTime = GETDATE();
        INSERT INTO dbo.ProcedureExecutionLog (ProcedureName, Step, StartTime, Status)
        VALUES (OBJECT_NAME(@@PROCID), @Step, @StartTime, 'Started');

        -- SELECT INTO #SGEI
        DROP TABLE IF EXISTS #SGEI;
        SELECT * 
        INTO #SGEI
        FROM [stage].[GradeExtractImport_d2l];

        -- Update log after SELECT INTO
        SET @EndTime = GETDATE();
        UPDATE dbo.ProcedureExecutionLog
        SET EndTime = @EndTime, Status = 'Completed'
        WHERE ProcedureName = OBJECT_NAME(@@PROCID) AND Step = @Step AND LogID = SCOPE_IDENTITY();

        -- Step 2: Log before INSERT INTO Stage.ODS_Duplicates
        SET @Step = 'INSERT INTO Stage.ODS_Duplicates';
        SET @StartTime = GETDATE();
        INSERT INTO dbo.ProcedureExecutionLog (ProcedureName, Step, StartTime, Status)
        VALUES (OBJECT_NAME(@@PROCID), @Step, @StartTime, 'Started');

        -- INSERT INTO Stage.ODS_Duplicates
        WITH cte AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY UserPK1, CoursePK1, AssignmentPK1, MembershipPK1, GradePK1 ORDER BY (SELECT NULL)) AS rn
            FROM #SGEI
        )
        INSERT INTO Stage.ODS_Duplicates
        SELECT PK1 AS PrimaryKey, 'Grade_Merge' AS STEP_FAILED_ON, CONVERT(DATE, GETDATE()) AS PROCCESED_ON
        FROM cte
        WHERE cte.rn > 1;

        -- Update log after INSERT INTO
        SET @EndTime = GETDATE();
        UPDATE dbo.ProcedureExecutionLog
        SET EndTime = @EndTime, Status = 'Completed'
        WHERE ProcedureName = OBJECT_NAME(@@PROCID) AND Step = @Step AND LogID = SCOPE_IDENTITY();

        -- Log end of the procedure
        SET @EndTime = GETDATE();
        UPDATE dbo.ProcedureExecutionLog
        SET EndTime = @EndTime, Status = 'Completed'
        WHERE LogID = @LogID;
    END TRY
    BEGIN CATCH
        -- Capture error details
        DECLARE @ErrorMessage NVARCHAR(4000);
        SET @EndTime = GETDATE();
        SET @ErrorMessage = ERROR_MESSAGE();

        -- Update log with error for the current step
        UPDATE dbo.ProcedureExecutionLog
        SET EndTime = @EndTime, Status = 'Failed: ' + @ErrorMessage
        WHERE ProcedureName = OBJECT_NAME(@@PROCID) AND Step = @Step AND LogID = SCOPE_IDENTITY();

        -- Update log with error for the entire procedure
        UPDATE dbo.ProcedureExecutionLog
        SET EndTime = @EndTime, Status = 'Failed: ' + @ErrorMessage
        WHERE LogID = @LogID;

        -- Rethrow the error
        THROW;
    END CATCH
END;
--------------------------------------------------------------------------------
SELECT 
    mid.statement AS 'TableName',
    mid.equality_columns AS 'EqualityColumns',
    mid.inequality_columns AS 'InequalityColumns',
    mid.included_columns AS 'IncludedColumns',
    migs.user_seeks AS 'UserSeeks',
    migs.user_scans AS 'UserScans',
    migs.avg_total_user_cost AS 'AvgTotalUserCost',
    migs.avg_user_impact AS 'AvgUserImpact',
    migs.last_user_seek AS 'LastUserSeek',
    'CREATE INDEX [IX_' + mid.statement + '_MissingIndex] ON ' + mid.statement + 
    ' (' + ISNULL(mid.equality_columns,'') + 
    CASE WHEN mid.inequality_columns IS NULL THEN '' ELSE 
    CASE WHEN mid.equality_columns IS NULL THEN '' ELSE ',' END + mid.inequality_columns END + ')' +
    ISNULL(' INCLUDE (' + mid.included_columns + ')', '') AS 'CreateIndexStatement'
FROM 
    sys.dm_db_missing_index_groups mig
INNER JOIN 
    sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle
INNER JOIN 
    sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
WHERE 
    migs.user_seeks > 0
ORDER BY 
    migs.avg_user_impact DESC, migs.last_user_seek DESC;
---------------------------------------------------------------------------------------
-- List tables without nonclustered indexes
SELECT 
    t.name AS TableName,
    s.name AS SchemaName
FROM 
    sys.tables t
INNER JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
WHERE 
    t.object_id NOT IN (
        SELECT DISTINCT 
            i.object_id
        FROM 
            sys.indexes i
        WHERE 
            i.type = 2 -- Nonclustered indexes
    )
ORDER BY 
    s.name, t.name;
CREATE PROCEDURE dbo.GrantRoleAccess
    @RoleName NVARCHAR(128),
    @AccessType NVARCHAR(10), -- e.g., 'SELECT', 'EXECUTE', 'CONTROL'
    @ObjectType NVARCHAR(20), -- e.g., 'TABLE', 'PROC', 'JOB', 'VIEW'
    @ObjectName NVARCHAR(128) -- e.g., 'dbo.MyTable', 'dbo.MyProc', 'MyJob', 'dbo.MyView'
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX)
    
    -- Initialize the dynamic SQL
    SET @SQL = ''

    -- Construct the dynamic SQL based on object type and access type
    IF @ObjectType = 'TABLE' OR @ObjectType = 'VIEW'
    BEGIN
        IF @AccessType IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
        BEGIN
            SET @SQL = 'GRANT ' + @AccessType + ' ON ' + @ObjectName + ' TO [' + @RoleName + '];'
        END
        ELSE IF @AccessType = 'CONTROL'
        BEGIN
            SET @SQL = 'GRANT CONTROL ON ' + @ObjectName + ' TO [' + @RoleName + '];'
        END
    END
    ELSE IF @ObjectType = 'PROC'
    BEGIN
        IF @AccessType = 'EXECUTE'
        BEGIN
            SET @SQL = 'GRANT EXECUTE ON ' + @ObjectName + ' TO [' + @RoleName + '];'
        END
        ELSE IF @AccessType = 'CONTROL'
        BEGIN
            SET @SQL = 'GRANT CONTROL ON ' + @ObjectName + ' TO [' + @RoleName + '];'
        END
    END
    ELSE IF @ObjectType = 'JOB'
    BEGIN
        -- Granting control permissions on SQL Server Agent jobs
        IF @AccessType = 'CONTROL'
        BEGIN
            SET @SQL = 'USE msdb;
                        GRANT CONTROL ON OBJECT::[dbo].[sysjobs] TO [' + @RoleName + '];
                        EXEC msdb.dbo.sp_add_jobserver @job_name = ''' + @ObjectName + ''', @server_name = ''(local)'';'
        END
    END

    -- Print the SQL for debugging purposes (optional)
    PRINT @SQL

    -- Execute the dynamic SQL
    EXEC sp_executesql @SQL
END
CREATE PROCEDURE dbo.GrantAccess
    @UserName NVARCHAR(128),
    @AccessType NVARCHAR(10), -- e.g., 'SELECT', 'EXECUTE', 'CONTROL'
    @ObjectType NVARCHAR(20), -- e.g., 'TABLE', 'PROC', 'JOB', 'VIEW'
    @ObjectName NVARCHAR(128) -- e.g., 'dbo.MyTable', 'dbo.MyProc', 'MyJob', 'dbo.MyView'
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX)
    
    -- Initialize the dynamic SQL
    SET @SQL = ''

    -- Construct the dynamic SQL based on object type and access type
    IF @ObjectType = 'TABLE' OR @ObjectType = 'VIEW'
    BEGIN
        IF @AccessType IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
        BEGIN
            SET @SQL = 'GRANT ' + @AccessType + ' ON ' + @ObjectName + ' TO [' + @UserName + '];'
        END
        ELSE IF @AccessType = 'CONTROL'
        BEGIN
            SET @SQL = 'GRANT CONTROL ON ' + @ObjectName + ' TO [' + @UserName + '];'
        END
    END
    ELSE IF @ObjectType = 'PROC'
    BEGIN
        IF @AccessType = 'EXECUTE'
        BEGIN
            SET @SQL = 'GRANT EXECUTE ON ' + @ObjectName + ' TO [' + @UserName + '];'
        END
        ELSE IF @AccessType = 'CONTROL'
        BEGIN
            SET @SQL = 'GRANT CONTROL ON ' + @ObjectName + ' TO [' + @UserName + '];'
        END
    END
    ELSE IF @ObjectType = 'JOB'
    BEGIN
        -- Granting control permissions on SQL Server Agent jobs
        IF @AccessType = 'CONTROL'
        BEGIN
            SET @SQL = 'USE msdb;
                        GRANT CONTROL ON OBJECT::[dbo].[sysjobs] TO [' + @UserName + '];
                        EXEC msdb.dbo.sp_add_jobserver @job_name = ''' + @ObjectName + ''', @server_name = ''(local)'';'
        END
    END

    -- Print the SQL for debugging purposes (optional)
    PRINT @SQL

    -- Execute the dynamic SQL
    EXEC sp_executesql @SQL
END
CREATE PROCEDURE dbo.FindMissingIndexes
    @DatabaseName NVARCHAR(128)
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX)

    -- Initialize the dynamic SQL to find missing indexes
    SET @SQL = '
    USE ' + QUOTENAME(@DatabaseName) + ';
    
    SELECT
        DB_NAME(database_id) AS DatabaseName,
        OBJECT_SCHEMA_NAME(mid.[object_id], mid.database_id) AS SchemaName,
        OBJECT_NAME(mid.[object_id], mid.database_id) AS TableName,
        mid.equality_columns AS EqualityColumns,
        mid.inequality_columns AS InequalityColumns,
        mid.included_columns AS IncludedColumns,
        migs.unique_compiles AS UniqueCompiles,
        migs.user_seeks AS UserSeeks,
        migs.user_scans AS UserScans,
        migs.last_user_seek AS LastUserSeek,
        migs.last_user_scan AS LastUserScan,
        migs.avg_total_user_cost AS AvgTotalUserCost,
        migs.avg_user_impact AS AvgUserImpact,
        ''CREATE INDEX IX_'' + OBJECT_NAME(mid.[object_id], mid.database_id) + ''_'' + REPLACE(REPLACE(REPLACE(mid.equality_columns, '','', ''_''), ''['', ''''), '']'', '''') + '' ON '' + OBJECT_SCHEMA_NAME(mid.[object_id], mid.database_id) + ''.'' + OBJECT_NAME(mid.[object_id], mid.database_id) + '' ('' + ISNULL(mid.equality_columns, '''') +
        CASE WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL THEN '','' ELSE '''' END +
        ISNULL(mid.inequality_columns, '''') + '')'' +
        ISNULL('' INCLUDE ('' + mid.included_columns + '')'', '''') AS CreateIndexStatement
    FROM
        sys.dm_db_missing_index_group_stats AS migs
        INNER JOIN sys.dm_db_missing_index_groups AS mig ON migs.group_handle = mig.index_group_handle
        INNER JOIN sys.dm_db_missing_index_details AS mid ON mig.index_handle = mid.index_handle
    WHERE
        mid.database_id = DB_ID(@DatabaseName)
    ORDER BY
        migs.avg_user_impact DESC;
    '

    -- Execute the dynamic SQL
    EXEC sp_executesql @SQL, N'@DatabaseName NVARCHAR(128)', @DatabaseName
END
CREATE PROCEDURE dbo.DropIndexes
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX) = '';

    -- Step 1: Identify and Store Existing Indexes
    SELECT @SQL += 'DROP INDEX ' + QUOTENAME(i.name) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ';' + CHAR(13)
    FROM sys.indexes i
    WHERE i.object_id = OBJECT_ID(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName))
    AND i.type_desc NOT IN ('HEAP');

    -- Execute the generated SQL to drop indexes
    EXEC sp_executesql @SQL;

    SET NOCOUNT OFF;
END;
GO
CREATE PROCEDURE dbo.DropSpecificIndex
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(128),
    @IndexName NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);

    SET @SQL = 'DROP INDEX ' + QUOTENAME(@IndexName) +
               ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);

    EXEC sp_executesql @SQL;

    SET NOCOUNT OFF;
END;
GO
CREATE PROCEDURE dbo.DropSpecificIndex
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(128),
    @IndexName NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);

    IF EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName))
            AND name = @IndexName
    )
    BEGIN
        SET @SQL = 'DROP INDEX ' + QUOTENAME(@IndexName) +
                   ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);

        EXEC sp_executesql @SQL;
    END
    ELSE
    BEGIN
        PRINT 'Index ' + QUOTENAME(@IndexName) + ' on table ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' does not exist.';
    END

    SET NOCOUNT OFF;
END;
GO
CREATE PROCEDURE dbo.DropSpecificIndex
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(128),
    @IndexName NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);

    BEGIN TRY
        IF EXISTS (
            SELECT 1
            FROM sys.indexes
            WHERE object_id = OBJECT_ID(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName))
                AND name = @IndexName
        )
        BEGIN
            SET @SQL = 'DROP INDEX ' + QUOTENAME(@IndexName) +
                       ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);

            EXEC sp_executesql @SQL;
            PRINT 'Index ' + QUOTENAME(@IndexName) + ' on table ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' dropped successfully.';
        END
        ELSE
        BEGIN
            PRINT 'Index ' + QUOTENAME(@IndexName) + ' on table ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' does not exist.';
        END
    END TRY
    BEGIN CATCH
        PRINT 'Error occurred while dropping index ' + QUOTENAME(@IndexName) + ' on table ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);
        PRINT ERROR_MESSAGE();
    END CATCH

    SET NOCOUNT OFF;
END;
GOCREATE PROCEDURE dbo.CreateIncludeIndex
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(128),
    @IndexName NVARCHAR(128),
    @Column NVARCHAR(MAX),  -- Column to index
    @IncludeColumns NVARCHAR(MAX) = NULL  -- Comma-separated list of columns to include
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);

    -- Construct the CREATE INDEX statement
    SET @SQL = 'CREATE NONCLUSTERED INDEX ' + QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) +
               ' (' + QUOTENAME(@Column) + ')';
    
    -- Append INCLUDE clause if @IncludeColumns is provided and not empty
    IF @IncludeColumns IS NOT NULL AND @IncludeColumns <> ''
    BEGIN
        SET @SQL = @SQL + ' INCLUDE (' + @IncludeColumns + ')';
    END

    -- Append index options
    SET @SQL = @SQL + ' WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ' +
                     '       ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)';

    -- Execute the SQL statement
    EXEC sp_executesql @SQL;

    PRINT 'Index ' + @IndexName + ' created on ' + @SchemaName + '.' + @TableName;
END
EXEC dbo.CreateIncludeIndex 
    @SchemaName = 'stage',
    @TableName = 'GradeExtractImport',
    @IndexName = 'idx_GEI_0003',
    @Column = 'AssignmentDisplayColumnName',
    @IncludeColumns = 'CourseTitle, UserEPK';

	CREATE INDEX idx_gei_CoursePK1 ON stage.GradeExtractImport (CoursePK1);
CREATE INDEX idx_gei_CourseEPK ON stage.GradeExtractImport (CourseEPK);
CREATE INDEX idx_cm_PK1 ON COURSE_MAIN (PK1);
CREATE INDEX idx_cm_ROW_STATUS ON COURSE_MAIN (ROW_STATUS);

SELECT new.CoursePrimaryKey
FROM stage.Courses new
INNER JOIN LS_ODS.Courses old ON new.CoursePrimaryKey = old.CoursePrimaryKey
    AND old.ActiveFlag = 1
WHERE HASHBYTES('SHA1', CONCAT(
        ISNULL(CONVERT(VARCHAR(100), new.DateTimeCreated), ''),
        ISNULL(CONVERT(VARCHAR(100), new.DateTimeModified), ''),
        ISNULL(new.RowStatus, ''),
        ISNULL(new.BatchUniqueIdentifier, ''),
        ISNULL(new.CourseCode, ''),
        ISNULL(new.CourseName, ''),
        ISNULL(new.SectionNumber, ''),
        ISNULL(CONVERT(VARCHAR(100), new.SectionStart, 121), ''),
        ISNULL(CONVERT(VARCHAR(100), new.SectionEnd, 121), ''),
        ISNULL(new.AdClassSchedId, ''),
        ISNULL(new.WeekNumber, ''),
        ISNULL(new.Week1AssignmentCount, ''),
        ISNULL(new.Week2AssignmentCount, ''),
        ISNULL(new.Week3AssignmentCount, ''),
        ISNULL(new.Week4AssignmentCount, ''),
        ISNULL(new.Week5AssignmentCount, ''),
        ISNULL(new.PrimaryInstructor, ''),
        ISNULL(new.SecondaryInstructor, ''),
        ISNULL(CONVERT(VARCHAR(100), new.Week1StartDate, 121), ''),
        ISNULL(CONVERT(VARCHAR(100), new.Week2StartDate, 121), ''),
        ISNULL(CONVERT(VARCHAR(100), new.Week3StartDate, 121), ''),
        ISNULL(CONVERT(VARCHAR(100), new.Week4StartDate, 121), ''),
        ISNULL(CONVERT(VARCHAR(100), new.Week5StartDate, 121), ''),
        ISNULL(new.ExtensionWeekStartDate, ''),
        ISNULL(new.IsOrganization, ''),
        ISNULL(new.AcademicFacilitator, ''),
        ISNULL(new.PrimaryInstructorId, ''),
        ISNULL(new.SecondaryInstructorId, ''),
        ISNULL(new.AcademicFacilitatorId, ''),
        ISNULL(new.DayNumber, ''),
        ISNULL(new.CengageCourseIndicator, ''),
        ISNULL(new.SourceSystem, '')
    )) <> HASHBYTES('SHA1', CONCAT(
        ISNULL(CONVERT(VARCHAR(100), old.DateTimeCreated), ''),
        ISNULL(CONVERT(VARCHAR(100), old.DateTimeModified), ''),
        ISNULL(old.RowStatus, ''),
        ISNULL(old.BatchUniqueIdentifier, ''),
        ISNULL(old.CourseCode, ''),
        ISNULL(old.CourseName, ''),
        ISNULL(old.SectionNumber, ''),
        ISNULL(CONVERT(VARCHAR(100), old.SectionStart, 121), ''),
        ISNULL(CONVERT(VARCHAR(100), old.SectionEnd, 121), ''),
        ISNULL(old.AdClassSchedId, ''),
        ISNULL(old.WeekNumber, ''),
        ISNULL(old.Week1AssignmentCount, ''),
        ISNULL(old.Week2AssignmentCount, ''),
        ISNULL(old.Week3AssignmentCount, ''),
        ISNULL(old.Week4AssignmentCount, ''),
        ISNULL(old.Week5AssignmentCount, ''),
        ISNULL(old.PrimaryInstructor, ''),
        ISNULL(old.SecondaryInstructor, ''),
        ISNULL(CONVERT(VARCHAR(100), old.Week1StartDate, 121), ''),
        ISNULL(CONVERT(VARCHAR(100), old.Week2StartDate, 121), ''),
        ISNULL(CONVERT(VARCHAR(100), old.Week3StartDate, 121), ''),
        ISNULL(CONVERT(VARCHAR(100), old.Week4StartDate, 121), ''),
        ISNULL(CONVERT(VARCHAR(100), old.Week5StartDate, 121), ''),
        ISNULL(old.ExtensionWeekStartDate, ''),
        ISNULL(old.IsOrganization, ''),
        ISNULL(old.AcademicFacilitator, ''),
        ISNULL(old.PrimaryInstructorId, ''),
        ISNULL(old.SecondaryInstructorId, ''),
        ISNULL(old.AcademicFacilitatorId, ''),
        ISNULL(old.DayNumber, ''),
        ISNULL(old.CengageCourseIndicator, ''),
        ISNULL(old.SourceSystem, '')
    ));
	-------------------------------------
	SELECT 
    mid.statement AS 'TableName',
    mid.equality_columns AS 'EqualityColumns',
    mid.inequality_columns AS 'InequalityColumns',
    mid.included_columns AS 'IncludedColumns',
    migs.user_seeks AS 'UserSeeks',
    migs.user_scans AS 'UserScans',
    migs.avg_total_user_cost AS 'AvgTotalUserCost',
    migs.avg_user_impact AS 'AvgUserImpact',
    migs.last_user_seek AS 'LastUserSeek',
    'CREATE INDEX [IX_' + mid.statement + '_MissingIndex] ON ' + mid.statement + 
    ' (' + ISNULL(mid.equality_columns,'') + 
    CASE WHEN mid.inequality_columns IS NULL THEN '' ELSE 
    CASE WHEN mid.equality_columns IS NULL THEN '' ELSE ',' END + mid.inequality_columns END + ')' +
    ISNULL(' INCLUDE (' + mid.included_columns + ')', '') AS 'CreateIndexStatement'
FROM 
    sys.dm_db_missing_index_groups mig
INNER JOIN 
    sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle
INNER JOIN 
    sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
WHERE 
    migs.user_seeks > 0
ORDER BY 
    migs.avg_user_impact DESC, migs.last_user_seek DESC;
	CREATE PROCEDURE dbo.GrantAccess
    @UserName NVARCHAR(128),
    @AccessType NVARCHAR(10), -- e.g., 'SELECT', 'EXECUTE', 'CONTROL'
    @ObjectType NVARCHAR(20), -- e.g., 'TABLE', 'PROC', 'JOB'
    @ObjectName NVARCHAR(128) -- e.g., 'dbo.MyTable', 'dbo.MyProc', 'MyJob'
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX)
    
    -- Initialize the dynamic SQL
    SET @SQL = ''

    -- Construct the dynamic SQL based on object type and access type
    IF @ObjectType = 'TABLE'
    BEGIN
        IF @AccessType IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
        BEGIN
            SET @SQL = 'GRANT ' + @AccessType + ' ON ' + @ObjectName + ' TO [' + @UserName + '];'
        END
        ELSE IF @AccessType = 'CONTROL'
        BEGIN
            SET @SQL = 'GRANT CONTROL ON ' + @ObjectName + ' TO [' + @UserName + '];'
        END
    END
    ELSE IF @ObjectType = 'PROC'
    BEGIN
        IF @AccessType = 'EXECUTE'
        BEGIN
            SET @SQL = 'GRANT EXECUTE ON ' + @ObjectName + ' TO [' + @UserName + '];'
        END
        ELSE IF @AccessType = 'CONTROL'
        BEGIN
            SET @SQL = 'GRANT CONTROL ON ' + @ObjectName + ' TO [' + @UserName + '];'
        END
    END
    ELSE IF @ObjectType = 'JOB'
    BEGIN
        IF @AccessType = 'CONTROL'
        BEGIN
            SET @SQL = 'EXEC msdb.dbo.sp_add_jobserver @job_name = ''' + @ObjectName + ''', @server_name = ''(local)'';' +
                       'GRANT CONTROL ON msdb.dbo.sysjobs TO [' + @UserName + '];'
        END
    END

    -- Print the SQL for debugging purposes (optional)
    PRINT @SQL

    -- Execute the dynamic SQL
    EXEC sp_executesql @SQL
END

