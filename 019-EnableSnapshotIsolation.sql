--DBCC USEROPTIONS

SELECT  name ,
        snapshot_isolation_state_desc ,
        is_read_committed_snapshot_on ,
        user_access_desc
FROM    sys.databases where name NOT IN ('master','msdb','model','tempdb') 
	AND name NOT IN ( SELECT name FROM master.dbo.sysdatabases WHERE name LIKE '%NexusEngine%' OR name LIKE '%Medilog' OR name LIKE '%Reporting')

ORDER BY name


SELECT  name ,
        snapshot_isolation_state_desc ,
        is_read_committed_snapshot_on ,
        user_access_desc
FROM    sys.databases where name IN ( SELECT name FROM master.dbo.sysdatabases WHERE name LIKE '%NexusEngine%' OR name LIKE '%Medilog' OR name LIKE '%Reporting')

ORDER BY name


DECLARE @SQL1 VARCHAR(MAX)
SELECT  @SQL1 = 'Kill ' + CAST(spid AS VARCHAR) + ';'
FROM    sys.sysprocesses
WHERE   dbid = DB_ID()
        AND spid <> @@SPID
        AND spid > 50
		
EXEC (@SQL1)


DECLARE DBCur CURSOR KEYSET
FOR
    SELECT  name
    FROM    master.dbo.sysdatabases
	WHERE name NOT IN ('master','msdb','model','tempdb') 
	AND name NOT IN ( SELECT name FROM master.dbo.sysdatabases WHERE name LIKE '%NexusEngine%' OR name LIKE '%Medilog' OR name LIKE '%Reporting')

DECLARE @dbname VARCHAR(255)
DECLARE @filename VARCHAR(255)
DECLARE @sql NVARCHAR(4000)

OPEN DBCur

FETCH NEXT FROM DBCur INTO @dbname
WHILE ( @@fetch_status <> -1 ) 
    BEGIN
        IF ( @@fetch_status <> -2 ) 
            BEGIN
                SET @sql = 'DECLARE FileCur CURSOR ' + 'KEYSET '
                    + 'FOR SELECT name FROM [' + @dbname + '].dbo.sysfiles '
                    + '		WHERE filename LIKE ''%.mdf'''
                EXEC sp_executesql @sql			
                OPEN FileCur
		
                FETCH NEXT FROM FileCur INTO @filename
                WHILE ( @@fetch_status <> -1 ) 
                    BEGIN
                        IF ( @@fetch_status <> -2 ) 
                            BEGIN
                                SET @sql = 'ALTER DATABASE [' + @dbname
                                    + '] SET READ_COMMITTED_SNAPSHOT ON
				ALTER DATABASE [' + @dbname
                                    + '] SET ALLOW_SNAPSHOT_ISOLATION ON'
                                PRINT @sql
				--If you want to run it plesae uncomment this
                                EXEC sp_executesql @sql
                            END
                        FETCH NEXT FROM FileCur INTO @filename
                    END
                CLOSE FileCur
                DEALLOCATE FileCur
            END
        FETCH NEXT FROM DBCur INTO @dbname
    END

CLOSE DBCur
DEALLOCATE DBCur
GO
