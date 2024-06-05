USE [master]

SET nocount ON

DECLARE @name SYSNAME ,
    @name2 SYSNAME ,
    @file_id INT ,
    @sqlcmd VARCHAR(MAX) ,
    @sqlcmd2 VARCHAR(MAX)


DECLARE db_mps_simple_logs_cur CURSOR
FOR
    SELECT  d.name ,
            mf.file_id ,
            mf.name
    FROM    sys.databases d
            JOIN sys.master_files mf ON d.database_id = mf.database_id
    WHERE   d.[name] LIKE 'CCHIE%'
    --and d.recovery_model = 3 --simple only
            AND mf.type = 1 --0 is data, 1 is log

OPEN db_mps_simple_logs_cur
FETCH NEXT FROM db_mps_simple_logs_cur INTO @name, @file_id, @name2

WHILE @@fetch_status = 0 
    BEGIN

        SET @sqlcmd = 'USE ' + QUOTENAME(@name) + 'dbcc shrinkfile ( '
            + CAST(@file_id AS VARCHAR) + ', TRUNCATEONLY );' 
        SET @sqlcmd2 = 'ALTER DATABASE [' + @name
            + '] MODIFY FILE (NAME = N''' + @name2 + ''', SIZE =256MB)'
        print @sqlcmd
        print @sqlcmd2

        FETCH NEXT FROM db_mps_simple_logs_cur INTO @name, @file_id, @name2
    END

CLOSE db_mps_simple_logs_cur
DEALLOCATE db_mps_simple_logs_cur
go