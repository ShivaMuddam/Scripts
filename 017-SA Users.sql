--USE MSDB
--GO
--SELECT GETDATE() AS 'ExecutionTime'
--GO
--SELECT @@SERVERNAME AS 'SQLServerInstance'
--GO
--SELECT j.[name] AS 'JobName', 
--Enabled = CASE WHEN j.Enabled = 0 THEN 'No'
--ELSE 'Yes'
--END,
--l.[name] AS 'OwnerName'
--FROM MSDB.dbo.sysjobs j
--INNER JOIN Master.dbo.syslogins l
--ON j.owner_sid = l.sid
--ORDER BY j.[name] 
--GO


use msdb

go

DECLARE @db_job_count INT
SELECT @db_job_count = count(distinct(suser_sname(owner_sid))) from msdb..sysjobs where suser_sname(owner_sid) <> 'sa'
IF @db_job_count > '0'
    BEGIN
        DECLARE @change_job_id VARCHAR(50)
        DECLARE @change_job_name VARCHAR(100)
        DECLARE job_id_cursor CURSOR FOR
        SELECT job_id, name    FROM msdb..sysjobs WHERE suser_sname(owner_sid) <> 'sa'
        OPEN job_id_cursor
        FETCH NEXT FROM job_id_cursor
        INTO @change_job_id, @change_job_name
        WHILE @@FETCH_STATUS = 0
            BEGIN
                DECLARE @sql_statement NVARCHAR(255)
                EXEC msdb..sp_update_job @job_id = @change_job_id, @owner_login_name ='sa'
                PRINT 'Job ' + @change_job_name + ' has been updated to sa ownership'
        FETCH NEXT FROM job_id_cursor INTO @change_job_id, @change_job_name
END
CLOSE job_id_cursor
DEALLOCATE job_id_cursor
END


--SELECT SUSER_SNAME(owner_sid),name FROM sys.databases

--ALTER AUTHORIZATION ON DATABASE::tempdb TO sa

DECLARE @dbname SYSNAME
DECLARE c_loop CURSOR
FOR
    SELECT  name
    FROM    sys.databases
    WHERE   owner_sid <> 0x01
    ORDER BY name

OPEN c_loop
FETCH NEXT FROM c_loop INTO @dbname
WHILE @@fetch_status <> -1 
    BEGIN -- determines loop is continuing
        IF @@fetch_status <> -2 
            BEGIN -- determines record is still available (not dirty)
                PRINT 'use ' + @dbname
                PRINT 'g' + 'o'
                PRINT 'exec sp_changedbowner ''sa'''
                PRINT 'g' + 'o'
                PRINT ''
            END
        FETCH NEXT FROM c_loop INTO @dbname
    END
CLOSE c_loop
DEALLOCATE c_loop


--CREATE LOGIN [LSLogin] WITH PASSWORD=N'5uJAH3cCZ0YUnk7', DEFAULT_DATABASE=[master], DEFAULT_LANGUAGE=[us_english], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
--GO

--EXEC sys.sp_addsrvrolemember @loginame = N'LSLogin', @rolename = N'sysadmin'
--GO


