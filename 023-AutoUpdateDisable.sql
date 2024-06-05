SELECT name,is_auto_create_stats_on,is_auto_update_stats_on
 FROM sys.databases
 


DECLARE DBCur CURSOR
KEYSET
FOR SELECT name FROM master.dbo.sysdatabases
	WHERE name NOT IN ('master','msdb','model','tempdb') AND name NOT IN ( SELECT name FROM master.dbo.sysdatabases WHERE name LIKE '%NexusEngine%')

DECLARE @dbname varchar(255)
DECLARE @filename varchar(255)
DECLARE @sql nvarchar(4000)

OPEN DBCur

FETCH NEXT FROM DBCur INTO @dbname
WHILE (@@fetch_status <> -1)
BEGIN
	IF (@@fetch_status <> -2)
	BEGIN
		SET @sql = 'DECLARE FileCur CURSOR ' + 
					'KEYSET ' + 
					'FOR SELECT name FROM ['+@dbname+'].dbo.sysfiles ' +
					'		WHERE filename LIKE ''%.mdf'''
		EXEC sp_executesql @sql			
		OPEN FileCur
		
		FETCH NEXT FROM FileCur INTO @filename
		WHILE (@@fetch_status <> -1)
		BEGIN
			IF (@@fetch_status <> -2)
			BEGIN
				SET @sql = 'ALTER DATABASE ['+@dbname+'] SET AUTO_UPDATE_STATISTICS OFF'
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



 