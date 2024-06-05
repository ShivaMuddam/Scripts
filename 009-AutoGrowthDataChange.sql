DECLARE DBCur CURSOR
KEYSET
FOR SELECT name FROM master.dbo.sysdatabases
	WHERE name NOT IN ('master','msdb','model','tempdb')

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
					'		WHERE filename NOT LIKE ''%.ldf'''
		EXEC sp_executesql @sql			
		OPEN FileCur
		
		FETCH NEXT FROM FileCur INTO @filename
		WHILE (@@fetch_status <> -1)
		BEGIN
			IF (@@fetch_status <> -2)
			BEGIN
				SET @sql = 'ALTER DATABASE ['+@dbname+'] MODIFY FILE ( ' + 
							'NAME = N'''+@filename+''', MAXSIZE=UNLIMITED,FILEGROWTH = 262144KB )'
				PRINT @sql
				-- EXEC sp_executesql @sql
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
