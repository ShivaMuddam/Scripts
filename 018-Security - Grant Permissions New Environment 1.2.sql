SET NOCOUNT ON
DECLARE 
	@Filter VARCHAR(100), 
	@GrantTo VARCHAR(100),
	@DBID int, 
	@DBName VARCHAR(500),
	@SQL NVARCHAR(4000)

SELECT @Filter = '%DHIN%', @GrantTo = 'PROD\CDST Implementation Engineers'   -- don't square bracket the user

SELECT @DBID = MIN(DBID) FROM master..sysdatabases WHERE name like @Filter -- skip system DBs
WHILE @DBID IS NOT NULL
BEGIN
	SELECT @DBName = name FROM master..sysdatabases WHERE dbid = @DBID

	SELECT @SQL = 'USE ['+@DBName+']
	SELECT ''USE ['+@DBName+']''
	SELECT ''CREATE USER ['+@GrantTo+']
	GO''
	SELECT ''EXEC sp_addrolemember ''''db_datareader'''', '''''+@GrantTo+'''''
	GO''
	SELECT ''EXEC sp_addrolemember ''''db_datawriter'''', '''''+@GrantTo+'''''
	GO''
	SELECT ''GRANT EXECUTE TO ['+@GrantTo+']''
'
	EXEC sp_executesql @SQL
	
	SELECT @DBID = MIN(dbid) 
	FROM master..sysdatabases 
	WHERE dbid>@DBID  -- next
	AND name like @Filter
	
END


