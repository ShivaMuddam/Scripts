DECLARE @SQL VARCHAR(8000), @sname VARCHAR(3)
SELECT @sname=CONVERT(VARCHAR(3),SERVERPROPERTY('PRODUCTVERSION'))
SELECT @sname=CONVERT(TINYINT,SUBSTRING(@sname,1,CHARINDEX('.',@sname)-1))
IF @sname=8
BEGIN
SET @SQL='USE ?
SELECT ''?'' [Dbname]
,[name] [Filename]
,CASE STATUS & 0x40 WHEN 0x40 THEN ''LOG'' ELSE ''ROWS'' END [Type]
,filename [FilePath]
,size/128.0 AS [TotalSize_MB]
,CONVERT(INT,FILEPROPERTY(name, ''SpaceUsed''))/128.0 [Space_Used_MB]
,CASE STATUS & 0x100000 WHEN 0x100000 THEN convert(NVARCHAR(3), growth) + ''%''
ELSE CONVERT(NVARCHAR(15), (growth * 8)/1024) + '' MB'' END [Autogrow_Value]
,CASE maxsize WHEN -1 THEN CASE growth WHEN 0 THEN ''Restricted'' ELSE N''Unlimited'' END
ELSE CONVERT(NVARCHAR(15), (maxsize * 8)/1024) + '' MB'' END [Max_Size]
FROM ?.dbo.sysfiles'
END
ELSE
BEGIN
SET @SQL=' USE ?
SELECT ''?'' [Dbname]
,[name] [Filename]
,type_desc [Type]
,physical_name [FilePath]
,CONVERT(INT,[size]/128.0) [TotalSize_MB]
,CONVERT(INT,FILEPROPERTY(name, ''SpaceUsed''))/128.0 AS [Space_Used_MB]
,CASE is_percent_growth
WHEN 1 THEN CONVERT(VARCHAR(5),growth)+''%''
ELSE CONVERT(VARCHAR(20),(growth/128))+'' MB''
END [Autogrow_Value]
,CASE max_size
WHEN -1 THEN CASE growth
WHEN 0 THEN CONVERT(VARCHAR(30),''Restricted'')
ELSE CONVERT(VARCHAR(30),''Unlimited'') END
ELSE CONVERT(VARCHAR(25),max_size/128)
END [Max_Size]
FROM ?.sys.database_files'
END
IF EXISTS(SELECT 1 FROM tempdb..sysobjects WHERE name='##Fdetails')
DROP TABLE ##Fdetails
CREATE TABLE  ##Fdetails (Dbname VARCHAR(50),Filename VARCHAR(50),Type VARCHAR(10),Filepath VARCHAR(2000)
,TotalSize_MB INT,Space_Used_MB INT,Autogrow_Value VARCHAR(15),Max_Size VARCHAR(30))
INSERT INTO ##Fdetails
EXEC sp_msforeachdb @SQL
SELECT * FROM ##Fdetails WHERE Dbname NOT IN ('master','msdb','model','tempdb')  ORDER BY Dbname