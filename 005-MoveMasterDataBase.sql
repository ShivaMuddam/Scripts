--Make sure you change Database Settins Folder.  

SELECT name, physical_name AS CurrentLocation
FROM sys.master_files
WHERE database_id < 5
GO

--Move Model Database
USE master;
GO
ALTER DATABASE model
MODIFY FILE (NAME = modeldev, FILENAME = 'E:\SQLDATA\model.mdf');
GO

ALTER DATABASE model
MODIFY FILE (NAME = Modellog, FILENAME = 'F:\SQLDATA\modelLog.ldf');
GO


--Move MSDB Database
USE master;
GO
ALTER DATABASE msdb
MODIFY FILE (NAME = MSDBData, FILENAME = 'E:\SQLDATA\MSDBData.mdf');
GO

ALTER DATABASE msdb
MODIFY FILE (NAME = MSDBLog, FILENAME = 'F:\SQLDATA\MSDBLog.ldf');
GO

--Move Tempdb Database
USE master;
GO
ALTER DATABASE tempdb
MODIFY FILE (NAME = tempdev, FILENAME = 'G:\SQLData\tempdev.mdf');
GO

ALTER DATABASE tempdb
MODIFY FILE (NAME = templog, FILENAME = 'G:\SQLData\tempLog.ldf');
GO

--Stop the instance, physically move the model,msdb and tempdb files to the new location

--To move the master database:
 
--In SQL Server configuration manager, edit the advanced properties for the SQL Server Service.
 
--Change the startup parameters to the new location of the files, –l flag refers to log destination and –d flag refers to data file destination:

--		-dE:\SQLData\master.mdf;-eC:\Program Files\Microsoft SQL Server\MSSQL10_50.MSSQLSERVER\MSSQL\Log\ERRORLOG;-lF:\SQLData\mastlog.ldf 

--Physically move the files to the new location
 
--Start the instance.

