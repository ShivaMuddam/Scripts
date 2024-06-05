DECLARE @database_name varchar(128);
 DECLARE @file_id varchar(3);
 DECLARE @old_file_id varchar(3);
 DECLARE @file_name varchar(128);
 DECLARE @file_name2 varchar(128);
 DECLARE @M_DB varchar(128);
 
DECLARE databases CURSOR FOR
 select fileid,db_name(dbid) as databas, filename from sysaltfiles where dbid > '4' and dbid < '32767'
 OPEN databases;
 
FETCH NEXT FROM databases INTO @file_id,@database_name,@file_name
 
Print ''
 print '************** Statement for Detach ***************'
 print ''
 WHILE @@FETCH_STATUS = 0
 Begin
 declare @cmd nvarchar(512)
 if @file_id = '1'
 begin
 select @cmd = 'USE [MASTER] ' +
 'ALTER DATABASE [' + @database_name + '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE ' +
 'EXEC dbo.sp_detach_db N' + CHAR(39) + @database_name + CHAR(39) +
 ', @keepfulltextindexfile = N' + CHAR(39) + 'true' +CHAR(39)
 print(@cmd)
 end
 FETCH NEXT FROM databases INTO @file_id,@database_name,@file_name
 end
 CLOSE databases;
 OPEN databases;
 
FETCH NEXT FROM databases INTO @file_id,@database_name,@file_name
 
Print ''
 print '************** Statement for Attach ***************'
 Print ''
 WHILE @@FETCH_STATUS = 0 

Begin
set @file_name2='E:\SQLData'
 declare @cmd2 nvarchar(2048)
 select @cmd2 = 'CREATE DATABASE ' + @database_name + ' ON'
 if @file_id = '1'
 begin
 select @cmd2 = @cmd2 + '(FILENAME = N' + CHAR(39) + @file_name + CHAR(39) + ')'
 FETCH NEXT FROM databases INTO @file_id,@database_name,@file_name
 set @old_file_id = '1'
 Loop_label:
 if @file_id = '1' or @file_id = @old_file_id GOTO Done_Label
 select @cmd2 = @cmd2 + ',(FILENAME = N' + CHAR(39) + @file_name + CHAR(39) + ')'
 set @old_file_id = @file_id
 FETCH NEXT FROM databases INTO @file_id,@database_name,@file_name
 goto Loop_label
 Done_Label:
 end
 select @cmd2 = @cmd2 + ' FOR ATTACH'
 print(@cmd2)
 
end
 CLOSE databases;
 DEALLOCATE databases
