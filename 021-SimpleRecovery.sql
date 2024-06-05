SELECT  name ,
        recovery_model ,
        recovery_model_desc
FROM    master.sys.databases
ORDER BY 1



USE master  

SELECT  'ALTER DATABASE [' + name + '] SET RECOVERY SIMPLE WITH NO_WAIT'
FROM    master..sysdatabases
WHERE   name NOT IN ( 'master', 'model', 'msdb', 'tempdb' )  

SELECT  'ALTER DATABASE [' + name + '] SET RECOVERY FULL WITH NO_WAIT'
FROM    master..sysdatabases
WHERE   name NOT IN ( 'master', 'model', 'msdb', 'tempdb' )  