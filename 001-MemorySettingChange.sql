SELECT
     [physical_memory_in_bytes]/1048576 AS [PhysMemMB],
     [physical_memory_in_use_kb]/1024 AS [PhysMemInUseMB],
     [available_physical_memory_kb]/1024 AS [PhysMemAvailMB],
     [locked_page_allocations_kb] AS [LPAllocKB],
     [max_server_memory] AS [MaxSvrMem],
     [min_server_memory] AS [MinSvrMem]
 FROM
     sys.dm_os_sys_info
 CROSS JOIN
     sys.dm_os_process_memory
 CROSS JOIN
     sys.dm_os_sys_memory
 CROSS JOIN (
     SELECT
         [value_in_use] AS [max_server_memory]
     FROM
         sys.configurations
     WHERE
         [name] = 'max server memory (MB)') AS c
 CROSS JOIN (
     SELECT
         [value_in_use] AS [min_server_memory]
     FROM
         sys.configurations
     WHERE
         [name] = 'min server memory (MB)') AS c2 



--EXEC sys.sp_configure 

---- Turn on advanced options
EXEC sp_configure 'show advanced options', 1; 
GO 
RECONFIGURE;
GO
EXEC sp_configure 'Ole Automation Procedures', 1; 
GO 
RECONFIGURE;
GO 
EXEC sp_configure 'clr enabled', 1
RECONFIGURE;
GO
EXEC sp_configure 'xp_cmdshell', 1
RECONFIGURE;
GO

---- Set min server memory = 5120MB for the server
EXEC  sp_configure'min server memory (MB)',5120;
GO
RECONFIGURE;
GO


---- Set max server memory = 6144MB for the server
EXEC  sp_configure'max server memory (MB)',6144;
GO
RECONFIGURE;
GO

