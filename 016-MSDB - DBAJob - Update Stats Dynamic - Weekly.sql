USE [msdb]
GO

/****** Object:  Job [DBAJob: Update Stats Dynamic - weekly]    Script Date: 01/05/2011 14:03:51 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[DBAJobs]]]    Script Date: 01/05/2011 14:03:51 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[DBAJobs]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[DBAJobs]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBAJob: Update Stats Dynamic - Weekly', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[DBAJobs]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Updatestats_Alldbs]    Script Date: 01/05/2011 14:03:53 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Updatestats_Alldbs', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DECLARE @SessionDT VARCHAR(50)
	, @SQL VARCHAR(MAX)

SELECT @SessionDT = CONVERT(VARCHAR, GETDATE(), 121)


SELECT @SQL = 
''
IF(''''?'''' NOT IN (''''master'''',''''model'''',''''tempdb'''',''''msdb''''))
	EXEC DBA.dbo.AP_UpdateStatsDynamic
		@DBName = ''''?'''', -- VARCHAR(255)
		@PercentChange = .1, -- DECIMAL (5,5)
		@MaxAgeInDays = 15, -- INT
		@VerboseLogging = 0, -- BIT
		@ColumnStatsOnly = 0, -- BIT
		@SessionDT = '''''' + @SessionDT + ''''''
''

EXEC SP_MSForEachDB @SQL

', 
		@database_name=N'master', 
		@output_file_name=N'C:\Program Files\Microsoft SQL Server\MSSQL.1\MSSQL\LOG\Update Stats Dynamic - weekly.txt', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Release Cache]    Script Date: 01/05/2011 14:03:54 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Release Cache', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DBCC FREESYSTEMCACHE (''All'') WITH MARK_IN_USE_FOR_REMOVAL', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Sunday night', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20090312, 
		@active_end_date=99991231, 
		@active_start_time=230000, 
		@active_end_time=235959, 
		@schedule_uid=N'cf6aea50-1234-4847-8596-1b1571f8bc18'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO


