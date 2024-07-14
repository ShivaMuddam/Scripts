USE [LMS_REPORTING_RESTORE]
GO
/****** Object:  StoredProcedure [LS_ODS].[ODS_Process_New_v1]    Script Date: 7/13/2024 12:37:16 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*** ============================================= 
-- Author:		Mimi Pierce-Byrd
-- Create date: 03/14/2023 
-- Description:	ODS Modification Script
-- 
-- Change Control 
-- 1.0		MPB			03/14/2023		Initial Code Deployed 
-- 1.1      MPB			03/15/2023		Removed Blackboard code
-- 1.2      MPB			03/20/2023      Added the Audit.GradeExtractImport_d2l table to save the data in case we need to reprocess grade records.
-- 1.3      PP          05/01/2023      Added audit.grades table and error handling for merge process also catch block is added 
-- Uncommented VA Report 		
		
		EXEC dbo.ProcessVABenefitReportOct2015Policy; 
 
		EXEC LS_ODS.AddODSLoadLog 'VA Report Data Processing Complete', 0;
		
		
		edited by: Rogan Richeart 
		edited on: 01/25/2024
		edit reason: Ticket VA Attendance Report issue SR#3322624
--1.4		CML			02/28/2024		Updated course code logic to capture EMT course data as the courses follow a different naming convention: AAA1111[xx] or AAA-1111[xx]
--										Updated WeekNumber code as well to account for EMT courses running 16 weeks.  Code no longer truncates double digit values.
--										NOTE: EMT week 5 - Week 16 assignments are added into Week 5 at this time; impact analysis and further review required to accurately capture assignments by week.
--											  EMT only does weekly grades in week 6, this is not currently captured, the Final Percentage is captured in week 5 in step: Update the stage.Students table with the weekly grades 
--   M.Mullane 7/5/2024

	Temporary Fix for an issue on 7/5/2024:
	added DISTINCT tO LINE 5384 for duplicate data issue

**/
ALTER PROCEDURE [LS_ODS].[ODS_Process_New_v1]
AS
BEGIN
    SET NOCOUNT ON;

    --**************************************************************************************************************************************** 
    --Declare any global variables 
    --**************************************************************************************************************************************** 
    DECLARE @CurrentDateTime DATETIME;
    SET @CurrentDatetime = GETDATE();


    ----**************************************************************************************************************************************** 
    ----Instiantiate any global variables 
    ----**************************************************************************************************************************************** 
    EXEC LS_ODS.AddODSLoadLog 'ODS Load Process Started', 0;
    BEGIN TRY
        --**************************************************************************************************************************************** 
        --Load Grade Extact Import related data from D2L
        --**************************************************************************************************************************************** 
        DECLARE @CountD2LGEI as int

        SELECT @CountD2LGEI = COUNT(*)
        FROM [stage].[GradeExtractImport_d2l]

        IF @CountD2LGEI > 0
        BEGIN
            BEGIN
                EXEC dbo.DropSpecificIndex @SchemaName = '[stage]',
                                           @TableName = '[GradeExtractImport]',
                                           @IndexName = 'idx_GEI_0001';
                EXEC dbo.DropSpecificIndex @SchemaName = '[stage]',
                                           @TableName = '[GradeExtractImport]',
                                           @IndexName = 'idx_GEI_0002';
                EXEC dbo.DropSpecificIndex @SchemaName = '[stage]',
                                           @TableName = '[GradeExtractImport]',
                                           @IndexName = 'idx_ODS_019';
            END;
            --**************************************************************************************************************************************** 
            --Dupes deletion from the gradeextract table 
            --**************************************************************************************************************************************** 
            -- Step 1: Insert duplicates into error table using a CTE
            WITH CTE_SGEI
            AS (SELECT PK1,
                       UserPK1,
                       UserEPK,
                       UserLastName,
                       UserFirstName,
                       UserUserId,
                       CoursePK1,
                       CourseEPK,
                       CourseCourseId,
                       CourseTitle,
                       MembershipPK1,
                       AssignmentPK1,
                       AssignmentIsExternalGradeIndicator,
                       AssignmentDisplayColumnName,
                       AssignmentPointsPossible,
                       AssignmentDisplayTitle,
                       GradePK1,
                       GradeAttemptDate,
                       GradeAttemptStatus,
                       GradeManualGrade,
                       GradeManualScore,
                       GradeDisplayGrade,
                       GradeDisplayScore,
                       GradeExemptIndicator,
                       GradeOverrideDate,
                       SourceSystem,
                       ROW_NUMBER() OVER (PARTITION BY UserPK1,
                                                       CoursePK1,
                                                       AssignmentPK1,
                                                       MembershipPK1,
                                                       GradePK1
                                          ORDER BY
                                              (
                                                  SELECT NULL
                                              )
                                         ) AS rn
                FROM stage.GradeExtractImport_d2l
               )
            INSERT INTO Stage.ODS_Duplicates
            (
                PrimaryKey,
                STEP_FAILED_ON,
                PROCCESED_ON
            )
            SELECT PK1 AS PrimaryKey,
                   'Grade_Merge' AS STEP_FAILED_ON,
                   CONVERT(DATE, GETDATE()) AS PROCCESED_ON
            FROM CTE_SGEI
            WHERE rn > 1;

            -- Step 2: Delete duplicates from the source table using a new CTE
            WITH CTE_DelDupes
            AS (SELECT PK1
                FROM
                (
                    SELECT PK1,
                           ROW_NUMBER() OVER (PARTITION BY UserPK1,
                                                           CoursePK1,
                                                           AssignmentPK1,
                                                           MembershipPK1,
                                                           GradePK1
                                              ORDER BY
                                                  (
                                                      SELECT NULL
                                                  )
                                             ) AS rn
                    FROM stage.GradeExtractImport_d2l
                ) AS subquery
                WHERE rn > 1
               )
            DELETE FROM stage.GradeExtractImport_d2l
            WHERE PK1 IN (
                             SELECT PK1 FROM CTE_DelDupes
                         );

            -- Step 3: Merge deduplicated data into the destination table
            WITH GEICTE
            AS (SELECT UserPK1,
                       UserEPK,
                       UserLastName,
                       UserFirstName,
                       UserUserId,
                       CoursePK1,
                       CourseEPK,
                       CourseCourseId,
                       CourseTitle,
                       MembershipPK1,
                       AssignmentPK1,
                       AssignmentIsExternalGradeIndicator,
                       AssignmentDisplayColumnName,
                       AssignmentPointsPossible,
                       AssignmentDisplayTitle,
                       GradePK1,
                       GradeAttemptDate,
                       GradeAttemptStatus,
                       GradeManualGrade,
                       GradeManualScore,
                       GradeDisplayGrade,
                       GradeDisplayScore,
                       GradeExemptIndicator,
                       GradeOverrideDate,
                       SourceSystem
                FROM stage.GradeExtractImport_d2l
                WHERE SourceSystem = 'D2L'
               )
            MERGE INTO GEICTE AS target
            USING
            (
                SELECT DISTINCT
                    UserPK1,
                    UserEPK,
                    UserLastName,
                    UserFirstName,
                    UserUserId,
                    CoursePK1,
                    CourseEPK,
                    CourseCourseId,
                    CourseTitle,
                    MembershipPK1,
                    AssignmentPK1,
                    AssignmentIsExternalGradeIndicator,
                    AssignmentDisplayColumnName,
                    AssignmentPointsPossible,
                    AssignmentDisplayTitle,
                    GradePK1,
                    GradeAttemptDate,
                    GradeAttemptStatus,
                    GradeManualGrade,
                    GradeManualScore,
                    GradeDisplayGrade,
                    GradeDisplayScore,
                    GradeExemptIndicator,
                    GradeOverrideDate,
                    SourceSystem
                FROM stage.GradeExtractImport_d2l
            ) AS source
            ON ISNULL(source.UserPK1, '') = ISNULL(target.UserPK1, '')
               AND ISNULL(source.CoursePK1, '') = ISNULL(target.CoursePK1, '')
               AND ISNULL(source.AssignmentPK1, '') = ISNULL(target.AssignmentPK1, '')
               AND ISNULL(source.MembershipPK1, '') = ISNULL(target.MembershipPK1, '')
               AND ISNULL(source.GradePK1, '') = ISNULL(target.GradePK1, '')
            WHEN MATCHED AND NOT EXISTS
        (
            SELECT source.UserEPK,
                   source.UserLastName,
                   source.UserFirstName,
                   source.UserUserId,
                   source.CourseEPK,
                   source.CourseCourseId,
                   source.CourseTitle,
                   source.AssignmentIsExternalGradeIndicator,
                   source.AssignmentDisplayColumnName,
                   source.AssignmentPointsPossible,
                   source.AssignmentDisplayTitle,
                   source.GradeAttemptDate,
                   source.GradeAttemptStatus,
                   source.GradeManualGrade,
                   source.GradeManualScore,
                   source.GradeDisplayGrade,
                   source.GradeDisplayScore,
                   source.GradeExemptIndicator,
                   source.GradeOverrideDate,
                   source.SourceSystem
            INTERSECT
            SELECT target.UserEPK,
                   target.UserLastName,
                   target.UserFirstName,
                   target.UserUserId,
                   target.CourseEPK,
                   target.CourseCourseId,
                   target.CourseTitle,
                   target.AssignmentIsExternalGradeIndicator,
                   target.AssignmentDisplayColumnName,
                   target.AssignmentPointsPossible,
                   target.AssignmentDisplayTitle,
                   target.GradeAttemptDate,
                   target.GradeAttemptStatus,
                   target.GradeManualGrade,
                   target.GradeManualScore,
                   target.GradeDisplayGrade,
                   target.GradeDisplayScore,
                   target.GradeExemptIndicator,
                   target.GradeOverrideDate,
                   target.SourceSystem
        )   THEN
                UPDATE SET UserEPK = source.UserEPK,
                           UserLastName = source.UserLastName,
                           UserFirstName = source.UserFirstName,
                           UserUserId = source.UserUserId,
                           CourseEPK = source.CourseEPK,
                           CourseCourseId = source.CourseCourseId,
                           CourseTitle = source.CourseTitle,
                           AssignmentIsExternalGradeIndicator = source.AssignmentIsExternalGradeIndicator,
                           AssignmentDisplayColumnName = source.AssignmentDisplayColumnName,
                           AssignmentPointsPossible = source.AssignmentPointsPossible,
                           AssignmentDisplayTitle = source.AssignmentDisplayTitle,
                           GradeAttemptDate = source.GradeAttemptDate,
                           GradeAttemptStatus = source.GradeAttemptStatus,
                           GradeManualGrade = source.GradeManualGrade,
                           GradeManualScore = source.GradeManualScore,
                           GradeDisplayGrade = source.GradeDisplayGrade,
                           GradeDisplayScore = source.GradeDisplayScore,
                           GradeExemptIndicator = source.GradeExemptIndicator,
                           GradeOverrideDate = source.GradeOverrideDate,
                           SourceSystem = source.SourceSystem
            WHEN NOT MATCHED THEN
                INSERT
                (
                    UserPK1,
                    UserEPK,
                    UserLastName,
                    UserFirstName,
                    UserUserId,
                    CoursePK1,
                    CourseEPK,
                    CourseCourseId,
                    CourseTitle,
                    MembershipPK1,
                    AssignmentPK1,
                    AssignmentIsExternalGradeIndicator,
                    AssignmentDisplayColumnName,
                    AssignmentPointsPossible,
                    AssignmentDisplayTitle,
                    GradePK1,
                    GradeAttemptDate,
                    GradeAttemptStatus,
                    GradeManualGrade,
                    GradeManualScore,
                    GradeDisplayGrade,
                    GradeDisplayScore,
                    GradeExemptIndicator,
                    GradeOverrideDate,
                    SourceSystem
                )
                VALUES
                (source.UserPK1,
                 source.UserEPK,
                 source.UserLastName,
                 source.UserFirstName,
                 source.UserUserId,
                 source.CoursePK1,
                 source.CourseEPK,
                 source.CourseCourseId,
                 source.CourseTitle,
                 source.MembershipPK1,
                 source.AssignmentPK1,
                 source.AssignmentIsExternalGradeIndicator,
                 source.AssignmentDisplayColumnName,
                 source.AssignmentPointsPossible,
                 source.AssignmentDisplayTitle,
                 source.GradePK1,
                 source.GradeAttemptDate,
                 source.GradeAttemptStatus,
                 source.GradeManualGrade,
                 source.GradeManualScore,
                 source.GradeDisplayGrade,
                 source.GradeDisplayScore,
                 source.GradeExemptIndicator,
                 source.GradeOverrideDate,
                 source.SourceSystem
                )
            WHEN NOT MATCHED BY SOURCE THEN
                DELETE;

            EXEC dbo.CreateIncludeIndex @SchemaName = 'Stage',
                                        @TableName = 'GradeExtractImport',
                                        @IndexName = 'idx_GEI_0001',
                                        @IndexType = 'NONCLUSTERED',
                                        @Column = 'GradeDisplayGrade',
                                        @IncludeColumns = 'GradeDisplayScore';
            EXEC dbo.CreateIncludeIndex @SchemaName = 'Stage',
                                        @TableName = 'GradeExtractImport',
                                        @IndexName = 'idx_GEI_0002',
                                        @IndexType = 'NONCLUSTERED',
                                        @Column = 'AssignmentDisplayColumnName',
                                        @IncludeColumns = 'CourseTitle, UserEPK';
            EXEC dbo.CreateIncludeIndex @SchemaName = 'Stage',
                                        @TableName = 'GradeExtractImport',
                                        @IndexName = 'idx_ODS_019',
                                        @IndexType = 'NONCLUSTERED',
                                        @Column = 'AssignmentDisplayColumnName',
                                        @IncludeColumns = '[UserPK1], [UserEPK], [CourseTitle]';


            /**	--**************************************************************************************************************************************** 
		--Save the data from the GradeExtractImport_d2l to an audit table in case we have to rerun the ods process

		TRUNCATE TABLE [Audit].[GradeExtractImport_d2l]
		WAITFOR DELAY '00:00:10';		
		INSERT INTO [Audit].[GradeExtractImport_d2l]([PK1], [UserPK1], [UserEPK], [UserLastName], [UserFirstName], [UserUserId], [CoursePK1], [CourseEPK], [CourseCourseId], [CourseTitle],
		[MembershipPK1], [AssignmentPK1], [AssignmentIsExternalGradeIndicator], [AssignmentDisplayColumnName], [AssignmentPointsPossible], [AssignmentDisplayTitle], [GradePK1], [GradeAttemptDate],
		[GradeAttemptStatus], [GradeManualGrade], [GradeManualScore], [GradeDisplayGrade], [GradeDisplayScore], [GradeExemptIndicator], [GradeOverrideDate], [SourceSystem])

		SELECT [PK1], [UserPK1], [UserEPK], [UserLastName], [UserFirstName], [UserUserId], [CoursePK1], [CourseEPK], [CourseCourseId], [CourseTitle], [MembershipPK1], [AssignmentPK1],
		[AssignmentIsExternalGradeIndicator], [AssignmentDisplayColumnName], [AssignmentPointsPossible], [AssignmentDisplayTitle], [GradePK1], [GradeAttemptDate], [GradeAttemptStatus],
		[GradeManualGrade], [GradeManualScore], [GradeDisplayGrade], [GradeDisplayScore], [GradeExemptIndicator], [GradeOverrideDate], [SourceSystem] FROM [Stage].[GradeExtractImport_d2l]

		--**************************************************************************************************************************************** */

            TRUNCATE TABLE [stage].[GradeExtractImport_d2l];
            EXEC LS_ODS.AddODSLoadLog 'Finished Loading GradeExtract Data from D2L',
                                      0;

        END
        ELSE
        BEGIN
            EXEC LS_ODS.AddODSLoadLog 'No GradeExtractImport D2L Data Is Available For Today',
                                      0;
            THROW 51000, 'No GradeExtractImport D2L Data Is Available For Today', 1;
        END


        --**************************************************************************************************************************************** 
        --Translate new student-facing reporting values to internal-facing values
        --**************************************************************************************************************************************** 
        DECLARE @BatchSize INT = 1000;
		DECLARE @RowsAffected INT;

-- Process the first update query in batches with optimization hints
		 SET @RowsAffected = 1;
		WHILE @RowsAffected > 0
		BEGIN
			UPDATE TOP (@BatchSize) stage.GradeExtractImport WITH (ROWLOCK)
			SET AssignmentDisplayColumnName = REPLACE(AssignmentDisplayColumnName, 'Assessment', 'Test'),
				AssignmentDisplayTitle = REPLACE(AssignmentDisplayTitle, 'Assessment', 'Test')
			WHERE AssignmentDisplayColumnName LIKE '%Assessment%'
				  OR AssignmentDisplayTitle LIKE '%Assessment%'
			OPTION (OPTIMIZE FOR UNKNOWN, MAXDOP 1);

			SET @RowsAffected = @@ROWCOUNT;
		END

-- Process the second update query in batches with optimization hints
		SET @RowsAffected = 1;
		WHILE @RowsAffected > 0
		BEGIN
			UPDATE TOP (@BatchSize) stage.GradeExtractImport WITH (ROWLOCK)
			SET AssignmentDisplayColumnName = REPLACE(AssignmentDisplayColumnName, 'Interactive', 'Module'),
				AssignmentDisplayTitle = REPLACE(AssignmentDisplayTitle, 'Interactive', 'Module')
			WHERE AssignmentDisplayColumnName LIKE '%Interactive%'
				  OR AssignmentDisplayTitle LIKE '%Interactive%'
			OPTION (OPTIMIZE FOR UNKNOWN, MAXDOP 1);

			SET @RowsAffected = @@ROWCOUNT;
		END

-- Process the third update query in batches with optimization hints
			SET @RowsAffected = 1;
			WHILE @RowsAffected > 0
			BEGIN
				UPDATE TOP (@BatchSize) stage.GradeExtractImport WITH (ROWLOCK)
				SET GradeDisplayGrade = CAST(GradeDisplayScore AS VARCHAR(50)) + '0'
				WHERE GradeDisplayGrade = 'Complete'
				OPTION (OPTIMIZE FOR UNKNOWN, MAXDOP 1);

				SET @RowsAffected = @@ROWCOUNT;
			END

		
		
		
		UPDATE stage.GradeExtractImport
        SET AssignmentDisplayColumnName = REPLACE(AssignmentDisplayColumnName, 'Assessment', 'Test'),
            AssignmentDisplayTitle = REPLACE(AssignmentDisplayTitle, 'Assessment', 'Test')
        WHERE AssignmentDisplayColumnName LIKE '%Assessment%'
              OR AssignmentDisplayTitle LIKE '%Assessment%';

        UPDATE stage.GradeExtractImport
        SET AssignmentDisplayColumnName = REPLACE(AssignmentDisplayColumnName, 'Interactive', 'Module'),
            AssignmentDisplayTitle = REPLACE(AssignmentDisplayTitle, 'Interactive', 'Module')
        WHERE AssignmentDisplayColumnName LIKE '%Interactive%'
              OR AssignmentDisplayTitle LIKE '%Interactive%';

        --**************************************************************************************************************************************** 
        --Clean up bad value in Gen 3 Courses 
        --**************************************************************************************************************************************** 
        UPDATE stage.GradeExtractImport
        SET GradeDisplayGrade = CAST(GradeDisplayScore AS VARCHAR(50)) + '0'
        WHERE GradeDisplayGrade = 'Complete';

        EXEC LS_ODS.AddODSLoadLog 'Cleaned Up Bad Gen 3 Course Values', 0;

        --**************************************************************************************************************************************** 
        --Clean up bad max points values found in the GradeExtract file 
        --**************************************************************************************************************************************** 
        DECLARE @Assignments TABLE
        (
            AssignmentPK1 INT,
            PointsPossible DECIMAL(18, 2),
            NumberOfAssignments INT
        );

        --Get list of all assignments, possible points and number of records that are the same 
        INSERT INTO @Assignments
        (
            AssignmentPK1,
            PointsPossible,
            NumberOfAssignments
        )
        SELECT gei.AssignmentPK1,
               REPLACE(gei.AssignmentPointsPossible, '"', '') 'PossiblePoints',
               COUNT(1) 'NumberOfAssignments'
        FROM stage.GradeExtractImport gei
        GROUP BY gei.AssignmentPK1,
                 REPLACE(gei.AssignmentPointsPossible, '"', '');

        --Review list of assignments 
        --SELECT * FROM @Assignments; 

        DECLARE @Adjustments TABLE
        (
            AssignmentPK1 INT,
            PointsPossible DECIMAL(18, 2)
        );

        --Compare the assignments to determine which have more than one value for points possible and store them 
        WITH cteMajorities (AssignmentPK1, MajorityCount)
        AS (SELECT a.AssignmentPK1,
                   MAX(a.NumberOfAssignments) 'MajorityCount'
            FROM @Assignments a
            GROUP BY a.AssignmentPK1
            HAVING COUNT(a.AssignmentPK1) > 1
           )
        INSERT INTO @Adjustments
        (
            AssignmentPK1,
            PointsPossible
        )
        SELECT a.AssignmentPK1,
               a.PointsPossible
        FROM @Assignments a
            INNER JOIN cteMajorities m
                ON a.AssignmentPK1 = m.AssignmentPK1
                   AND a.NumberOfAssignments = m.MajorityCount;

        --Review the list of assignments that need cleanup 
        --SELECT * FROM @Adjustments; 

        --Update the GradeExtractImport table to remove/overwrite all the assignments with "wrong" values for points possible 
        UPDATE gei
        SET gei.AssignmentPointsPossible = a.PointsPossible
        FROM stage.GradeExtractImport gei
            INNER JOIN @Adjustments a
                ON gei.AssignmentPK1 = a.AssignmentPK1;

        EXEC LS_ODS.AddODSLoadLog 'Cleaned Up Bad Assignment Max Points Values',
                                  0;

        --**************************************************************************************************************************************** 
        --Clean up missing Assignment Status values found in the GradeExtract file 
        --**************************************************************************************************************************************** 
        -- Step 0: Drop the temporary table if it exists
		IF OBJECT_ID('tempdb..#AssignmentStatuses') IS NOT NULL
			DROP TABLE #AssignmentStatuses;

		-- Step 1: Create a temporary table to store the result of the CTE
		CREATE TABLE #AssignmentStatuses
		(
			UserEPK INT,
			CourseEPK INT,
			GradePK1 INT,
			FirstAttemptStatus INT
		);

		-- Step 2: Insert data into the temporary table
		INSERT INTO #AssignmentStatuses (UserEPK, CourseEPK, GradePK1, FirstAttemptStatus)
		SELECT u.BATCH_UID AS UserEPK,
			   cm.BATCH_UID AS CourseEPK,
			   gg.PK1 AS GradePk1,
			   a.[STATUS] AS FirstAttemptStatus
		FROM dbo.GRADEBOOK_GRADE gg
			INNER JOIN dbo.COURSE_USERS cu ON gg.COURSE_USERS_PK1 = cu.PK1
			INNER JOIN dbo.USERS u ON cu.USERS_PK1 = u.PK1
			INNER JOIN dbo.COURSE_MAIN cm ON cu.CRSMAIN_PK1 = cm.PK1
			INNER JOIN dbo.GRADEBOOK_MAIN gm ON gg.GRADEBOOK_MAIN_PK1 = gm.PK1
			LEFT JOIN dbo.ATTEMPT a ON gg.FIRST_ATTEMPT_PK1 = a.PK1
		WHERE gg.HIGHEST_ATTEMPT_PK1 IS NULL
			  AND a.[STATUS] IS NOT NULL;

		-- Step 3: Create indexes on the temporary table to improve join performance
		CREATE INDEX IX_AssignmentStatuses_UserCourseGrade
		ON #AssignmentStatuses (UserEPK, CourseEPK, GradePK1);

		-- Step 4: Perform the update using the temporary table with ROWLOCK hint
		UPDATE gei
		SET gei.GradeAttemptStatus = cas.FirstAttemptStatus
		FROM stage.GradeExtractImport gei
			INNER JOIN #AssignmentStatuses cas
				ON gei.UserEPK = cas.UserEPK
				   AND gei.CourseEPK = cas.CourseEPK
				   AND gei.GradePK1 = cas.GradePK1
		WHERE gei.GradeAttemptDate IS NOT NULL
			  AND gei.GradeAttemptStatus IS NULL
		OPTION (OPTIMIZE FOR UNKNOWN, MAXDOP 1);

		-- Step 5: Update remaining records in the GradeExtractImport table with ROWLOCK hint
		UPDATE stage.GradeExtractImport
		SET GradeAttemptStatus = 6
		WHERE GradeAttemptDate IS NOT NULL
			  AND GradeAttemptStatus IS NULL
		OPTION (OPTIMIZE FOR UNKNOWN, MAXDOP 1);

		-- Step 6: Drop the temporary table after use
		DROP TABLE #AssignmentStatuses;

        EXEC LS_ODS.AddODSLoadLog 'Cleaned Up Missing Assignment Status Values',
                                  0;

        --Fix bad display score values
        -- Ensure the necessary indexes exist
		CREATE INDEX IX_GradeDisplayScore ON stage.GradeExtractImport (GradeDisplayScore);
		CREATE INDEX IX_GradeManualScore ON stage.GradeExtractImport (GradeManualScore);

		-- Define batch size
		DECLARE @BatchSize INT = 1000;

		-- Variables to keep track of rows affected and processed
		DECLARE @RowsAffected INT;
		DECLARE @TotalRows INT = 0;

-- Batch update for GradeDisplayScore
		WHILE 1 = 1
		BEGIN
			-- Update in batches
			UPDATE TOP (@BatchSize) stage.GradeExtractImport WITH (ROWLOCK)
			SET GradeDisplayGrade = NULL,
				GradeDisplayScore = NULL
			WHERE GradeDisplayScore LIKE '%E%';

			-- Check the number of rows affected
			SET @RowsAffected = @@ROWCOUNT;
			SET @TotalRows = @TotalRows + @RowsAffected;

			-- Exit loop if no more rows are affected
			IF @RowsAffected = 0
				BREAK;
		END;

		-- Reset variables for the next update
		SET @TotalRows = 0;

		-- Batch update for GradeManualScore
		WHILE 1 = 1
		BEGIN
			-- Update in batches
			UPDATE TOP (@BatchSize) stage.GradeExtractImport WITH (ROWLOCK)
			SET GradeManualGrade = NULL,
				GradeManualScore = NULL
			WHERE GradeManualScore LIKE '%E%';

			-- Check the number of rows affected
			SET @RowsAffected = @@ROWCOUNT;
			SET @TotalRows = @TotalRows + @RowsAffected;

			-- Exit loop if no more rows are affected
			IF @RowsAffected = 0
				BREAK;
		END;

-- Optionally, drop the indexes if they were created solely for this update
-- DROP INDEX stage.GradeExtractImport.IX_GradeDisplayScore;
-- DROP INDEX stage.GradeExtractImport.IX_GradeManualScore;


        --Replace all double quotes to commas as expected (bug in GradebookExtract translates all commas to double quotes)
        UPDATE stage.GradeExtractImport
        SET CourseTitle = REPLACE(CourseTitle, '"', ',');

        --**************************************************************************************************************************************** 
        --Truncate the stage tables needed for processing the data 
        --**************************************************************************************************************************************** 
        TRUNCATE TABLE stage.Students;
        TRUNCATE TABLE stage.Courses;
        TRUNCATE TABLE stage.Assignments;
        TRUNCATE TABLE stage.Grades;

        EXEC LS_ODS.AddODSLoadLog 'Truncated Working Tables', 0;

        --**************************************************************************************************************************************** 
        --Fill the stage.Students table with all the values from the raw import table 
        --**************************************************************************************************************************************** 
-- Drop temporary tables if they exist
	IF OBJECT_ID('tempdb..#VAStudents') IS NOT NULL
		DROP TABLE #VAStudents;

	IF OBJECT_ID('tempdb..#Notices') IS NOT NULL
		DROP TABLE #Notices;

	IF OBJECT_ID('tempdb..#Retakes') IS NOT NULL
		DROP TABLE #Retakes;

	-- Create temporary tables and insert data into them

	-- Create a temporary table for cteVAStudents
	CREATE TABLE #VAStudents (
		SyStudentId INT,
		BenefitName VARCHAR(255)
	);

	-- Insert data into #VAStudents
	INSERT INTO #VAStudents (SyStudentId, BenefitName)
	SELECT s.SyStudentId,
		   uv.FieldValue
	FROM CV_Prod.dbo.syStudent s
		INNER JOIN CV_Prod.dbo.SyUserValues uv
			ON s.SyStudentId = uv.syStudentID
			   AND uv.syUserDictID = 51
			   AND LEFT(uv.FieldValue, 2) = 'VA';

	-- Create a temporary table for cteNotices
	CREATE TABLE #Notices (
		SyStudentId INT,
		NoticeName VARCHAR(255),
		NoticeDueDate DATETIME
	);

	-- Insert data into #Notices
	INSERT INTO #Notices (SyStudentId, NoticeName, NoticeDueDate)
	SELECT e.SyStudentId,
		   t.Descrip,
		   MAX(e.DueDate)
	FROM CV_Prod.dbo.CmEvent e
		INNER JOIN CV_Prod.dbo.CmTemplate t
			ON e.CmTemplateID = t.CmTemplateID
	WHERE e.CmTemplateID IN (1404, 1405)
		  AND e.CmEventStatusID = 1
	GROUP BY e.SyStudentId, t.Descrip;

	-- Create a temporary table for cteRetakes
	CREATE TABLE #Retakes (
		SyStudentId INT,
		AdEnrollId INT,
		AdCourseId INT,
		Tries INT
	);

	-- Insert data into #Retakes
	INSERT INTO #Retakes (SyStudentId, AdEnrollId, AdCourseId, Tries)
	SELECT A.SyStudentID,
		   A.AdEnrollID,
		   A.AdCourseID,
		   COUNT(A.AdCourseID)
	FROM CV_Prod.dbo.AdEnrollSched A
		INNER JOIN CV_Prod.dbo.AdEnroll B
			ON A.AdEnrollID = B.AdEnrollID
		INNER JOIN CV_Prod.dbo.SySchoolStatus
			ON B.SySchoolStatusID = SySchoolStatus.SySchoolStatusID
		INNER JOIN CV_Prod.dbo.syStatus
			ON SySchoolStatus.SyStatusID = syStatus.SyStatusID
	WHERE B.SyCampusID = 9
		  AND A.[Status] IN ('P', 'C') -- Posted, Current
		  AND (syStatus.Category IN ('A', 'T') OR syStatus.Category IN ('E'))
		  AND (A.RetakeFlag IS NOT NULL AND A.AdGradeLetterCode IN ('F', ''))
	GROUP BY A.SyStudentID, A.AdEnrollID, A.AdCourseID
	HAVING COUNT(A.AdCourseID) > 1;

	-- Create indexes on temporary tables
	CREATE INDEX IX_VAStudents_SyStudentId ON #VAStudents (SyStudentId);
	CREATE INDEX IX_Notices_SyStudentId ON #Notices (SyStudentId);
	CREATE INDEX IX_Retakes_SyStudentId_AdEnrollId_AdCourseId ON #Retakes (SyStudentId, AdEnrollId, AdCourseId);

	-- Insert data into the final table

	INSERT INTO stage.Students (
		StudentPrimaryKey,
		DateTimeCreated,
		DateTimeModified,
		RowStatus,
		BatchUniqueIdentifier,
		BlackboardUsername,
		SyStudentId,
		FirstName,
		LastName,
		Campus,
		AdEnrollSchedId,
		AdClassSchedId,
		CourseUsersPrimaryKey,
		VAStudent,
		NoticeName,
		NoticeDueDate,
		VABenefitName,
		ClassStatus,
		AdEnrollId,
		IsRetake,
		StudentCourseUserKeys,
		ProgramCode,
		ProgramName,
		ProgramVersionCode,
		ProgramVersionName,
		StudentNumber,
		SourceSystem
	)
	SELECT DISTINCT
		gei.UserPK1 AS StudentPrimaryKey,
		u.DTCREATED AS DateTimeCreated,
		u.DTMODIFIED AS DateTimeModified,
		bs.[Description] AS RowStatus,
		gei.UserEPK AS BatchUniqueIdentifier,
		gei.UserUserId AS BlackboardUsername,
		REPLACE(gei.UserEPK, 'SyStudent_', '') AS SyStudentId,
		gei.UserFirstName AS FirstName,
		gei.UserLastName AS LastName,
		c.Descrip AS Campus,
		CAST(es.AdEnrollSchedID AS VARCHAR(100)) AS AdEnrollSchedId,
		REPLACE(gei.CourseEPK, 'AdCourse_', '') AS AdClassSchedId,
		gei.MembershipPK1 AS CourseUsersPrimaryKey,
		CASE
			WHEN vas.SyStudentId IS NOT NULL THEN 1 ELSE 0 END AS VAStudent,
		n.NoticeName AS NoticeName,
		n.NoticeDueDate AS NoticeDueDate,
		vas.BenefitName AS VABenefitName,
		es.[Status] AS ClassStatus,
		es.AdEnrollID AS AdEnrollId,
		CASE
			WHEN r.Tries > 1 THEN 1 ELSE 0 END AS IsRetake,
		CAST(gei.UserPK1 AS VARCHAR(50)) + CAST(gei.MembershipPK1 AS VARCHAR(50)) AS StudentCourseUserKeys,
		pr.Code AS ProgramCode,
		pr.Descrip AS ProgramName,
		pv.Code AS ProgramVersionCode,
		pv.Descrip AS ProgramVersionName,
		st.StuNum AS StudentNumber,
		gei.SourceSystem
	FROM stage.GradeExtractImport gei
		LEFT JOIN USERS u
			ON gei.UserPK1 = u.PK1
		LEFT JOIN stage.BlackboardStatuses bs
			ON u.ROW_STATUS = bs.PrimaryKey
			   AND bs.[TYPE] = 'Row'
		LEFT JOIN CV_Prod.dbo.AdClassSched cs
			ON REPLACE(gei.CourseEPK, 'AdCourse_', '') = CAST(cs.AdClassSchedID AS VARCHAR(50))
		LEFT JOIN CV_Prod.dbo.SyCampus c
			ON cs.SyCampusID = c.SyCampusID
		LEFT JOIN CV_Prod.dbo.AdEnrollSched es
			ON REPLACE(gei.CourseEPK, 'AdCourse_', '') = CAST(es.AdClassSchedID AS VARCHAR(50))
			   AND REPLACE(gei.UserEPK, 'SyStudent_', '') = CAST(es.SyStudentID AS VARCHAR(50))
			   AND es.[Status] IN ('C', 'S', 'P')
		LEFT JOIN #VAStudents vas
			ON REPLACE(gei.UserEPK, 'SyStudent_', '') = CAST(vas.SyStudentId AS VARCHAR(50))
		LEFT JOIN #Notices n
			ON REPLACE(gei.UserEPK, 'SyStudent_', '') = CAST(n.SyStudentId AS VARCHAR(50))
		LEFT JOIN #Retakes r
			ON es.SyStudentID = r.SyStudentId
			   AND es.AdEnrollID = r.AdEnrollId
			   AND es.AdCourseID = r.AdCourseId
		LEFT JOIN CV_Prod.dbo.AdEnroll en
			ON es.AdEnrollId = en.AdEnrollID
		LEFT JOIN CV_Prod.dbo.AdProgram pr
			ON en.AdProgramID = pr.AdProgramID
		LEFT JOIN CV_Prod.dbo.AdProgramVersion pv
			ON en.AdProgramVersionID = pv.AdProgramVersionID
		LEFT JOIN CV_Prod.dbo.SyStudent st
			ON en.SyStudentID = st.SyStudentId
		WHERE LEFT(gei.UserEPK, 9) = 'SyStudent' -- Only Students
		  AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8 -- Filter Out Test/Bad Students
		  AND LEFT(gei.CourseEPK, 8) = 'AdCourse' -- Only Courses
		  AND (gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%'
			   OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%'
			   OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%'
			   OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%'
			  )
		  AND gei.UserFirstName NOT LIKE 'BBAFL%' -- More Test Students
		  AND gei.UserEPK NOT LIKE '%PART1%' -- More Test Students
		  AND gei.UserEPK NOT LIKE '%PART2%' -- More Test Students
		  AND gei.UserEPK NOT LIKE '%PART3%' -- More Test Students
		  AND gei.UserEPK NOT LIKE '%PART4%' -- More Test Students
		  AND gei.USEREPK NOT LIKE '%PART5%'; -- More Test Students
        --AND gei.UserEPK <> 'SyStudent_2670907' AND gei.UserEPK <> 'SyStudent_4729014'             --Commented out this portion of the where 
        --clause which was excluding the loading of these students
		DROP TABLE #VAStudents;
		DROP TABLE #Notices;
		DROP TABLE #Retakes;

        EXEC LS_ODS.AddODSLoadLog 'Loaded Students Working Table', 0;

        --**************************************************************************************************************************************** 
        --Fill the stage.Courses table with all the values from the raw import table 
        --**************************************************************************************************************************************** 
		WITH cteInstructors (AdClassSchedId, PrimaryInstructorId, PrimaryInstructor, SecondaryInstructorId, SecondaryInstructor)
		AS (
			SELECT 
				cs.AdClassSchedID AS AdClassSchedId,
				spi.SyStaffID AS PrimaryInstructorId,
				spi.LastName + ', ' + spi.FirstName AS PrimaryInstructor,
				spi2.SyStaffID AS SecondaryInstructorId,
				spi2.LastName + ', ' + spi2.FirstName AS SecondaryInstructor
			FROM 
				CV_Prod.dbo.AdClassSched cs 
				LEFT JOIN CV_Prod.dbo.SyStaff spi ON cs.AdTeacherID = spi.SyStaffID
				LEFT JOIN CV_Prod.dbo.AdClassSchedInstructorAttributes t ON cs.AdClassSchedID = t.AdClassSchedID AND t.AdInstructorAttributesID = 2
				LEFT JOIN CV_Prod.dbo.SyStaff spi2 ON t.AdTeacherID = spi2.SyStaffID
		)
		INSERT INTO stage.Courses (
			CoursePrimaryKey,
			DateTimeCreated,
			DateTimeModified,
			RowStatus,
			BatchUniqueIdentifier,
			CourseCode,
			CourseName,
			SectionNumber,
			AdClassSchedId,
			PrimaryInstructor,
			SecondaryInstructor,
			IsOrganization,
			AcademicFacilitator,
			PrimaryInstructorId,
			SecondaryInstructorId,
			AcademicFacilitatorId,
			SourceSystem
		)
		SELECT DISTINCT
			gei.CoursePK1 AS CoursePrimaryKey,
			cm.DTCREATED AS DateTimeCreated,
			cm.DTMODIFIED AS DateTimeModified,
			bs.[Description] AS RowStatus,
			gei.CourseEPK AS BatchUniqueIdentifier,
			LEFT(gei.CourseTitle, CHARINDEX(':', gei.CourseTitle) - 1) AS CourseCode,
			LTRIM(RTRIM(SUBSTRING(gei.CourseTitle, CHARINDEX(':', gei.CourseTitle) + 2, CHARINDEX('(', gei.CourseTitle) - 10))) AS CourseName,
			LTRIM(RTRIM(REVERSE(LEFT(REVERSE(gei.CourseTitle), CHARINDEX(' ', REVERSE(gei.CourseTitle)))))) AS SectionNumber,
			REPLACE(gei.CourseEPK, 'AdCourse_', '') AS AdClassSchedId,
			i.PrimaryInstructor AS PrimaryInstructor,
			i.SecondaryInstructor AS SecondaryInstructor,
			CASE
				WHEN cm.SERVICE_LEVEL = 'F' THEN 0
				WHEN cm.SERVICE_LEVEL = 'C' THEN 1
				ELSE NULL
			END AS IsOrganization,
			st.LastName + ', ' + st.FirstName AS AcademicFacilitator,
			i.PrimaryInstructorId,
			i.SecondaryInstructorId,
			st.SyStaffID AS AcademicFacilitatorId,
			gei.SourceSystem
		FROM 
			stage.GradeExtractImport gei WITH (ROWLOCK)
			LEFT JOIN COURSE_MAIN cm WITH (ROWLOCK) ON gei.CoursePK1 = cm.PK1
			LEFT JOIN stage.BlackboardStatuses bs WITH (ROWLOCK) ON cm.ROW_STATUS = bs.PrimaryKey AND bs.[Type] = 'Row'
			LEFT JOIN cteInstructors i WITH (ROWLOCK) ON REPLACE(gei.CourseEPK, 'AdCourse_', '') = CAST(i.AdClassSchedId AS VARCHAR(50))
			LEFT JOIN FREEDOM.dbo.TutorAssignment ta WITH (ROWLOCK) ON REPLACE(gei.CourseEPK, 'AdCourse_', '') = CAST(ta.AdClassSchedID AS VARCHAR(50))
			LEFT JOIN CV_Prod.dbo.SyStaff st WITH (ROWLOCK) ON ta.FacilitatorID = st.SyStaffID
		WHERE 
			LEFT(gei.UserEPK, 9) = 'SyStudent' --Only Students 
			AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8 --Filter Out Test/Bad Students 
			AND LEFT(gei.CourseEPK, 8) = 'AdCourse' --Only Courses 
			AND (
				gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --Filter Out Test/Bad Courses 
				OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --To bring in CLW courses 
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%' --2/28/2024 CML: Captures EMT Courses
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%' --2/28/2024 CML: Captures EMT Courses based out of CLW
			)
			AND gei.UserFirstName NOT LIKE 'BBAFL%' --More Test Students
			AND gei.UserEPK NOT LIKE '%PART1%' --More Test Students
			AND gei.UserEPK NOT LIKE '%PART2%' --More Test Students
			AND gei.UserEPK NOT LIKE '%PART3%' --More Test Students
			AND gei.UserEPK NOT LIKE '%PART4%' --More Test Students
			AND gei.USEREPK NOT LIKE '%PART5%'; --More Test Students

        EXEC LS_ODS.AddODSLoadLog 'Loaded Courses Working Table', 0;

        --**************************************************************************************************************************************** 
        --Fill the stage.Assignments table with all the values from the raw import table 
        --**************************************************************************************************************************************** 

     -- Drop constraint and indexes
		ALTER TABLE [stage].[Assignments] DROP CONSTRAINT [PK_Assignments_2]
		WITH (ONLINE = OFF);

		DROP INDEX IF EXISTS [idx_Assignments_3] ON [stage].[Assignments];
		DROP INDEX IF EXISTS [idx_ODS_004] ON [stage].[Assignments];
		DROP INDEX IF EXISTS [idx_ODS_005] ON [stage].[Assignments];

		-- Insert into stage.Assignments with optimized SELECT
		INSERT INTO stage.Assignments (
			AssignmentPrimaryKey,
			CoursePrimaryKey,
			WeekNumber,
			AssignmentTitle,
			DueDate,
			PossiblePoints,
			DateTimeCreated,
			DateTimeModified,
			ScoreProviderHandle,
			CourseContentsPrimaryKey1,
			AlternateTitle,
			IsReportable,
			CountsAsSubmission,
			SourceSystem
		)
		SELECT DISTINCT
			gei.AssignmentPK1 AS AssignmentPrimaryKey,
			gei.CoursePK1 AS CoursePrimaryKey,
			CASE
				WHEN LEFT(gei.AssignmentDisplayColumnName, 4) = 'Week' AND ISNUMERIC(SUBSTRING(gei.AssignmentDisplayColumnName, 6, 2)) = 1 THEN SUBSTRING(gei.AssignmentDisplayColumnName, 6, 2)
				WHEN LEFT(gei.AssignmentDisplayColumnName, 4) = 'Week' AND ISNUMERIC(SUBSTRING(gei.AssignmentDisplayColumnName, 6, 1)) = 1 THEN SUBSTRING(gei.AssignmentDisplayColumnName, 6, 1)
				WHEN LEFT(gei.AssignmentDisplayColumnName, 3) = 'Wk ' AND ISNUMERIC(SUBSTRING(gei.AssignmentDisplayColumnName, 4, 2)) = 1 THEN SUBSTRING(gei.AssignmentDisplayColumnName, 4, 2)
				WHEN LEFT(gei.AssignmentDisplayColumnName, 3) = 'Wk ' AND ISNUMERIC(SUBSTRING(gei.AssignmentDisplayColumnName, 4, 1)) = 1 THEN SUBSTRING(gei.AssignmentDisplayColumnName, 4, 1)
				ELSE 0
			END AS WeekNumber,
			CASE
				WHEN LEFT(gei.AssignmentDisplayColumnName, 4) = 'Week' THEN
					CASE
						WHEN SUBSTRING(gei.AssignmentDisplayColumnName, 8, 2) = '- ' THEN LTRIM(RTRIM(SUBSTRING(gei.AssignmentDisplayColumnName, 10, 1000)))
						ELSE LTRIM(RTRIM(SUBSTRING(gei.AssignmentDisplayColumnName, 8, 1000)))
					END
				WHEN LEFT(gei.AssignmentDisplayColumnName, 3) = 'Wk ' THEN
					LTRIM(RTRIM(SUBSTRING(gei.AssignmentDisplayColumnName, 8, 1000)))
				ELSE gei.AssignmentDisplayColumnName
			END AS AssignmentTitle,
			gm.DUE_DATE AS DueDate,
			REPLACE(gei.AssignmentPointsPossible, '"', '') AS PossiblePoints,
			gm.DATE_ADDED AS DateTimeCreated,
			gm.DATE_MODIFIED AS DateTimeModified,
			gm.SCORE_PROVIDER_HANDLE AS ScoreProviderHandle,
			gm.COURSE_CONTENTS_PK1 AS CourseContentsPrimaryKey1,
			gei.AssignmentDisplayTitle AS AlternateTitle,
			1 AS IsReportable,
			1 AS CountsAsSubmission,
			gei.SourceSystem AS SourceSystem
		FROM stage.GradeExtractImport gei
		LEFT JOIN GRADEBOOK_MAIN gm ON gei.AssignmentPK1 = gm.PK1
		WHERE LEFT(gei.UserEPK, 9) = 'SyStudent'
			AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8
			AND LEFT(gei.CourseEPK, 8) = 'AdCourse'
			AND (
				gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%'
				OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%'
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%'
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%'
			)
			AND gei.UserFirstName NOT LIKE 'BBAFL%'
			AND gei.UserEPK NOT LIKE '%PART[1-5]%'
			AND gei.AssignmentDisplayTitle NOT LIKE '% Extended %'
			AND gei.AssignmentDisplayTitle NOT LIKE '%Grade %'
			AND (
				(gei.AssignmentDisplayColumnName = 'Final Grade' AND gei.AssignmentIsExternalGradeIndicator = 'Y')
				OR (gei.AssignmentDisplayColumnName <> 'Final Grade' AND gei.AssignmentIsExternalGradeIndicator = 'N')
			);

		-- Update AssignmentType
		UPDATE stage.Assignments
		SET AssignmentType = COALESCE(REPLACE(gt.NAME, '.name', ''), 
			CASE
				WHEN asg.ScoreProviderHandle IN ('resource/x-bb-assignment', 'resource/mcgraw-hill-assignment', 'resource/x-bb-assessment') 
					 OR asg.AssignmentTitle LIKE '%Assign%' THEN 'Assignment'
				WHEN asg.ScoreProviderHandle = 'resource/x-bb-assessment' THEN 'Test'
				WHEN asg.ScoreProviderHandle = 'resource/x-bb-forumlink' THEN 'Discussion'
				WHEN asg.ScoreProviderHandle = 'resource/x-plugin-scormengine' THEN 'SCORM/AICC'
				-- D2L ScoreProviderHandle
				WHEN asg.ScoreProviderHandle = 'resource/d2l/Assessment' THEN 'Assessment'
				WHEN asg.ScoreProviderHandle = 'resource/d2l/Assignments' THEN 'Assignment'
				WHEN asg.ScoreProviderHandle = 'resource/d2l/Discussions' THEN 'Discussion'
				WHEN asg.ScoreProviderHandle = 'resource/d2l/ExtraCredit' THEN 'Extra Credit'
				WHEN asg.ScoreProviderHandle = 'resource/d2l/RollCall' THEN 'Roll Call'
				WHEN asg.ScoreProviderHandle = 'resource/d2l/SCORM' THEN 'SCORM/AICC'
				ELSE 'Unknown'
			END)
		FROM stage.Assignments asg
		INNER JOIN stage.Courses co ON asg.CoursePrimaryKey = co.CoursePrimaryKey
		LEFT JOIN dbo.GRADEBOOK_MAIN gm ON asg.CourseContentsPrimaryKey1 = gm.COURSE_CONTENTS_PK1 AND co.CoursePrimaryKey = gm.CRSMAIN_PK1
		LEFT JOIN dbo.GRADEBOOK_TYPE gt ON gm.GRADEBOOK_TYPE_PK1 = gt.PK1;

		-- Recreate constraints and indexes
		ALTER TABLE [stage].[Assignments]
		ADD CONSTRAINT [PK_Assignments_2] PRIMARY KEY CLUSTERED (
			[AssignmentPrimaryKey] ASC,
			[CoursePrimaryKey] ASC
		) WITH (
			PAD_INDEX = OFF,
			STATISTICS_NORECOMPUTE = OFF,
			SORT_IN_TEMPDB = OFF,
			IGNORE_DUP_KEY = OFF,
			ONLINE = OFF,
			ALLOW_ROW_LOCKS = ON,
			ALLOW_PAGE_LOCKS = ON,
			FILLFACTOR = 80
		) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX [idx_Assignments_3] ON [stage].[Assignments] (
			[CountsAsSubmission] ASC,
			[WeekNumber] ASC
		) INCLUDE (
			[CoursePrimaryKey],
			[AssignmentTitle],
			[PossiblePoints]
		) WITH (
			PAD_INDEX = OFF,
			STATISTICS_NORECOMPUTE = OFF,
			SORT_IN_TEMPDB = OFF,
			DROP_EXISTING = OFF,
			ONLINE = OFF,
			ALLOW_ROW_LOCKS = ON,
			ALLOW_PAGE_LOCKS = ON,
			FILLFACTOR = 80
		) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX [idx_ODS_004] ON [stage].[Assignments] ([AssignmentTitle] ASC) INCLUDE (
			[AssignmentPrimaryKey],
			[CoursePrimaryKey]
		) WITH (
			PAD_INDEX = OFF,
			STATISTICS_NORECOMPUTE = OFF,
			SORT_IN_TEMPDB = OFF,
			DROP_EXISTING = OFF,
			ONLINE = OFF,
			ALLOW_ROW_LOCKS = ON,
			ALLOW_PAGE_LOCKS = ON
		) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX [idx_ODS_005] ON [stage].[Assignments] (
			[CoursePrimaryKey] ASC,
			[CountsAsSubmission] ASC,
			[WeekNumber] ASC
		) INCLUDE (
			[AssignmentTitle],
			[PossiblePoints]
		) WITH (
			PAD_INDEX = OFF,
			STATISTICS_NORECOMPUTE = OFF,
			SORT_IN_TEMPDB = OFF,
			DROP_EXISTING = OFF,
			ONLINE = OFF,
			ALLOW_ROW_LOCKS = ON,
			ALLOW_PAGE_LOCKS = ON,
			FILLFACTOR = 80
		) ON [PRIMARY];

        EXEC LS_ODS.AddODSLoadLog 'Loaded Assignments Working Table', 0;

        --**************************************************************************************************************************************** 
        --Fill the stage.Grades table with all the values from the raw import table 
        --****************************************************************************************************************************************
        --All Assignments With A Primary Key
		INSERT INTO stage.Grades
			(
				GradePrimaryKey,
				CourseUsersPrimaryKey,
				RowStatus,
				HighestScore,
				HighestGrade,
				HighestAttemptDateTime,
				ManualScore,
				ManualGrade,
				ManualDateTime,
				ExemptIndicator,
				HighestDateTimeCreated,
				HighestDateTimeModified,
				HighestIsLatestAttemptIndicator,
				FirstScore,
				FirstGrade,
				FirstAttemptDateTime,
				FirstIsLatestAttemptIndicator,
				FirstDateTimeCreated,
				FirstDateTimeModified,
				AssignmentPrimaryKey,
				AssignmentStatus,
				SourceSystem
			)
			SELECT DISTINCT
				gei.GradePK1 AS GradePrimaryKey,
				cu.PK1 AS CourseUsersPrimaryKey,
				bs.[Description] AS RowStatus,
				gei.GradeDisplayScore AS HighestScore,
				gei.GradeDisplayGrade AS HighestGrade,
				gei.GradeAttemptDate AS HighestAttemptDateTime,
				gei.GradeManualScore AS ManualScore,
				gei.GradeManualGrade AS ManualGrade,
				gei.GradeOverrideDate AS ManualDateTime,
				gei.GradeExemptIndicator AS ExemptIndicator,
				ha.DATE_ADDED AS HighestDateTimeCreated,
				ha.DATE_MODIFIED AS HighestDateTimeModified,
				CASE
					WHEN gg.HIGHEST_ATTEMPT_PK1 = gg.LAST_ATTEMPT_PK1 THEN 1
					ELSE 0
				END AS HighestIsLatestAttemptIndicator,
				fa.SCORE AS FirstScore,
				fa.GRADE AS FirstGrade,
				fa.ATTEMPT_DATE AS FirstAttemptDateTime,
				CASE
					WHEN gg.FIRST_ATTEMPT_PK1 = gg.LAST_ATTEMPT_PK1 THEN 1
					ELSE 0
				END AS FirstIsLatestAttemptIndicator,
				fa.DATE_ADDED AS FirstDateTimeCreated,
				fa.DATE_MODIFIED AS FirstDateTimeModified,
				gei.AssignmentPK1 AS AssignmentPrimaryKey,
				CASE
					WHEN gei.GradeAttemptStatus IS NULL AND gei.GradeAttemptDate IS NULL THEN 'NOT COMPLETE'
					ELSE gs.[Description]
				END AS AssignmentStatus,
				gei.SourceSystem AS SourceSystem
			FROM
				stage.GradeExtractImport gei
			LEFT JOIN
				COURSE_USERS cu ON gei.UserPK1 = cu.USERS_PK1 AND gei.CoursePK1 = cu.CRSMAIN_PK1
			LEFT JOIN
				GRADEBOOK_GRADE gg ON gei.GradePK1 = gg.PK1
			LEFT JOIN
				stage.BlackboardStatuses bs ON gg.[STATUS] = bs.PrimaryKey AND bs.[Type] = 'Row'
			LEFT JOIN
				ATTEMPT ha ON gg.HIGHEST_ATTEMPT_PK1 = ha.PK1
			LEFT JOIN
				ATTEMPT fa ON gg.FIRST_ATTEMPT_PK1 = fa.PK1
			LEFT JOIN
				stage.BlackboardStatuses gs ON gei.GradeAttemptStatus = gs.PrimaryKey AND gs.[Type] = 'Grade'
			LEFT JOIN
				dbo.DATA_SOURCE ds ON ds.PK1 = cu.DATA_SRC_PK1 -- Adding to deal with erroneous DSKs added in the SIS Framework cleanup effort
			WHERE
				LEFT(gei.UserEPK, 9) = 'SyStudent' -- Only Students
				AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8 -- Filter Out Test/Bad Students
				AND LEFT(gei.CourseEPK, 8) = 'AdCourse' -- Only Courses
				AND (
					gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' -- Filter Out Test/Bad Courses
					OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' -- To bring in CLW courses
					OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%' -- Captures EMT Courses
					OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%' -- Captures EMT Courses based out of CLW
				)
				AND gei.UserFirstName NOT LIKE 'BBAFL%' -- More Test Students
				AND gei.UserEPK NOT LIKE '%PART1%' -- More Test Students
				AND gei.UserEPK NOT LIKE '%PART2%' -- More Test Students
				AND gei.UserEPK NOT LIKE '%PART3%' -- More Test Students
				AND gei.UserEPK NOT LIKE '%PART4%' -- More Test Students
				AND gei.USEREPK NOT LIKE '%PART5%' -- More Test Students
				AND gei.AssignmentDisplayColumnName LIKE '%IEHR%' -- IEHR Only
				AND gei.GradePK1 IS NULL -- SCORM IEHR Only
				AND gei.GradeManualGrade IS NOT NULL -- Student Has Completed The Assignment
				AND ds.batch_uid NOT IN ('ENR_181008_02.txt', 'ENR_181008', 'ENR_181008_1558036.txt') -- Adding to deal with erroneous DSKs added in the SIS Framework cleanup effort

        --in the SIS Framework cleanup effort
        EXEC LS_ODS.AddODSLoadLog 'Loaded Grades Working Table', 0;

        --**************************************************************************************************************************************** 
        --Update the IEHR Assignment statuses in the stage.Grades table 
        --**************************************************************************************************************************************** 
		--        -- Add indexes if not already present
		--CREATE INDEX IX_Grades_AssignmentPrimaryKey ON stage.Grades (AssignmentPrimaryKey);
		--CREATE INDEX IX_Assignments_AssignmentPrimaryKey ON stage.Assignments (AssignmentPrimaryKey);
		--CREATE INDEX IX_Grades_HighestScore ON stage.Grades (HighestScore);
		
		UPDATE g
        SET g.AssignmentStatus = 'COMPLETED'
        FROM stage.Grades g
            INNER JOIN stage.Assignments a
                ON g.AssignmentPrimaryKey = a.AssignmentPrimaryKey
                   --AND a.AssignmentTitle = 'IEHR Assign' 
                   AND g.HighestScore IS NOT NULL
                   AND g.HighestScore <> 0;

        EXEC LS_ODS.AddODSLoadLog 'Updated IEHR Assignment Statuses', 0;

        --**************************************************************************************************************************************** 
        --Update the stage.Students table with calculated values 
        --**************************************************************************************************************************************** 
    -- Ensure there are indexes on the join columns if not already present
		CREATE INDEX IX_StudentActivityLog_SyStudentID_EventId ON RTSATWeb.dbo.StudentActivityLog (SyStudentID, EventId);
		CREATE INDEX IX_Login_UserPK ON RTSAT.[Login] (UserPK);
		CREATE INDEX IX_User_UserPK ON RTSAT.[User] (UserPK);
		CREATE INDEX IX_Students_SyStudentId ON stage.Students (SyStudentId);

		WITH cteLastLogins (SyStudentId, LastLoginDateTime) AS (
			SELECT jq.SyStudentId,
				   MAX(jq.LastLoginDateTime) AS LastLoginDateTime
			FROM (
				-- Uncomment and include additional sources if needed
				-- SELECT 
				--     sal.SyStudentID AS SyStudentId, 
				--     MAX(sal.EventTime) AS LastLoginDateTime 
				-- FROM RTSATWeb.dbo.StudentActivityLog sal  
				-- WHERE EventId = 1 
				-- GROUP BY 
				--     sal.SyStudentID 
				-- UNION ALL 
				SELECT us.SyStudentId AS SyStudentId,
					   MAX(lo.LoginDateTime) AS LastLoginDateTime
				FROM RTSAT.[Login] lo 
				INNER JOIN RTSAT.[User] us 
					ON lo.UserPK = us.UserPK
				GROUP BY us.SyStudentId
			) AS jq
			GROUP BY jq.SyStudentId
		)
		UPDATE s WITH (ROWLOCK)
		SET s.LastLoginDateTime = ll.LastLoginDateTime
		FROM stage.Students s WITH (ROWLOCK)
		INNER JOIN cteLastLogins ll 
			ON s.SyStudentId = ll.SyStudentId
		OPTION (OPTIMIZE FOR UNKNOWN);

    EXEC LS_ODS.AddODSLoadLog 'Updated Student Last Logins', 0;

        --**************************************************************************************************************************************** 
        --Update the stage.Students table with the time in class 
        --**************************************************************************************************************************************** 
       DECLARE @FilterDate DATE;
		SET @FilterDate = DATEADD(DAY, -90, GETDATE());

		-- Check for temp table and delete if it exists 
		IF OBJECT_ID('tempdb..#TimeInClassTemp') IS NOT NULL
			DROP TABLE #TimeInClassTemp;

		CREATE TABLE #TimeInClassTemp
		(
			USER_PK1 INT,
			COURSE_PK1 INT,
			[DayOfWeek] INT,
			TimeInClass NUMERIC(12, 2)
		);

		WITH cteTimeInClass AS
		(
			SELECT
				aa.USER_PK1,
				aa.COURSE_PK1,
				DATEPART(WEEKDAY, aa.[TIMESTAMP]) AS [DayOfWeek],
				SUM(CAST(DATEDIFF(SECOND, MIN(aa.[TIMESTAMP]), MAX(aa.[TIMESTAMP])) AS NUMERIC(12, 2)) / 3600) AS TimeInClass
			FROM
				ACTIVITY_ACCUMULATOR aa
			WHERE
				aa.COURSE_PK1 IS NOT NULL
				AND aa.USER_PK1 IS NOT NULL
				AND aa.[TIMESTAMP] >= @FilterDate
			GROUP BY
				aa.USER_PK1,
				aa.COURSE_PK1,
				DATEPART(WEEKDAY, aa.[TIMESTAMP])
		)
		INSERT INTO #TimeInClassTemp (USER_PK1, COURSE_PK1, [DayOfWeek], TimeInClass)
		SELECT USER_PK1, COURSE_PK1, [DayOfWeek], TimeInClass
		FROM cteTimeInClass;

		WITH cteTotal AS
		(
			SELECT
				USER_PK1,
				COURSE_PK1,
				SUM(TimeInClass) AS TotalTimeInClass
			FROM
				#TimeInClassTemp
			GROUP BY
				USER_PK1,
				COURSE_PK1
		)
		UPDATE s
		SET s.TimeInClass = cte.TotalTimeInClass
		FROM
			stage.Students s
			INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
			INNER JOIN cteTotal cte ON cu.USERS_PK1 = cte.USER_PK1 AND cu.CRSMAIN_PK1 = cte.COURSE_PK1;

		UPDATE s
		SET s.MondayTimeInClass = tic.TimeInClass
		FROM
			stage.Students s
			INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
			INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1 AND cu.CRSMAIN_PK1 = tic.COURSE_PK1 AND tic.[DayOfWeek] = 2;

		UPDATE s
		SET s.TuesdayTimeInClass = tic.TimeInClass
		FROM
			stage.Students s
			INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
			INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1 AND cu.CRSMAIN_PK1 = tic.COURSE_PK1 AND tic.[DayOfWeek] = 3;

		UPDATE s
		SET s.WednesdayTimeInClass = tic.TimeInClass
		FROM
			stage.Students s
			INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
			INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1 AND cu.CRSMAIN_PK1 = tic.COURSE_PK1 AND tic.[DayOfWeek] = 4;

		UPDATE s
		SET s.ThursdayTimeInClass = tic.TimeInClass
		FROM
			stage.Students s
			INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
			INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1 AND cu.CRSMAIN_PK1 = tic.COURSE_PK1 AND tic.[DayOfWeek] = 5;

		UPDATE s
		SET s.FridayTimeInClass = tic.TimeInClass
		FROM
			stage.Students s
			INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
			INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1 AND cu.CRSMAIN_PK1 = tic.COURSE_PK1 AND tic.[DayOfWeek] = 6;

		UPDATE s
		SET s.SaturdayTimeInClass = tic.TimeInClass
		FROM
			stage.Students s
			INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
			INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1 AND cu.CRSMAIN_PK1 = tic.COURSE_PK1 AND tic.[DayOfWeek] = 7;

		UPDATE s
		SET s.SundayTimeInClass = tic.TimeInClass
		FROM
			stage.Students s
			INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
			INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1 AND cu.CRSMAIN_PK1 = tic.COURSE_PK1 AND tic.[DayOfWeek] = 1;

        EXEC LS_ODS.AddODSLoadLog 'Updated Student Times In Class', 0;

        --**************************************************************************************************************************************** 
        --Update the stage.Students table with I3 interaction information 
        --**************************************************************************************************************************************** 
        --Define needed variables 
        DECLARE @I3CurrentDateTime DATETIME;
		DECLARE @LastUpdatedDateTime DATETIME;
		DECLARE @RemoteQuery NVARCHAR(4000);

		-- Populate needed variables
		SET @I3CurrentDateTime = GETDATE();

		-- Create table to hold new/updated calls
		DECLARE @Calls TABLE
		(
			PhoneNumber VARCHAR(50) NOT NULL PRIMARY KEY,
			LastInteractionDateTime DATETIME,
			SourceSystem VARCHAR(50) NULL
		);

		DECLARE @CallsBTB TABLE
		(
			PhoneNumber VARCHAR(50) NOT NULL PRIMARY KEY,
			LastInteractionDateTime DATETIME
		);

		DECLARE @CallsMCS TABLE
		(
			PhoneNumber VARCHAR(50) NOT NULL PRIMARY KEY,
			LastInteractionDateTime DATETIME
		);

		DECLARE @CallsCombined TABLE
		(
			PhoneNumber VARCHAR(50) NOT NULL,
			LastInteractionDateTime DATETIME,
			SourceSystem VARCHAR(50) NOT NULL,
			PRIMARY KEY (PhoneNumber, SourceSystem)
		);

		-- Get the most recent time the I3 Interactions was updated
		SET @LastUpdatedDateTime = (SELECT MAX(i3.LastUpdatedDateTime) FROM LS_ODS.I3Interactions i3);

		-- Back to Basics (BTB) Interaction Data
		INSERT INTO @CallsBTB (PhoneNumber, LastInteractionDateTime)
		SELECT 
			REPLACE(btbcalldetail.RemoteNumber, '+', '') AS PhoneNumber,
			MAX(btbcalldetail.InitiatedDate) AS LastInteractionDateTime
		FROM 
			[COL-TEL-P-SQ01].I3_IC_PROD.dbo.CallDetail_viw btbcalldetail
		WHERE 
			btbcalldetail.CallType = 'External'
			AND RTRIM(LTRIM(btbcalldetail.RemoteNumber)) <> ''
			AND btbcalldetail.CallDurationSeconds >= 90
			AND LEN(REPLACE(btbcalldetail.RemoteNumber, '+', '')) = 10
			AND ISNUMERIC(REPLACE(btbcalldetail.RemoteNumber, '+', '')) = 1
			AND btbcalldetail.InitiatedDate >= @LastUpdatedDateTime
		GROUP BY 
			REPLACE(btbcalldetail.RemoteNumber, '+', '');

		-- MCS Interaction Data
		SET @RemoteQuery = N'
		SELECT 
			MAX(DATEADD(SECOND, I.StartDTOffset, I.InitiatedDateTimeUTC)) AS LastInteractionDateTime,
			CASE 
				WHEN LEN(REPLACE(I.RemoteID, ''+'', '''')) = 0 OR REPLACE(I.RemoteID, ''+'', '''') IS NULL 
				THEN ''-'' 
				ELSE I.RemoteID 
			END as PhoneNumber
		FROM    
			MCS_I3_IC.dbo.InteractionSummary I
		WHERE 
			DATEADD(SECOND, I.StartDTOffset, I.InitiatedDateTimeUTC) > ''' + CAST(@LastUpdatedDateTime AS NVARCHAR(25)) + '''
			AND ConnectedDateTimeUTC > ''' + CAST(@LastUpdatedDateTime AS NVARCHAR(25)) + '''
			AND TerminatedDateTimeUTC > ''' + CAST(@LastUpdatedDateTime AS NVARCHAR(25)) + '''
			AND I.ConnectionType = 1
			AND LEN(I.RemoteID) > 0
			AND LEN(REPLACE(I.RemoteID, ''+'', '''')) = 10 
			AND ISNUMERIC(REPLACE(I.RemoteID, ''+'', '''')) = 1 
			AND CAST(ROUND(DATEDIFF(MILLISECOND, ConnectedDateTimeUTC, TerminatedDateTimeUTC) / 1000.000, 0) AS BIGINT) > 90
			AND DATEDIFF(DAY, ConnectedDateTimeUTC, TerminatedDateTimeUTC) < 23
		GROUP BY 
			I.RemoteID';

		INSERT INTO @CallsMCS (PhoneNumber, LastInteractionDateTime)
		EXEC [COL-MCS-P-SQ02].master.dbo.sp_executesql @RemoteQuery;

		-- Combine calls from BTB and MCS
		INSERT INTO @CallsCombined (PhoneNumber, LastInteractionDateTime, SourceSystem)
		SELECT 
			PhoneNumber,
			LastInteractionDateTime,
			'BTB'
		FROM 
			@CallsBTB
		UNION ALL
		SELECT 
			PhoneNumber,
			LastInteractionDateTime,
			'MCS'
		FROM 
			@CallsMCS;

		-- Add the new/updated calls into the table variable
		WITH cteCalls AS
		(
			SELECT 
				PhoneNumber,
				MAX(LastInteractionDateTime) AS LastInteractionDateTime
			FROM 
				@CallsCombined
			GROUP BY 
				PhoneNumber
		)
		INSERT INTO @Calls (PhoneNumber, LastInteractionDateTime, SourceSystem)
		SELECT 
			cc.PhoneNumber,
			cc.LastInteractionDateTime,
			cc.SourceSystem
		FROM 
			@CallsCombined cc
		INNER JOIN 
			cteCalls ca ON cc.PhoneNumber = ca.PhoneNumber AND cc.LastInteractionDateTime = ca.LastInteractionDateTime;

		-- Update the phone numbers that have a new interaction date/time
		UPDATE i3
		SET 
			i3.LastInteractionDateTime = c.LastInteractionDateTime,
			i3.SourceSystem = c.SourceSystem,
			i3.LastUpdatedDateTime = @I3CurrentDateTime
		FROM 
			LS_ODS.I3Interactions i3
		INNER JOIN 
			@Calls c ON i3.PhoneNumber = c.PhoneNumber;

		-- Add new phone numbers that don't exist in the interactions table
		INSERT INTO LS_ODS.I3Interactions (PhoneNumber, LastInteractionDateTime, SourceSystem, LastUpdatedDateTime)
		SELECT 
			c.PhoneNumber,
			c.LastInteractionDateTime,
			c.SourceSystem,
			@I3CurrentDateTime
		FROM 
			@Calls c
		WHERE 
			c.PhoneNumber NOT IN (SELECT i3.PhoneNumber FROM LS_ODS.I3Interactions i3);

		-- Update student information with the latest interaction data
		UPDATE s
		SET 
			s.LastI3InteractionNumberMainPhone = mpi.PhoneNumber,
			s.LastI3InteractionDateTimeMainPhone = mpi.LastInteractionDateTime,
			s.DaysSinceLastI3InteractionMainPhone = DATEDIFF(DAY, mpi.LastInteractionDateTime, @I3CurrentDateTime),
			s.LastI3InteractionNumberWorkPhone = wpi.PhoneNumber,
			s.LastI3InteractionDateTimeWorkPhone = wpi.LastInteractionDateTime,
			s.DaysSinceLastI3InteractionWorkPhone = DATEDIFF(DAY, wpi.LastInteractionDateTime, @I3CurrentDateTime),
			s.LastI3InteractionNumberMobilePhone = mopi.PhoneNumber,
			s.LastI3InteractionDateTimeMobilePhone = mopi.LastInteractionDateTime,
			s.DaysSinceLastI3InteractionMobilePhone = DATEDIFF(DAY, mopi.LastInteractionDateTime, @I3CurrentDateTime),
			s.LastI3InteractionNumberOtherPhone = opi.PhoneNumber,
			s.LastI3InteractionDateTimeOtherPhone = opi.LastInteractionDateTime,
			s.DaysSinceLastI3InteractionOtherPhone = DATEDIFF(DAY, opi.LastInteractionDateTime, @I3CurrentDateTime)
		FROM 
			stage.Students s
		INNER JOIN 
			CV_Prod.dbo.SyStudent cvs ON s.SyStudentID = cvs.SyStudentId
		LEFT JOIN 
			LS_ODS.I3Interactions mpi ON REPLACE(REPLACE(REPLACE(REPLACE(RTRIM(LTRIM(cvs.Phone)), '-', ''), '*', ''), '(', ''), ')', '') = mpi.PhoneNumber
		LEFT JOIN 
			LS_ODS.I3Interactions wpi ON REPLACE(REPLACE(REPLACE(REPLACE(RTRIM(LTRIM(cvs.WorkPhone)), '-', ''), '*', ''), '(', ''), ')', '') = wpi.PhoneNumber
		LEFT JOIN 
			LS_ODS.I3Interactions mopi ON REPLACE(REPLACE(REPLACE(REPLACE(RTRIM(LTRIM(cvs.MobileNumber)), '-', ''), '*', ''), '(', ''), ')', '') = mopi.PhoneNumber
		LEFT JOIN 
			LS_ODS.I3Interactions opi ON REPLACE(REPLACE(REPLACE(REPLACE(RTRIM(LTRIM(cvs.OtherPhone)), '-', ''), '*', ''), '(', ''), ')', '') = opi.PhoneNumber;

        EXEC LS_ODS.AddODSLoadLog 'Updated Student Last I3 Interactions', 0;

        --**************************************************************************************************************************************** 
        --Update the stage.Students table with the weekly grades 
        --NEED TO PERFORM ANALYSIS TO ACCOUNT FOR 16 WEEK EMT PROGRAM -cml 2/28/2024
        --stage.Courses only has columns for weeks 1 - 5 -cml 2/28/2024
        --EMT has a week 6 grade and a Final Percentage only, only Final Percentage will be placed in week 5 based on current logic -cml 2/28/2024
        --**************************************************************************************************************************************** 
        WITH cteWeeklyGrades (StudentPrimaryKey, CoursePrimaryKey, WeekNumber, WeeklyGrade)
        AS (SELECT gei.UserPK1 'StudentPrimaryKey',
                   gei.CoursePK1 'CoursePrimaryKey',
                   CASE
                       WHEN gei.AssignmentDisplayTitle IN ( 'Week 1 Grade %', 'Week 1 Grade (%)' ) THEN
                           1
                       WHEN gei.AssignmentDisplayTitle IN ( 'Week 2 Grade %', 'Week 2 Grade (%)' ) THEN
                           2
                       WHEN gei.AssignmentDisplayTitle IN ( 'Week 3 Grade %', 'Week 3 Grade (%)' ) THEN
                           3
                       WHEN gei.AssignmentDisplayTitle IN ( 'Week 4 Grade %', 'Week 4 Grade (%)' ) THEN
                           4
                       ELSE
                           5
                   END 'WeekNumber',
                   (CAST(gei.GradeManualScore AS NUMERIC(12, 2))
                    / CAST(REPLACE(gei.AssignmentPointsPossible, '"', '') AS NUMERIC(12, 2))
                   ) 'WeeklyGrade'
            FROM stage.GradeExtractImport gei
            WHERE gei.AssignmentDisplayTitle IN ( 'Week 1 Grade %', 'Week 2 Grade %', 'Week 3 Grade %',
                                                  'Week 4 Grade %', 'Week 1 Grade (%)', 'Week 2 Grade (%)',
                                                  'Week 3 Grade (%)', 'Week 4 Grade (%)', 'Final Percentage'
                                                )
                  AND CAST(REPLACE(gei.AssignmentPointsPossible, '"', '') AS NUMERIC(12, 2)) <> 0
           )
        UPDATE s
        SET s.Week1Grade = w1.WeeklyGrade,
            s.Week2Grade = w2.WeeklyGrade,
            s.Week3Grade = w3.WeeklyGrade,
            s.Week4Grade = w4.WeeklyGrade,
            s.Week5Grade = w5.WeeklyGrade
        FROM stage.Students s
            INNER JOIN stage.Courses c
                ON s.AdClassSchedId = c.AdClassSchedId
            LEFT JOIN cteWeeklyGrades w1
                ON s.StudentPrimaryKey = w1.StudentPrimaryKey
                   AND c.CoursePrimaryKey = w1.CoursePrimaryKey
                   AND w1.WeekNumber = 1
            LEFT JOIN cteWeeklyGrades w2
                ON s.StudentPrimaryKey = w2.StudentPrimaryKey
                   AND c.CoursePrimaryKey = w2.CoursePrimaryKey
                   AND w2.WeekNumber = 2
            LEFT JOIN cteWeeklyGrades w3
                ON s.StudentPrimaryKey = w3.StudentPrimaryKey
                   AND c.CoursePrimaryKey = w3.CoursePrimaryKey
                   AND w3.WeekNumber = 3
            LEFT JOIN cteWeeklyGrades w4
                ON s.StudentPrimaryKey = w4.StudentPrimaryKey
                   AND c.CoursePrimaryKey = w4.CoursePrimaryKey
                   AND w4.WeekNumber = 4
            LEFT JOIN cteWeeklyGrades w5
                ON s.StudentPrimaryKey = w5.StudentPrimaryKey
                   AND c.CoursePrimaryKey = w5.CoursePrimaryKey
                   AND w5.WeekNumber = 5;

        EXEC LS_ODS.AddODSLoadLog 'Updated Student Weekly Grades', 0;

        --**************************************************************************************************************************************** 
        --Update the stage.Students table with the count of practice exercises, tests, and assignments 
        --**************************************************************************************************************************************** 
		WITH cteCounts AS (
			SELECT 
				cm.PK1 AS CoursePrimaryKey,
				cu.USERS_PK1 AS StudentPrimaryKey,
				SUM(CASE WHEN gm.TITLE LIKE '%Practice Exercise%' THEN 1 ELSE 0 END) AS PracticeExercisesCount,
				SUM(CASE WHEN gm.TITLE LIKE '%Test%' THEN 1 ELSE 0 END) AS TestsCount,
				SUM(CASE WHEN gm.TITLE LIKE '%Assignment%' THEN 1 ELSE 0 END) AS AssignmentsCount
			FROM dbo.ATTEMPT a
			INNER JOIN GRADEBOOK_GRADE gg ON a.GRADEBOOK_GRADE_PK1 = gg.PK1
			INNER JOIN GRADEBOOK_MAIN gm ON gg.GRADEBOOK_MAIN_PK1 = gm.PK1
			INNER JOIN COURSE_USERS cu ON gg.COURSE_USERS_PK1 = cu.PK1
			INNER JOIN COURSE_MAIN cm ON cu.CRSMAIN_PK1 = cm.PK1
			WHERE NOT EXISTS (
				SELECT 1
				FROM GRADEBOOK_MAIN gm_sub
				WHERE gm_sub.PK1 = gm.PK1
				AND gm_sub.TITLE LIKE '%IEHR%'
				AND gm_sub.COURSE_CONTENTS_PK1 IS NULL
			)
			GROUP BY cm.PK1, cu.USERS_PK1
		)
		UPDATE s
		SET 
			s.SelfTestsCount = co.PracticeExercisesCount,
			s.AssessmentsCount = co.TestsCount,
			s.AssignmentsCount = co.AssignmentsCount
		FROM stage.Students s
		INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
		INNER JOIN cteCounts co ON s.StudentPrimaryKey = co.StudentPrimaryKey
		AND c.CoursePrimaryKey = co.CoursePrimaryKey;

        EXEC LS_ODS.AddODSLoadLog 'Updated Student Counts Of Practice Exercises, Tests And Assignments', 0;
		
		DECLARE @CurrentDateTime DATETIME;
		SET @CurrentDateTime = GETDATE();

-- CTE to calculate discussion counts
		WITH cteCounts AS (
			SELECT 
				cm.CRSMAIN_PK1 AS CoursePrimaryKey,
				mm.USERS_PK1 AS StudentPrimaryKey,
				COUNT(mm.PK1) AS DiscussionsCount
			FROM MSG_MAIN mm
			INNER JOIN FORUM_MAIN fm ON mm.FORUMMAIN_PK1 = fm.PK1
			INNER JOIN CONFERENCE_MAIN cm ON fm.CONFMAIN_PK1 = cm.PK1
			GROUP BY cm.CRSMAIN_PK1, mm.USERS_PK1
		)
		-- Update Students table with discussion counts
		UPDATE s
		SET s.DiscussionsCount = co.DiscussionsCount
		FROM stage.Students s
		INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
		INNER JOIN cteCounts co ON s.StudentPrimaryKey = co.StudentPrimaryKey AND c.CoursePrimaryKey = co.CoursePrimaryKey;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Counts Of Discussion Posts', 0;

		-- Update stage.Courses with section start date, end date, and week number
		UPDATE c
		SET 
			c.SectionStart = cs.StartDate,
			c.SectionEnd = cs.EndDate,
			c.WeekNumber = CASE
				WHEN DATEDIFF(WEEK, cs.StartDate, @CurrentDateTime) + 1 >= 7 THEN 7
				ELSE DATEDIFF(WEEK, cs.StartDate, @CurrentDateTime) + 1
			END,
			c.DayNumber = CASE
				WHEN DATEDIFF(DAY, cs.StartDate, @CurrentDateTime) >= 49 THEN 49
				ELSE DATEDIFF(DAY, cs.StartDate, @CurrentDateTime)
			END
		FROM stage.Courses c
		INNER JOIN CV_Prod.dbo.AdClassSched cs ON c.AdClassSchedId = cs.AdClassSchedID;

		-- Update stage.Courses with Cengage values
		UPDATE co
		SET co.CengageCourseIndicator = 1
		FROM stage.Courses co
		INNER JOIN Cengage.CourseLookup cl ON co.CourseCode = cl.CourseCode
		AND co.SectionStart BETWEEN cl.StartDate AND cl.EndDate;

		-- Create a table to hold the holiday schedule
		DECLARE @Holidays TABLE (
			StartDate DATE,
			EndDate DATE,
			WeeksOff INT
		);

		-- Populate holiday schedule with Christmas Break Online values
		INSERT INTO @Holidays (StartDate, EndDate, WeeksOff)
		SELECT ca.StartDate, ca.EndDate, ((DATEDIFF(DAY, ca.StartDate, ca.EndDate) + 1) / 7) AS WeeksOff
		FROM CV_Prod.dbo.AdCalendar ca
		INNER JOIN CV_Prod.dbo.SyCampusList cl ON ca.SyCampusGrpID = cl.SyCampusGrpID
		WHERE cl.SyCampusID = 9 AND LEFT(ca.Code, 2) = 'CB'
		ORDER BY ca.StartDate DESC;

		-- Update stage.Courses to remove holiday weeks before further processing
		DECLARE @HolidayDateCheck DATETIME;
		SET @HolidayDateCheck = CAST(GETDATE() AS DATE);

		UPDATE co
		SET co.WeekNumber = co.WeekNumber - CASE
			WHEN @HolidayDateCheck < ho.StartDate THEN 0
			WHEN @HolidayDateCheck >= ho.StartDate AND @HolidayDateCheck <= ho.EndDate THEN ls_co.WeekNumber
			WHEN @HolidayDateCheck > ho.EndDate THEN ho.WeeksOff
			ELSE 0
		END
		FROM stage.Courses co
		INNER JOIN @Holidays ho ON co.SectionStart <= ho.StartDate AND co.SectionEnd >= ho.EndDate
		INNER JOIN LS_ODS.Courses ls_co ON co.CoursePrimaryKey = ls_co.CoursePrimaryKey
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7;

		EXEC LS_ODS.AddODSLoadLog 'Updated Course Start Dates And Week Numbers', 0;

		-- Update stage.Courses with week start dates and extension week start date
		UPDATE stage.Courses
		SET 
			Week1StartDate = SectionStart,
			Week2StartDate = DATEADD(DAY, 7, SectionStart),
			Week3StartDate = DATEADD(DAY, 14, SectionStart),
			Week4StartDate = DATEADD(DAY, 21, SectionStart),
			Week5StartDate = DATEADD(DAY, 28, SectionStart),
			ExtensionWeekStartDate = DATEADD(DAY, 35, SectionStart);

		-- Adjust week start dates for holidays
		UPDATE co
		SET co.Week5StartDate = DATEADD(WEEK, ISNULL(ho.WeeksOff, 0), co.Week5StartDate),
			co.ExtensionWeekStartDate = DATEADD(WEEK, ISNULL(ho.WeeksOff, 0), co.ExtensionWeekStartDate)
		FROM stage.Courses co
		LEFT JOIN @Holidays ho ON (co.Week1StartDate BETWEEN ho.StartDate AND ho.EndDate OR
								   co.Week2StartDate BETWEEN ho.StartDate AND ho.EndDate OR
								   co.Week3StartDate BETWEEN ho.StartDate AND ho.EndDate OR
								   co.Week4StartDate BETWEEN ho.StartDate AND ho.EndDate OR
								   co.Week5StartDate BETWEEN ho.StartDate AND ho.EndDate)
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7;

		UPDATE co
		SET co.Week4StartDate = DATEADD(WEEK, ISNULL(ho.WeeksOff, 0), co.Week4StartDate)
		FROM stage.Courses co
		LEFT JOIN @Holidays ho ON (co.Week1StartDate BETWEEN ho.StartDate AND ho.EndDate OR
								   co.Week2StartDate BETWEEN ho.StartDate AND ho.EndDate OR
								   co.Week3StartDate BETWEEN ho.StartDate AND ho.EndDate OR
								   co.Week4StartDate BETWEEN ho.StartDate AND ho.EndDate)
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7;

		UPDATE co
		SET co.Week3StartDate = DATEADD(WEEK, ISNULL(ho.WeeksOff, 0), co.Week3StartDate)
		FROM stage.Courses co
		LEFT JOIN @Holidays ho ON (co.Week1StartDate BETWEEN ho.StartDate AND ho.EndDate OR
								   co.Week2StartDate BETWEEN ho.StartDate AND ho.EndDate OR
								   co.Week3StartDate BETWEEN ho.StartDate AND ho.EndDate)
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7;

		UPDATE co
		SET co.Week2StartDate = DATEADD(WEEK, ISNULL(ho.WeeksOff, 0), co.Week2StartDate)
		FROM stage.Courses co
		LEFT JOIN @Holidays ho ON (co.Week1StartDate BETWEEN ho.StartDate AND ho.EndDate OR
								   co.Week2StartDate BETWEEN ho.StartDate AND ho.EndDate)
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7;

		UPDATE co
		SET co.Week1StartDate = DATEADD(WEEK, ISNULL(ho.WeeksOff, 0), co.Week1StartDate)
		FROM stage.Courses co
		LEFT JOIN @Holidays ho ON co.Week1StartDate BETWEEN ho.StartDate AND ho.EndDate
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7;

        --**************************************************************************************************************************************** 
        --Update the stage.Students table with the count of discussion posts 
        --**************************************************************************************************************************************** 
        EXEC LS_ODS.AddODSLoadLog 'Updated Course Week X Start Dates And Extension Week Start Date',0;

        --**************************************************************************************************************************************** 
        --Update the stage.Assignments table with the IsReportable and CountsAsSubmission values 
        --**************************************************************************************************************************************** 
       UPDATE stage.Assignments
		SET 
			IsReportable = CASE WHEN ad.IsReportable = 0 THEN 0 ELSE IsReportable END,
			CountsAsSubmission = CASE WHEN ad.CountsAsSubmission = 0 THEN 0 ELSE CountsAsSubmission END
		FROM stage.Assignments a
		INNER JOIN LS_ODS.AssignmentDetails ad ON a.AssignmentTitle = ad.AssignmentTitle;

        EXEC LS_ODS.AddODSLoadLog 'Updated Assignments IsReportable And CountsAsSubmission Flags',
                                  0;

        --**************************************************************************************************************************************** 
        --Update the stage.Courses table with the weekly assignment counts 
        --**************************************************************************************************************************************** 
        WITH cteCounts AS (
				SELECT
					a.CoursePrimaryKey,
					a.WeekNumber,
					COUNT(a.AssignmentPrimaryKey) AS AssignmentCount
				FROM
					stage.Assignments a
				WHERE
					a.WeekNumber <> 0  -- Filter out assignments that are not part of a week
					AND a.CountsAsSubmission = 1
				GROUP BY
					a.CoursePrimaryKey,
					a.WeekNumber
			)
			UPDATE stage.Courses
			SET
				Week1AssignmentCount = COALESCE(c1.AssignmentCount, 0),
				Week2AssignmentCount = COALESCE(c2.AssignmentCount, 0),
				Week3AssignmentCount = COALESCE(c3.AssignmentCount, 0),
				Week4AssignmentCount = COALESCE(c4.AssignmentCount, 0),
				Week5AssignmentCount = COALESCE(c5.AssignmentCount, 0)
			FROM
				stage.Courses c
				LEFT JOIN cteCounts c1 ON c.CoursePrimaryKey = c1.CoursePrimaryKey AND c1.WeekNumber = 1
				LEFT JOIN cteCounts c2 ON c.CoursePrimaryKey = c2.CoursePrimaryKey AND c2.WeekNumber = 2
				LEFT JOIN cteCounts c3 ON c.CoursePrimaryKey = c3.CoursePrimaryKey AND c3.WeekNumber = 3
				LEFT JOIN cteCounts c4 ON c.CoursePrimaryKey = c4.CoursePrimaryKey AND c4.WeekNumber = 4
				LEFT JOIN cteCounts c5 ON c.CoursePrimaryKey = c5.CoursePrimaryKey AND c5.WeekNumber = 5;

        EXEC LS_ODS.AddODSLoadLog 'Updated Course Weekly Assignment Counts', 0;

        --**************************************************************************************************************************************** 
        --Update the stage.Students table with the weekly completed assignment counts and submission rates 
        --**************************************************************************************************************************************** 
        -- Optimize the first UPDATE query
			WITH cteCounts AS (
				SELECT
					g.CourseUsersPrimaryKey,
					a.WeekNumber,
					COUNT(g.GradePrimaryKey) AS GradeCount
				FROM
					stage.Grades g
					INNER JOIN stage.Assignments a ON g.AssignmentPrimaryKey = a.AssignmentPrimaryKey
				WHERE
					(
						g.AssignmentStatus IN ('NEEDS GRADING', 'COMPLETED', 'IN MORE PROGRESS', 'NEEDS MORE GRADING')
						OR (a.AlternateTitle LIKE '%Disc%' AND g.AssignmentStatus = 'IN PROGRESS')
					)
					AND a.WeekNumber <> 0
					AND a.CountsAsSubmission = 1
				GROUP BY
					g.CourseUsersPrimaryKey,
					a.WeekNumber
			),
			cteCourseCounts AS (
				SELECT
					c.AdClassSchedId,
					c.Week1AssignmentCount,
					c.Week2AssignmentCount,
					c.Week3AssignmentCount,
					c.Week4AssignmentCount,
					c.Week5AssignmentCount
				FROM
					stage.Courses c
			)
			UPDATE s
			SET
				Week1CompletedAssignments = COALESCE(w1.GradeCount, 0),
				Week2CompletedAssignments = COALESCE(w2.GradeCount, 0),
				Week3CompletedAssignments = COALESCE(w3.GradeCount, 0),
				Week4CompletedAssignments = COALESCE(w4.GradeCount, 0),
				Week5CompletedAssignments = COALESCE(w5.GradeCount, 0),
				Week1CompletionRate = CAST(w1.GradeCount AS NUMERIC(12, 2)) / NULLIF(c.Week1AssignmentCount, 0),
				Week2CompletionRate = CAST(w2.GradeCount AS NUMERIC(12, 2)) / NULLIF(c.Week2AssignmentCount, 0),
				Week3CompletionRate = CAST(w3.GradeCount AS NUMERIC(12, 2)) / NULLIF(c.Week3AssignmentCount, 0),
				Week4CompletionRate = CAST(w4.GradeCount AS NUMERIC(12, 2)) / NULLIF(c.Week4AssignmentCount, 0),
				Week5CompletionRate = CAST(w5.GradeCount AS NUMERIC(12, 2)) / NULLIF(c.Week5AssignmentCount, 0),
				CoursePercentage = CAST((COALESCE(w1.GradeCount, 0) + COALESCE(w2.GradeCount, 0)
										+ COALESCE(w3.GradeCount, 0) + COALESCE(w4.GradeCount, 0)
										+ COALESCE(w5.GradeCount, 0)
									   ) AS NUMERIC(12, 2)) / NULLIF((c.Week1AssignmentCount + c.Week2AssignmentCount
																	+ c.Week3AssignmentCount + c.Week4AssignmentCount
																	+ c.Week5AssignmentCount
																   ), 0)
			FROM
				stage.Students s
				INNER JOIN cteCourseCounts c ON s.AdClassSchedId = c.AdClassSchedId
				LEFT JOIN cteCounts w1 ON s.CourseUsersPrimaryKey = w1.CourseUsersPrimaryKey AND w1.WeekNumber = 1
				LEFT JOIN cteCounts w2 ON s.CourseUsersPrimaryKey = w2.CourseUsersPrimaryKey AND w2.WeekNumber = 2
				LEFT JOIN cteCounts w3 ON s.CourseUsersPrimaryKey = w3.CourseUsersPrimaryKey AND w3.WeekNumber = 3
				LEFT JOIN cteCounts w4 ON s.CourseUsersPrimaryKey = w4.CourseUsersPrimaryKey AND w4.WeekNumber = 4
				LEFT JOIN cteCounts w5 ON s.CourseUsersPrimaryKey = w5.CourseUsersPrimaryKey AND w5.WeekNumber = 5;

			-- Optimize the second UPDATE query
			WITH cteTotalWork AS (
				SELECT
					s.SyStudentId,
					c.SectionStart,
					SUM(CAST(COALESCE(s.Week1CompletedAssignments, 0) + COALESCE(s.Week2CompletedAssignments, 0)
							 + COALESCE(s.Week3CompletedAssignments, 0) + COALESCE(s.Week4CompletedAssignments, 0)
							 + COALESCE(s.Week5CompletedAssignments, 0) AS NUMERIC(12, 2))
						) AS CompletedAssignments,
					SUM(CAST(c.Week1AssignmentCount + c.Week2AssignmentCount + c.Week3AssignmentCount
							 + c.Week4AssignmentCount + c.Week5AssignmentCount AS NUMERIC(12, 2))
						) AS TotalAssignments
				FROM
					stage.Students s
					INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId AND c.SectionStart IS NOT NULL
				GROUP BY
					s.SyStudentId,
					c.SectionStart
			)
			UPDATE s
			SET
				TotalWorkPercentage = tw.CompletedAssignments / NULLIF(tw.TotalAssignments, 0)
			FROM
				stage.Students s
				INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
				INNER JOIN cteTotalWork tw ON s.SyStudentId = tw.SyStudentId AND c.SectionStart = tw.SectionStart;

			-- Optimize the third UPDATE query
			UPDATE st
			SET
				Week1CompletionRateFixed = CASE WHEN co.DayNumber BETWEEN 0 AND 6 THEN st.Week1CompletionRate ELSE st1.Week1CompletionRateFixed END,
				Week2CompletionRateFixed = CASE WHEN co.DayNumber BETWEEN 7 AND 13 THEN st.Week2CompletionRate ELSE st1.Week2CompletionRateFixed END,
				Week3CompletionRateFixed = CASE WHEN co.DayNumber BETWEEN 14 AND 20 THEN st.Week3CompletionRate ELSE st1.Week3CompletionRateFixed END,
				Week4CompletionRateFixed = CASE WHEN co.DayNumber BETWEEN 21 AND 27 THEN st.Week4CompletionRate ELSE st1.Week4CompletionRateFixed END,
				Week5CompletionRateFixed = CASE WHEN co.DayNumber BETWEEN 28 AND 34 THEN st.Week5CompletionRate ELSE st1.Week5CompletionRateFixed END
			FROM
				stage.Students st
				INNER JOIN stage.Courses co ON st.AdClassSchedId = co.AdClassSchedId
				LEFT JOIN LS_ODS.Students st1 ON st.SyStudentId = st1.SyStudentId AND st.AdClassSchedId = st1.AdClassSchedId;



        EXEC LS_ODS.AddODSLoadLog 'Updated Student Counts Of Completed Assignments And Submission Rates',0;

        --**************************************************************************************************************************************** 
        --Update completion/submission rates by assignment type 
        --****************************************************************************************************************************************		
        EXEC LS_ODS.ProcessStudentRatesByAssignmentType;

        EXEC LS_ODS.AddODSLoadLog 'Updated Completion/Submission Rates By Assignment Type',
                                  0;

        --**************************************************************************************************************************************** 
        --Update the stage.Students table with the weekly LDAs 
        --**************************************************************************************************************************************** 
        --Get value from current table 
       -- Update weekly LDA values in a single operation
		WITH WeeklyLDAUpdates AS (
			SELECT
				s.SyStudentId,
				s.AdEnrollSchedId,
				MAX(CASE WHEN wl.WeekNumber = 1 THEN wl.LDA END) AS Week1LDA,
				MAX(CASE WHEN wl.WeekNumber = 2 THEN wl.LDA END) AS Week2LDA,
				MAX(CASE WHEN wl.WeekNumber = 3 THEN wl.LDA END) AS Week3LDA,
				MAX(CASE WHEN wl.WeekNumber = 4 THEN wl.LDA END) AS Week4LDA,
				MAX(CASE WHEN wl.WeekNumber = 5 THEN wl.LDA END) AS Week5LDA
			FROM
				stage.Students s
				INNER JOIN (
					SELECT
						es.SyStudentId,
						es.AdEnrollSchedID,
						c.WeekNumber,
						es.LDA
					FROM
						CV_PROD.dbo.AdEnrollSched es
						INNER JOIN stage.Courses c ON es.AdClassSchedID = c.AdClassSchedId
				) AS wl ON s.SyStudentId = wl.SyStudentId AND s.AdEnrollSchedId = wl.AdEnrollSchedId
			GROUP BY
				s.SyStudentId,
				s.AdEnrollSchedId
		)
		UPDATE s
		SET
			s.Week1LDA = COALESCE(w.Week1LDA, s.Week1LDA),
			s.Week2LDA = COALESCE(w.Week2LDA, s.Week2LDA),
			s.Week3LDA = COALESCE(w.Week3LDA, s.Week3LDA),
			s.Week4LDA = COALESCE(w.Week4LDA, s.Week4LDA),
			s.Week5LDA = COALESCE(w.Week5LDA, s.Week5LDA)
		FROM
			stage.Students s
			INNER JOIN WeeklyLDAUpdates w ON s.SyStudentId = w.SyStudentId AND s.AdEnrollSchedId = w.AdEnrollSchedId;


        EXEC LS_ODS.AddODSLoadLog 'Updated Student Weekly LDAs', 0;

        --**************************************************************************************************************************************** 
        --Update the stage.Grades table with the number of attempts 
        --**************************************************************************************************************************************** 
                -- Use a CTE to count attempts per GRADEBOOK_GRADE_PK1
        WITH cteCounts AS (
          SELECT
            a.GRADEBOOK_GRADE_PK1 AS GradePrimaryKey,
            COUNT(a.PK1) AS AttemptCount
          FROM
            ATTEMPT a
          GROUP BY
            a.GRADEBOOK_GRADE_PK1
        )

        -- Update Grades table with the attempt counts
        UPDATE g
        SET
          g.NumberOfAttempts = COALESCE(c.AttemptCount, 0) -- Handle cases where no attempts are counted
        FROM
          stage.Grades g
          LEFT JOIN cteCounts c ON g.GradePrimaryKey = c.GradePrimaryKey;


        EXEC LS_ODS.AddODSLoadLog 'Updated Grade Counts Of Attempts', 0;

        --**************************************************************************************************************************************** 
        --Add new records to the TimeInModule table 
        --**************************************************************************************************************************************** 
        -- Use a CTE to insert data into LS_ODS.TimeInModule
		INSERT INTO LS_ODS.TimeInModule (
		  ScormRegistrationId,
		  LaunchHistoryId,
		  BlackboardUsername,
		  UserPrimaryKey,
		  SyStudentId,
		  CourseUsersPrimaryKey,
		  CoursePrimaryKey,
		  AssignmentPrimaryKey,
		  StartDateTime,
		  EndDateTime,
		  ElapsedTimeMinutes,
		  CompletionStatus,
		  SatisfactionStatus,
		  ScormRegistrationLaunchHistoryStartDateTimeKey
		)
		SELECT
		  sr.SCORM_REGISTRATION_ID AS ScormRegistrationId,
		  slh.LAUNCH_HISTORY_ID AS LaunchHistoryId,
		  sr.GLOBAL_OBJECTIVE_SCOPE AS BlackboardUsername,
		  u.PK1 AS UserPrimaryKey,
		  REPLACE(u.BATCH_UID, 'SyStudent_', '') AS SyStudentId,
		  cu.PK1 AS CourseUsersPrimaryKey,
		  cm.PK1 AS CoursePrimaryKey,
		  cc.PK1 AS AssignmentPrimaryKey,
		  slh.LAUNCH_TIME AS StartDateTime,
		  slh.EXIT_TIME AS EndDateTime,
		  DATEDIFF(MINUTE, slh.LAUNCH_TIME, slh.EXIT_TIME) AS ElapsedTimeMinutes,
		  slh.COMPLETION AS CompletionStatus,
		  slh.SATISFACTION AS SatisfactionStatus,
		  sr.SCORM_REGISTRATION_ID + '_' + slh.LAUNCH_HISTORY_ID + '_' + CONVERT(VARCHAR(50), slh.LAUNCH_TIME, 126) AS ScormRegistrationLaunchHistoryStartDateTimeKey
		FROM
		  dbo.SCORMLAUNCHHISTORY slh
		  INNER JOIN dbo.SCORMREGISTRATION sr ON slh.SCORM_REGISTRATION_ID = sr.SCORM_REGISTRATION_ID
		  INNER JOIN dbo.USERS u ON sr.GLOBAL_OBJECTIVE_SCOPE = u.[USER_ID] AND LEFT(u.BATCH_UID, 10) = 'SyStudent_'
		  INNER JOIN dbo.COURSE_CONTENTS cc ON REPLACE(REPLACE(sr.CONTENT_ID, '_1', ''), '_', '') = cc.PK1
		  INNER JOIN dbo.COURSE_MAIN cm ON cc.CRSMAIN_PK1 = cm.PK1
		  INNER JOIN dbo.COURSE_USERS cu ON u.PK1 = cu.USERS_PK1 AND cm.PK1 = cu.CRSMAIN_PK1
		  LEFT JOIN dbo.DATA_SOURCE ds ON ds.PK1 = cu.DATA_SRC_PK1 -- Adding to handle erroneous DSKs
		WHERE
		  sr.SCORM_REGISTRATION_ID + '_' + slh.LAUNCH_HISTORY_ID + '_' + CONVERT(VARCHAR(50), slh.LAUNCH_TIME, 126) NOT IN (
			SELECT tim.ScormRegistrationLaunchHistoryStartDateTimeKey FROM LS_ODS.TimeInModule tim
		  )
		  AND ds.batch_uid NOT IN ('ENR_181008_02.txt', 'ENR_181008', 'ENR_181008_1558036.txt'); -- Adding to handle erroneous DSKs


        EXEC LS_ODS.AddODSLoadLog 'Updated Time In Module Table', 0;

        --**************************************************************************************************************************************** 
        --Update the stage.Students table with the Current Course Grade 
        --**************************************************************************************************************************************** 
        DECLARE @TodayDayNumber INT;
		SET @TodayDayNumber = DATEPART(WEEKDAY, GETDATE());

		WITH cteCurrentCourseGrade AS (
			SELECT 
				s.SyStudentId,
				s.AdClassSchedId,
				CASE
					WHEN s.Week1Grade IS NULL
						 AND s.Week2Grade IS NULL
						 AND s.Week3Grade IS NULL
						 AND s.Week4Grade IS NULL
						 AND s.Week5Grade IS NULL THEN NULL
					WHEN c.WeekNumber = 1 THEN 1.0
					WHEN c.WeekNumber = 2 THEN
						CASE WHEN @TodayDayNumber < 5 THEN 1.0 ELSE s.Week1Grade END
					WHEN c.WeekNumber = 3 THEN
						CASE WHEN @TodayDayNumber < 5 THEN s.Week1Grade ELSE s.Week2Grade END
					WHEN c.WeekNumber = 4 THEN
						CASE WHEN @TodayDayNumber < 5 THEN s.Week2Grade ELSE s.Week3Grade END
					WHEN c.WeekNumber = 5 THEN
						CASE WHEN @TodayDayNumber < 5 THEN s.Week3Grade ELSE s.Week4Grade END
					WHEN c.WeekNumber = 6 THEN
						CASE WHEN @TodayDayNumber < 5 THEN s.Week4Grade ELSE s.Week5Grade END
					ELSE s.Week5Grade
				END AS CurrentCourseGrade
			FROM 
				stage.Students s
				INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
		)
		UPDATE s
		SET 
			s.CurrentCourseGrade = ccg.CurrentCourseGrade
		FROM 
			stage.Students s
			INNER JOIN cteCurrentCourseGrade ccg ON s.SyStudentId = ccg.SyStudentId AND s.AdClassSchedId = ccg.AdClassSchedId;

        EXEC LS_ODS.AddODSLoadLog 'Updated Student Current Course Grade', 0;


        --**************************************************************************************************************************************** 
        --Update the stage.Students table with the Current Posted Grade 
        --**************************************************************************************************************************************** 
        DECLARE @PostedGradesCollectorPath VARCHAR(500);
        SET @PostedGradesCollectorPath = '"C:\Program Files\PostedGradesImporter\PostedGradeODSLoader.exe"';
        EXEC master..xp_cmdshell @PostedGradesCollectorPath;

        EXEC LS_ODS.AddODSLoadLog 'Updated Student Posted Grades', 0;

        --**************************************************************************************************************************************** 
        --Update Student records that have changed 
        --**************************************************************************************************************************************** 
        --Create Table Variable To Hold Changed Student Records 
        -- CTE to find changed students
		WITH ChangedStudents AS (
		SELECT
				new.StudentPrimaryKey,
				new.CourseUsersPrimaryKey
		FROM
				stage.Students new
				INNER JOIN LS_ODS.Students old ON
						new.StudentPrimaryKey = old.StudentPrimaryKey
						AND new.CourseUsersPrimaryKey = old.CourseUsersPrimaryKey
		WHERE
				old.ActiveFlag = 1
				AND (
						new.DateTimeCreated <> old.DateTimeCreated
						OR new.DateTimeModified <> old.DateTimeModified
						OR new.RowStatus <> old.RowStatus
						OR new.BatchUniqueIdentifier <> old.BatchUniqueIdentifier
						OR new.BlackboardUsername <> old.BlackboardUsername
						OR new.SyStudentId <> old.SyStudentId
						OR new.FirstName <> old.FirstName
						OR new.LastName <> old.LastName
						OR new.Campus <> old.Campus
						OR new.AdEnrollSchedId <> old.AdEnrollSchedId
						OR new.AdClassSchedId <> old.AdClassSchedId
						OR new.LastLoginDateTime <> old.LastLoginDateTime
						OR new.TimeInClass <> old.TimeInClass
						OR new.LastI3InteractionNumberMainPhone <> old.LastI3InteractionNumberMainPhone
						OR new.LastI3InteractionDateTimeMainPhone <> old.LastI3InteractionDateTimeMainPhone
						OR new.DaysSinceLastI3InteractionMainPhone <> old.DaysSinceLastI3InteractionMainPhone
						OR new.LastI3InteractionNumberWorkPhone <> old.LastI3InteractionNumberWorkPhone
						OR new.LastI3InteractionDateTimeWorkPhone <> old.LastI3InteractionDateTimeWorkPhone
						OR new.DaysSinceLastI3InteractionWorkPhone <> old.DaysSinceLastI3InteractionWorkPhone
						OR new.LastI3InteractionNumberMobilePhone <> old.LastI3InteractionNumberMobilePhone
						OR new.LastI3InteractionDateTimeMobilePhone <> old.LastI3InteractionDateTimeMobilePhone
						OR new.DaysSinceLastI3InteractionMobilePhone <> old.DaysSinceLastI3InteractionMobilePhone
						OR new.LastI3InteractionNumberOtherPhone <> old.LastI3InteractionNumberOtherPhone
						OR new.LastI3InteractionDateTimeOtherPhone <> old.LastI3InteractionDateTimeOtherPhone
						OR new.DaysSinceLastI3InteractionOtherPhone <> old.DaysSinceLastI3InteractionOtherPhone
						OR new.Week1Grade <> old.Week1Grade
						OR new.Week2Grade <> old.Week2Grade
						OR new.Week3Grade <> old.Week3Grade
						OR new.Week4Grade <> old.Week4Grade
						OR new.Week5Grade <> old.Week5Grade
						OR new.SelfTestsCount <> old.SelfTestsCount
						OR new.AssessmentsCount <> old.AssessmentsCount
						OR new.AssignmentsCount <> old.AssignmentsCount
						OR new.DiscussionsCount <> old.DiscussionsCount
						OR new.Week1CompletionRate > old.Week1CompletionRate
						OR new.Week2CompletionRate > old.Week2CompletionRate
						OR new.Week3CompletionRate > old.Week3CompletionRate
						OR new.Week4CompletionRate > old.Week4CompletionRate
						OR new.Week5CompletionRate > old.Week5CompletionRate
						OR new.VAStudent <> old.VAStudent
						OR new.NoticeName <> old.NoticeName
						OR new.NoticeDueDate <> old.NoticeDueDate
						OR new.VABenefitName <> old.VABenefitName
						OR new.ClassStatus <> old.ClassStatus
						OR new.Week1LDA <> old.Week1LDA
						OR new.Week2LDA <> old.Week2LDA
						OR new.Week3LDA <> old.Week3LDA
						OR new.Week4LDA <> old.Week4LDA
						OR new.Week5LDA <> old.Week5LDA
						OR new.Week1CompletedAssignments <> old.Week1CompletedAssignments
						OR new.Week2CompletedAssignments <> old.Week2CompletedAssignments
						OR new.Week3CompletedAssignments <> old.Week3CompletedAssignments
						OR new.Week4CompletedAssignments <> old.Week4CompletedAssignments
						OR new.Week5CompletedAssignments <> old.Week5CompletedAssignments
						OR new.CoursePercentage <> old.CoursePercentage
						OR new.TotalWorkPercentage <> old.TotalWorkPercentage
						OR new.AdEnrollId <> old.AdEnrollId
						OR new.IsRetake <> old.IsRetake
						OR new.StudentCourseUserKeys <> old.StudentCourseUserKeys
						OR new.CurrentCourseGrade <> old.CurrentCourseGrade
						OR new.ProgramCode <> old.ProgramCode
						OR new.ProgramName <> old.ProgramName
						OR new.ProgramVersionCode <> old.ProgramVersionCode
						OR new.ProgramVersionName <> old.ProgramVersionName
						OR new.MondayTimeInClass <> old.MondayTimeInClass
						OR new.TuesdayTimeInClass <> old.TuesdayTimeInClass
						OR new.WednesdayTimeInClass <> old.WednesdayTimeInClass
						OR new.ThursdayTimeInClass <> old.ThursdayTimeInClass
						OR new.FridayTimeInClass <> old.FridayTimeInClass
						OR new.SaturdayTimeInClass <> old.SaturdayTimeInClass
						OR new.SundayTimeInClass <> old.SundayTimeInClass
						OR new.Week1CompletionRateFixed <> old.Week1CompletionRateFixed
						OR new.Week2CompletionRateFixed <> old.Week2CompletionRateFixed
						OR new.Week3CompletionRateFixed <> old.Week3CompletionRateFixed
						OR new.Week4CompletionRateFixed <> old.Week4CompletionRateFixed
						OR new.Week5CompletionRateFixed <> old.Week5CompletionRateFixed
						OR new.StudentNumber <> old.StudentNumber
						OR (new.DateTimeCreated IS NOT NULL AND old.DateTimeCreated IS NULL)
						OR (new.DateTimeModified IS NOT NULL AND old.DateTimeModified IS NULL)
						OR (new.RowStatus IS NOT NULL AND old.RowStatus IS NULL)
						OR (new.BatchUniqueIdentifier IS NOT NULL AND old.BatchUniqueIdentifier IS NULL)
						OR (new.BlackboardUsername IS NOT NULL AND old.BlackboardUsername IS NULL)
						OR (new.SyStudentId IS NOT NULL AND old.SyStudentId IS NULL)
						OR (new.FirstName IS NOT NULL AND old.FirstName IS NULL)
						OR (new.LastName IS NOT NULL AND old.LastName IS NULL)
						OR (new.Campus IS NOT NULL AND old.Campus IS NULL)
						OR (new.AdEnrollSchedId IS NOT NULL AND old.AdEnrollSchedId IS NULL)
						OR (new.AdClassSchedId IS NOT NULL AND old.AdClassSchedId IS NULL)
						OR (new.LastLoginDateTime IS NOT NULL AND old.LastLoginDateTime IS NULL)
						OR (new.CourseUsersPrimaryKey IS NOT NULL AND old.CourseUsersPrimaryKey IS NULL)
						OR (new.TimeInClass IS NOT NULL AND old.TimeInClass IS NULL)
						OR (new.LastI3InteractionNumberMainPhone IS NOT NULL AND old.LastI3InteractionNumberMainPhone IS NULL)
						OR (new.LastI3InteractionDateTimeMainPhone IS NOT NULL AND old.LastI3InteractionDateTimeMainPhone IS NULL)
						OR (new.DaysSinceLastI3InteractionMainPhone IS NOT NULL AND old.DaysSinceLastI3InteractionMainPhone IS NULL)
						OR (new.LastI3InteractionNumberWorkPhone IS NOT NULL AND old.LastI3InteractionNumberWorkPhone IS NULL)
						OR (new.LastI3InteractionDateTimeWorkPhone IS NOT NULL AND old.LastI3InteractionDateTimeWorkPhone IS NULL)
						OR (new.DaysSinceLastI3InteractionWorkPhone IS NOT NULL AND old.DaysSinceLastI3InteractionWorkPhone IS NULL)
						OR (new.LastI3InteractionNumberMobilePhone IS NOT NULL AND old.LastI3InteractionNumberMobilePhone IS NULL)
						OR (new.LastI3InteractionDateTimeMobilePhone IS NOT NULL AND old.LastI3InteractionDateTimeMobilePhone IS NULL)
						OR (new.DaysSinceLastI3InteractionMobilePhone IS NOT NULL AND old.DaysSinceLastI3InteractionMobilePhone IS NULL)
						OR (new.LastI3InteractionNumberOtherPhone IS NOT NULL AND old.LastI3InteractionNumberOtherPhone IS NULL)
						OR (new.LastI3InteractionDateTimeOtherPhone IS NOT NULL AND old.LastI3InteractionDateTimeOtherPhone IS NULL)
						OR (new.DaysSinceLastI3InteractionOtherPhone IS NOT NULL AND old.DaysSinceLastI3InteractionOtherPhone IS NULL)
						OR (new.Week1Grade IS NOT NULL AND old.Week1Grade IS NULL)
						OR (new.Week2Grade IS NOT NULL AND old.Week2Grade IS NULL)
						OR (new.Week3Grade IS NOT NULL AND old.Week3Grade IS NULL)
						OR (new.Week4Grade IS NOT NULL AND old.Week4Grade IS NULL)
						OR (new.Week5Grade IS NOT NULL AND old.Week5Grade IS NULL)
						OR (new.SelfTestsCount IS NOT NULL AND old.SelfTestsCount IS NULL)
						OR (new.AssessmentsCount IS NOT NULL AND old.AssessmentsCount IS NULL)
						OR (new.AssignmentsCount IS NOT NULL AND old.AssignmentsCount IS NULL)
						OR (new.DiscussionsCount IS NOT NULL AND old.DiscussionsCount IS NULL)
						OR (new.Week1CompletionRate IS NOT NULL AND old.Week1CompletionRate IS NULL)
						OR (new.Week2CompletionRate IS NOT NULL AND old.Week2CompletionRate IS NULL)
						OR (new.Week3CompletionRate IS NOT NULL AND old.Week3CompletionRate IS NULL)
						OR (new.Week4CompletionRate IS NOT NULL AND old.Week4CompletionRate IS NULL)
						OR (new.Week5CompletionRate IS NOT NULL AND old.Week5CompletionRate IS NULL)
						OR (new.VAStudent IS NOT NULL AND old.VAStudent IS NULL)
						OR (new.NoticeName IS NOT NULL AND old.NoticeName IS NULL)
						OR (new.NoticeDueDate IS NOT NULL AND old.NoticeDueDate IS NULL)
						OR (new.VABenefitName IS NOT NULL AND old.VABenefitName IS NULL)
						OR (new.ClassStatus IS NOT NULL AND old.ClassStatus IS NULL)
						OR (new.Week1LDA IS NOT NULL AND old.Week1LDA IS NULL)
						OR (new.Week2LDA IS NOT NULL AND old.Week2LDA IS NULL)
						OR (new.Week3LDA IS NOT NULL AND old.Week3LDA IS NULL)
						OR (new.Week4LDA IS NOT NULL AND old.Week4LDA IS NULL)
						OR (new.Week5LDA IS NOT NULL AND old.Week5LDA IS NULL)
						OR (new.Week1CompletedAssignments IS NOT NULL AND old.Week1CompletedAssignments IS NULL)
						OR (new.Week2CompletedAssignments IS NOT NULL AND old.Week2CompletedAssignments IS NULL)
						OR (new.Week3CompletedAssignments IS NOT NULL AND old.Week3CompletedAssignments IS NULL)
						OR (new.Week4CompletedAssignments IS NOT NULL AND old.Week4CompletedAssignments IS NULL)
						OR (new.Week5CompletedAssignments IS NOT NULL AND old.Week5CompletedAssignments IS NULL)
						OR (new.CoursePercentage IS NOT NULL AND old.CoursePercentage IS NULL)
						OR (new.TotalWorkPercentage IS NOT NULL AND old.TotalWorkPercentage IS NULL)
						OR (new.AdEnrollId IS NOT NULL AND old.AdEnrollId IS NULL)
						OR (new.IsRetake IS NOT NULL AND old.IsRetake IS NULL)
						OR (new.StudentCourseUserKeys IS NOT NULL AND old.StudentCourseUserKeys IS NULL)
						OR (new.CurrentCourseGrade IS NOT NULL AND old.CurrentCourseGrade IS NULL)
						OR (new.ProgramCode IS NOT NULL AND old.ProgramCode IS NULL)
						OR (new.ProgramName IS NOT NULL AND old.ProgramName IS NULL)
						OR (new.ProgramVersionCode IS NOT NULL AND old.ProgramVersionCode IS NULL)
						OR (new.ProgramVersionName IS NOT NULL AND old.ProgramVersionName IS NULL)
						OR (new.MondayTimeInClass IS NOT NULL AND old.MondayTimeInClass IS NULL)
						OR (new.TuesdayTimeInClass IS NOT NULL AND old.TuesdayTimeInClass IS NULL)
						OR (new.WednesdayTimeInClass IS NOT NULL AND old.WednesdayTimeInClass IS NULL)
						OR (new.ThursdayTimeInClass IS NOT NULL AND old.ThursdayTimeInClass IS NULL)
						OR (new.FridayTimeInClass IS NOT NULL AND old.FridayTimeInClass IS NULL)
						OR (new.SaturdayTimeInClass IS NOT NULL AND old.SaturdayTimeInClass IS NULL)
						OR (new.SundayTimeInClass IS NOT NULL AND old.SundayTimeInClass IS NULL)
						OR (new.Week1CompletionRateFixed IS NOT NULL AND old.Week1CompletionRateFixed IS NULL)
						OR (new.Week2CompletionRateFixed IS NOT NULL AND old.Week2CompletionRateFixed IS NULL)
						OR (new.Week3CompletionRateFixed IS NOT NULL AND old.Week3CompletionRateFixed IS NULL)
						OR (new.Week4CompletionRateFixed IS NOT NULL AND old.Week4CompletionRateFixed IS NULL)
						OR (new.Week5CompletionRateFixed IS NOT NULL AND old.Week5CompletionRateFixed IS NULL)
						OR (new.StudentNumber IS NOT NULL AND old.StudentNumber IS NULL)
				)
)
-- Update LS_ODS.Students to inactive where records are in ChangedStudents
			UPDATE LS_ODS.Students
			SET ActiveFlag = 0
			WHERE EXISTS (
					SELECT 1
					FROM ChangedStudents cs
					WHERE LS_ODS.Students.StudentPrimaryKey = cs.StudentPrimaryKey
							AND LS_ODS.Students.CourseUsersPrimaryKey = cs.CourseUsersPrimaryKey
			);

			-- Insert new and updated records from stage.Students into LS_ODS.Students
			INSERT INTO LS_ODS.Students (
					StudentPrimaryKey,
					CourseUsersPrimaryKey,
					DateTimeCreated,
					DateTimeModified,
					RowStatus,
					BatchUniqueIdentifier,
					BlackboardUsername,
					SyStudentId,
					FirstName,
					LastName,
					Campus,
					AdEnrollSchedId,
					AdClassSchedId,
					LastLoginDateTime,
					TimeInClass,
					LastI3InteractionNumberMainPhone,
					LastI3InteractionDateTimeMainPhone,
					DaysSinceLastI3InteractionMainPhone,
					LastI3InteractionNumberWorkPhone,
					LastI3InteractionDateTimeWorkPhone,
					DaysSinceLastI3InteractionWorkPhone,
					LastI3InteractionNumberMobilePhone,
					LastI3InteractionDateTimeMobilePhone,
					DaysSinceLastI3InteractionMobilePhone,
					LastI3InteractionNumberOtherPhone,
					LastI3InteractionDateTimeOtherPhone,
					DaysSinceLastI3InteractionOtherPhone,
					Week1Grade,
					Week2Grade,
					Week3Grade,
					Week4Grade,
					Week5Grade,
					SelfTestsCount,
					AssessmentsCount,
					AssignmentsCount,
					DiscussionsCount,
					Week1CompletionRate,
					Week2CompletionRate,
					Week3CompletionRate,
					Week4CompletionRate,
					Week5CompletionRate,
					VAStudent,
					NoticeName,
					NoticeDueDate,
					VABenefitName,
					ClassStatus,
					Week1LDA,
					Week2LDA,
					Week3LDA,
					Week4LDA,
					Week5LDA,
					Week1CompletedAssignments,
					Week2CompletedAssignments,
					Week3CompletedAssignments,
					Week4CompletedAssignments,
					Week5CompletedAssignments,
					CoursePercentage,
					TotalWorkPercentage,
					AdEnrollId,
					IsRetake,
					StudentCourseUserKeys,
					CurrentCourseGrade,
					ProgramCode,
					ProgramName,
					ProgramVersionCode,
					ProgramVersionName,
					MondayTimeInClass,
					TuesdayTimeInClass,
					WednesdayTimeInClass,
					ThursdayTimeInClass,
					FridayTimeInClass,
					SaturdayTimeInClass,
					SundayTimeInClass,
					Week1CompletionRateFixed,
					Week2CompletionRateFixed,
					Week3CompletionRateFixed,
					Week4CompletionRateFixed,
					Week5CompletionRateFixed,
					StudentNumber
			)
			SELECT
					StudentPrimaryKey,
					CourseUsersPrimaryKey,
					DateTimeCreated,
					DateTimeModified,
					RowStatus,
					BatchUniqueIdentifier,
					BlackboardUsername,
					SyStudentId,
					FirstName,
					LastName,
					Campus,
					AdEnrollSchedId,
					AdClassSchedId,
					LastLoginDateTime,
					TimeInClass,
					LastI3InteractionNumberMainPhone,
					LastI3InteractionDateTimeMainPhone,
					DaysSinceLastI3InteractionMainPhone,
					LastI3InteractionNumberWorkPhone,
					LastI3InteractionDateTimeWorkPhone,
					DaysSinceLastI3InteractionWorkPhone,
					LastI3InteractionNumberMobilePhone,
					LastI3InteractionDateTimeMobilePhone,
					DaysSinceLastI3InteractionMobilePhone,
					LastI3InteractionNumberOtherPhone,
					LastI3InteractionDateTimeOtherPhone,
					DaysSinceLastI3InteractionOtherPhone,
					Week1Grade,
					Week2Grade,
					Week3Grade,
					Week4Grade,
					Week5Grade,
					SelfTestsCount,
					AssessmentsCount,
					AssignmentsCount,
					DiscussionsCount,
					Week1CompletionRate,
					Week2CompletionRate,
					Week3CompletionRate,
					Week4CompletionRate,
					Week5CompletionRate,
					VAStudent,
					NoticeName,
					NoticeDueDate,
					VABenefitName,
					ClassStatus,
					Week1LDA,
					Week2LDA,
					Week3LDA,
					Week4LDA,
					Week5LDA,
					Week1CompletedAssignments,
					Week2CompletedAssignments,
					Week3CompletedAssignments,
					Week4CompletedAssignments,
					Week5CompletedAssignments,
					CoursePercentage,
					TotalWorkPercentage,
					AdEnrollId,
					IsRetake,
					StudentCourseUserKeys,
					CurrentCourseGrade,
					ProgramCode,
					ProgramName,
					ProgramVersionCode,
					ProgramVersionName,
					MondayTimeInClass,
					TuesdayTimeInClass,
					WednesdayTimeInClass,
					ThursdayTimeInClass,
					FridayTimeInClass,
					SaturdayTimeInClass,
					SundayTimeInClass,
					Week1CompletionRateFixed,
					Week2CompletionRateFixed,
					Week3CompletionRateFixed,
					Week4CompletionRateFixed,
					Week5CompletionRateFixed,
					StudentNumber
			FROM stage.Students s
			WHERE NOT EXISTS (
					SELECT 1
					FROM ChangedStudents cs
					WHERE s.StudentPrimaryKey = cs.StudentPrimaryKey
							AND s.CourseUsersPrimaryKey = cs.CourseUsersPrimaryKey
			);


        EXEC LS_ODS.AddODSLoadLog 'Updated Students Records That Have Changed', 0;

        --**************************************************************************************************************************************** 
        --Update Course records that have changed 
        --**************************************************************************************************************************************** 
        --Create Table Variable To Hold Changed Course Records 
        -- Use OPTION (RECOMPILE) hint to optimize execution plan
			DECLARE @ChangedCourses TABLE (CoursePrimaryKey INT);

			-- Find Changed Courses And Populate Table Variable 
			INSERT INTO @ChangedCourses (CoursePrimaryKey)
			SELECT new.CoursePrimaryKey
			FROM stage.Courses new WITH (FORCESEEK) -- Force index seek where possible
			INNER JOIN LS_ODS.Courses old WITH (INDEX(ix_Courses_ActiveFlag_CoursePrimaryKey)) -- Use specific index
				ON new.CoursePrimaryKey = old.CoursePrimaryKey
			   AND old.ActiveFlag = 1
			WHERE
				(   -- Use EXISTS for efficient comparison
					EXISTS (SELECT new.DateTimeCreated, old.DateTimeCreated WHERE new.DateTimeCreated <> old.DateTimeCreated)
				 OR EXISTS (SELECT new.DateTimeModified, old.DateTimeModified WHERE new.DateTimeModified <> old.DateTimeModified)
				 OR EXISTS (SELECT new.RowStatus, old.RowStatus WHERE new.RowStatus <> old.RowStatus)
				 OR EXISTS (SELECT new.BatchUniqueIdentifier, old.BatchUniqueIdentifier WHERE new.BatchUniqueIdentifier <> old.BatchUniqueIdentifier)
				 OR EXISTS (SELECT new.CourseCode, old.CourseCode WHERE new.CourseCode <> old.CourseCode)
				 OR EXISTS (SELECT new.CourseName, old.CourseName WHERE new.CourseName <> old.CourseName)
				 OR EXISTS (SELECT new.SectionNumber, old.SectionNumber WHERE new.SectionNumber <> old.SectionNumber)
				 OR EXISTS (SELECT new.SectionStart, old.SectionStart WHERE new.SectionStart <> old.SectionStart)
				 OR EXISTS (SELECT new.SectionEnd, old.SectionEnd WHERE new.SectionEnd <> old.SectionEnd)
				 OR EXISTS (SELECT new.AdClassSchedId, old.AdClassSchedId WHERE new.AdClassSchedId <> old.AdClassSchedId)
				 OR EXISTS (SELECT new.WeekNumber, old.WeekNumber WHERE new.WeekNumber <> old.WeekNumber)
				 OR EXISTS (SELECT new.Week1AssignmentCount, old.Week1AssignmentCount WHERE new.Week1AssignmentCount <> old.Week1AssignmentCount)
				 OR EXISTS (SELECT new.Week2AssignmentCount, old.Week2AssignmentCount WHERE new.Week2AssignmentCount <> old.Week2AssignmentCount)
				 OR EXISTS (SELECT new.Week3AssignmentCount, old.Week3AssignmentCount WHERE new.Week3AssignmentCount <> old.Week3AssignmentCount)
				 OR EXISTS (SELECT new.Week4AssignmentCount, old.Week4AssignmentCount WHERE new.Week4AssignmentCount <> old.Week4AssignmentCount)
				 OR EXISTS (SELECT new.Week5AssignmentCount, old.Week5AssignmentCount WHERE new.Week5AssignmentCount <> old.Week5AssignmentCount)
				 OR EXISTS (SELECT new.PrimaryInstructor, old.PrimaryInstructor WHERE new.PrimaryInstructor <> old.PrimaryInstructor)
				 OR EXISTS (SELECT new.SecondaryInstructor, old.SecondaryInstructor WHERE new.SecondaryInstructor <> old.SecondaryInstructor)
				 OR EXISTS (SELECT new.Week1StartDate, old.Week1StartDate WHERE new.Week1StartDate <> old.Week1StartDate)
				 OR EXISTS (SELECT new.Week2StartDate, old.Week2StartDate WHERE new.Week2StartDate <> old.Week2StartDate)
				 OR EXISTS (SELECT new.Week3StartDate, old.Week3StartDate WHERE new.Week3StartDate <> old.Week3StartDate)
				 OR EXISTS (SELECT new.Week4StartDate, old.Week4StartDate WHERE new.Week4StartDate <> old.Week4StartDate)
				 OR EXISTS (SELECT new.Week5StartDate, old.Week5StartDate WHERE new.Week5StartDate <> old.Week5StartDate)
				 OR EXISTS (SELECT new.IsOrganization, old.IsOrganization WHERE new.IsOrganization <> old.IsOrganization)
				 OR EXISTS (SELECT new.ExtensionWeekStartDate, old.ExtensionWeekStartDate WHERE new.ExtensionWeekStartDate <> old.ExtensionWeekStartDate)
				 OR EXISTS (SELECT new.AcademicFacilitator, old.AcademicFacilitator WHERE new.AcademicFacilitator <> old.AcademicFacilitator)
				 OR EXISTS (SELECT new.PrimaryInstructorId, old.PrimaryInstructorId WHERE new.PrimaryInstructorId <> old.PrimaryInstructorId)
				 OR EXISTS (SELECT new.SecondaryInstructorId, old.SecondaryInstructorId WHERE new.SecondaryInstructorId <> old.SecondaryInstructorId)
				 OR EXISTS (SELECT new.AcademicFacilitatorId, old.AcademicFacilitatorId WHERE new.AcademicFacilitatorId <> old.AcademicFacilitatorId)
				 OR EXISTS (SELECT new.DayNumber, old.DayNumber WHERE new.DayNumber <> old.DayNumber)
				 OR EXISTS (SELECT new.CengageCourseIndicator, old.CengageCourseIndicator WHERE new.CengageCourseIndicator <> old.CengageCourseIndicator)
				 OR (new.DateTimeCreated IS NOT NULL AND old.DateTimeCreated IS NULL)
				 OR (new.DateTimeModified IS NOT NULL AND old.DateTimeModified IS NULL)
				 OR (new.RowStatus IS NOT NULL AND old.RowStatus IS NULL)
				 OR (new.BatchUniqueIdentifier IS NOT NULL AND old.BatchUniqueIdentifier IS NULL)
				 OR (new.CourseCode IS NOT NULL AND old.CourseCode IS NULL)
				 OR (new.CourseName IS NOT NULL AND old.CourseName IS NULL)
				 OR (new.SectionNumber IS NOT NULL AND old.SectionNumber IS NULL)
				 OR (new.SectionStart IS NOT NULL AND old.SectionStart IS NULL)
				 OR (new.SectionEnd IS NOT NULL AND old.SectionEnd IS NULL)
				 OR (new.AdClassSchedId IS NOT NULL AND old.AdClassSchedId IS NULL)
				 OR (new.WeekNumber IS NOT NULL AND old.WeekNumber IS NULL)
				 OR (new.Week1AssignmentCount IS NOT NULL AND old.Week1AssignmentCount IS NULL)
				 OR (new.Week2AssignmentCount IS NOT NULL AND old.Week2AssignmentCount IS NULL)
				 OR (new.Week3AssignmentCount IS NOT NULL AND old.Week3AssignmentCount IS NULL)
				 OR (new.Week4AssignmentCount IS NOT NULL AND old.Week4AssignmentCount IS NULL)
				 OR (new.Week5AssignmentCount IS NOT NULL AND old.Week5AssignmentCount IS NULL)
				 OR (new.PrimaryInstructor IS NOT NULL AND old.PrimaryInstructor IS NULL)
				 OR (new.SecondaryInstructor IS NOT NULL AND old.SecondaryInstructor IS NULL)
				 OR (new.Week1StartDate IS NOT NULL AND old.Week1StartDate IS NULL)
				 OR (new.Week2StartDate IS NOT NULL AND old.Week2StartDate IS NULL)
				 OR (new.Week3StartDate IS NOT NULL AND old.Week3StartDate IS NULL)
				 OR (new.Week4StartDate IS NOT NULL AND old.Week4StartDate IS NULL)
				 OR (new.Week5StartDate IS NOT NULL AND old.Week5StartDate IS NULL)
				 OR (new.ExtensionWeekStartDate IS NOT NULL AND old.ExtensionWeekStartDate IS NULL)
				 OR (new.IsOrganization IS NOT NULL AND old.IsOrganization IS NULL)
				 OR (new.AcademicFacilitator IS NOT NULL AND old.AcademicFacilitator IS NULL)
				 OR (new.PrimaryInstructorId IS NOT NULL AND old.PrimaryInstructorId IS NULL)
				 OR (new.SecondaryInstructorId IS NOT NULL AND old.SecondaryInstructorId IS NULL)
				 OR (new.AcademicFacilitatorId IS NOT NULL AND old.AcademicFacilitatorId IS NULL)
				 OR (new.DayNumber IS NOT NULL AND old.DayNumber IS NULL)
				 OR (new.CengageCourseIndicator IS NOT NULL AND old.CengageCourseIndicator IS NULL)
				 OR (new.SourceSystem IS NOT NULL AND old.SourceSystem IS NULL)
				);

			-- Update LS_ODS Course Table To Inactivate Changed Course Records 
			UPDATE old
			SET old.ActiveFlag = 0
			FROM LS_ODS.Courses old WITH (INDEX(ix_Courses_ActiveFlag_CoursePrimaryKey)) -- Use specific index
			INNER JOIN @ChangedCourses new ON old.CoursePrimaryKey = new.CoursePrimaryKey;

			-- Add Changed Course Records To LS_ODS Course Table 
			INSERT INTO LS_ODS.Courses (
				CoursePrimaryKey,
				DateTimeCreated,
				DateTimeModified,
				RowStatus,
				BatchUniqueIdentifier,
				CourseCode,
				CourseName,
				SectionNumber,
				SectionStart,
				SectionEnd,
				AdClassSchedId,
				WeekNumber,
				Week1AssignmentCount,
				Week2AssignmentCount,
				Week3AssignmentCount,
				Week4AssignmentCount,
				Week5AssignmentCount,
				PrimaryInstructor,
				SecondaryInstructor,
				Week1StartDate,
				Week2StartDate,
				Week3StartDate,
				Week4StartDate,
				Week5StartDate,
				ExtensionWeekStartDate,
				IsOrganization,
				AcademicFacilitator,
				PrimaryInstructorId,
				SecondaryInstructorId,
				AcademicFacilitatorId,
				DayNumber,
				CengageCourseIndicator,
				SourceSystem
			)
			SELECT new.CoursePrimaryKey,
				   new.DateTimeCreated,
				   new.DateTimeModified,
				   new.RowStatus,
				   new.BatchUniqueIdentifier,
				   new.CourseCode,
				   new.CourseName,
				   new.SectionNumber,
				   new.SectionStart,
				   new.SectionEnd,
				   new.AdClassSchedId,
				   new.WeekNumber,
				   new.Week1AssignmentCount,
				   new.Week2AssignmentCount,
				   new.Week3AssignmentCount,
				   new.Week4AssignmentCount,
				   new.Week5AssignmentCount,
				   new.PrimaryInstructor,
				   new.SecondaryInstructor,
				   new.Week1StartDate,
				   new.Week2StartDate,
				   new.Week3StartDate,
				   new.Week4StartDate,
				   new.Week5StartDate,
				   new.ExtensionWeekStartDate,
				   new.IsOrganization,
				   new.AcademicFacilitator,
				   new.PrimaryInstructorId,
				   new.SecondaryInstructorId,
				   new.AcademicFacilitatorId,
				   new.DayNumber,
				   new.CengageCourseIndicator,
				   new.SourceSystem
			FROM stage.Courses new
			INNER JOIN @ChangedCourses changed ON new.CoursePrimaryKey = changed.CoursePrimaryKey;

        EXEC LS_ODS.AddODSLoadLog 'Updated Course Records That Have Changed', 0;

        --**************************************************************************************************************************************** 
        --Update Assignment records that have changed 
        --**************************************************************************************************************************************** 
        --Create Table Variable To Hold Changed Assignment Records 
        -- Use OPTION (RECOMPILE) hint to optimize execution plan
		DECLARE @ChangedAssignments TABLE (AssignmentPrimaryKey INT);

		-- Find Changed Assignments And Populate Table Variable 
		INSERT INTO @ChangedAssignments (AssignmentPrimaryKey)
		SELECT new.AssignmentPrimaryKey
		FROM stage.Assignments new WITH (FORCESEEK) -- Force index seek where possible
		INNER JOIN LS_ODS.Assignments old WITH (INDEX(ix_Assignments_ActiveFlag_AssignmentPrimaryKey)) -- Use specific index
			ON new.AssignmentPrimaryKey = old.AssignmentPrimaryKey
		   AND old.ActiveFlag = 1
		WHERE EXISTS (
			SELECT 1
			FROM (
				VALUES
					(new.CoursePrimaryKey, old.CoursePrimaryKey),
					(new.WeekNumber, old.WeekNumber),
					(new.AssignmentTitle, old.AssignmentTitle),
					(new.DueDate, old.DueDate),
					(new.PossiblePoints, old.PossiblePoints),
					(new.DateTimeCreated, old.DateTimeCreated),
					(new.DateTimeModified, old.DateTimeModified),
					(new.ScoreProviderHandle, old.ScoreProviderHandle),
					(new.CourseContentsPrimaryKey1, old.CourseContentsPrimaryKey1),
					(new.AlternateTitle, old.AlternateTitle),
					(new.IsReportable, old.IsReportable),
					(new.CountsAsSubmission, old.CountsAsSubmission),
					(new.AssignmentType, old.AssignmentType),
					(CASE WHEN new.CoursePrimaryKey IS NOT NULL AND old.CoursePrimaryKey IS NULL THEN 1 ELSE 0 END),
					(CASE WHEN new.WeekNumber IS NOT NULL AND old.WeekNumber IS NULL THEN 1 ELSE 0 END),
					(CASE WHEN new.AssignmentTitle IS NOT NULL AND old.AssignmentTitle IS NULL THEN 1 ELSE 0 END),
					(CASE WHEN new.DueDate IS NOT NULL AND old.DueDate IS NULL THEN 1 ELSE 0 END),
					(CASE WHEN new.PossiblePoints IS NOT NULL AND old.PossiblePoints IS NULL THEN 1 ELSE 0 END),
					(CASE WHEN new.DateTimeCreated IS NOT NULL AND old.DateTimeCreated IS NULL THEN 1 ELSE 0 END),
					(CASE WHEN new.DateTimeModified IS NOT NULL AND old.DateTimeModified IS NULL THEN 1 ELSE 0 END),
					(CASE WHEN new.ScoreProviderHandle IS NOT NULL AND old.ScoreProviderHandle IS NULL THEN 1 ELSE 0 END),
					(CASE WHEN new.CourseContentsPrimaryKey1 IS NOT NULL AND old.CourseContentsPrimaryKey1 IS NULL THEN 1 ELSE 0 END),
					(CASE WHEN new.AlternateTitle IS NOT NULL AND old.AlternateTitle IS NULL THEN 1 ELSE 0 END),
					(CASE WHEN new.IsReportable IS NOT NULL AND old.IsReportable IS NULL THEN 1 ELSE 0 END),
					(CASE WHEN new.CountsAsSubmission IS NOT NULL AND old.CountsAsSubmission IS NULL THEN 1 ELSE 0 END),
					(CASE WHEN new.AssignmentType IS NOT NULL AND old.AssignmentType IS NULL THEN 1 ELSE 0 END),
					(CASE WHEN new.SourceSystem IS NOT NULL AND old.SourceSystem IS NULL THEN 1 ELSE 0 END)
			) AS C (NewValue, OldValue)
			WHERE NewValue IS NOT NULL AND NewValue <> OldValue
		);

		-- Update LS_ODS Assignments Table To Inactivate Changed Assignments Records 
		UPDATE old
		SET old.ActiveFlag = 0
		FROM LS_ODS.Assignments old WITH (INDEX(ix_Assignments_ActiveFlag_AssignmentPrimaryKey)) -- Use specific index
		INNER JOIN @ChangedAssignments new ON old.AssignmentPrimaryKey = new.AssignmentPrimaryKey;

-- Count the number of D2L assignments in stage
		DECLARE @CountStageD2LAssignments INT;
		SELECT @CountStageD2LAssignments = COUNT(*)
		FROM stage.Assignments
		WHERE SourceSystem = 'D2L';

		-- Check if there are D2L assignments to process
		IF @CountStageD2LAssignments > 0
		BEGIN
			-- Update LS_ODS Assignments Table To Inactivate Duplicated D2L Assignments
			UPDATE Assignments
			SET Assignments.ActiveFlag = 0
			FROM LS_ODS.Assignments Assignments
			WHERE EXISTS (
				SELECT 1
				FROM (
					SELECT asg.CoursePrimaryKey,
						   asg.WeekNumber,
						   asg.AssignmentTitle,
						   COUNT(*) AS Total
					FROM LS_ODS.Assignments asg
					GROUP BY asg.CoursePrimaryKey,
							 asg.WeekNumber,
							 asg.AssignmentTitle
					HAVING COUNT(*) > 1
				) da
					INNER JOIN LS_ODS.Assignments asg ON da.CoursePrimaryKey = asg.CoursePrimaryKey
													   AND da.WeekNumber = asg.WeekNumber
													   AND da.AssignmentTitle = asg.AssignmentTitle
					INNER JOIN dbo.COURSE_MAIN cm ON cm.PK1 = asg.CoursePrimaryKey
												   AND cm.SourceSystem = 'D2L'
					LEFT JOIN stage.Assignments sasg ON sasg.AssignmentPrimaryKey = asg.AssignmentPrimaryKey
				WHERE sasg.AssignmentPrimaryKey IS NULL
			);

			-- Update LS_ODS Assignments Table To Inactivate deleted Assignments
			UPDATE Assignments
			SET Assignments.ActiveFlag = 0
			FROM LS_ODS.Assignments Assignments
				LEFT JOIN stage.Assignments sasg ON sasg.AssignmentPrimaryKey = Assignments.AssignmentPrimaryKey
			WHERE Assignments.SourceSystem = 'D2L'
				  AND sasg.AssignmentPrimaryKey IS NULL;
		END

		-- Add Changed Assignment Records To LS_ODS Assignments Table 
		INSERT INTO LS_ODS.Assignments (
			AssignmentPrimaryKey,
			CoursePrimaryKey,
			WeekNumber,
			AssignmentTitle,
			DueDate,
			PossiblePoints,
			DateTimeCreated,
			DateTimeModified,
			ScoreProviderHandle,
			CourseContentsPrimaryKey1,
			AlternateTitle,
			IsReportable,
			CountsAsSubmission,
			AssignmentType,
			SourceSystem
		)
		SELECT new.AssignmentPrimaryKey,
			   new.CoursePrimaryKey,
			   new.WeekNumber,
			   new.AssignmentTitle,
			   new.DueDate,
			   new.PossiblePoints,
			   new.DateTimeCreated,
			   new.DateTimeModified,
			   new.ScoreProviderHandle,
			   new.CourseContentsPrimaryKey1,
			   new.AlternateTitle,
			   new.IsReportable,
			   new.CountsAsSubmission,
			   new.AssignmentType,
			   new.SourceSystem
		FROM stage.Assignments new
		INNER JOIN @ChangedAssignments changed ON new.AssignmentPrimaryKey = changed.AssignmentPrimaryKey;

        EXEC LS_ODS.AddODSLoadLog 'Updated Assignment Records That Have Changed',
                                  0;

        --**************************************************************************************************************************************** 
        --Update Grade records that have changed 
        --**************************************************************************************************************************************** 
        --Create Table Variable To Hold Changed Grades Records 
        -- Find Changed Grades And Populate Table Variable 
			DECLARE @ChangedGrades TABLE (GradePrimaryKey INT);

			INSERT INTO @ChangedGrades (GradePrimaryKey)
			SELECT new.GradePrimaryKey
			FROM stage.Grades new
			INNER JOIN LS_ODS.Grades old ON new.GradePrimaryKey = old.GradePrimaryKey
			WHERE EXISTS (
				SELECT 1
				FROM (
					VALUES
						(new.CourseUsersPrimaryKey, old.CourseUsersPrimaryKey),
						(new.RowStatus, old.RowStatus),
						(new.HighestScore, old.HighestScore),
						(new.HighestGrade, old.HighestGrade),
						(new.HighestAttemptDateTime, old.HighestAttemptDateTime),
						(new.ManualScore, old.ManualScore),
						(new.ManualGrade, old.ManualGrade),
						(new.ManualDateTime, old.ManualDateTime),
						(new.ExemptIndicator, old.ExemptIndicator),
						(new.HighestDateTimeCreated, old.HighestDateTimeCreated),
						(new.HighestDateTimeModified, old.HighestDateTimeModified),
						(new.HighestIsLatestAttemptIndicator, old.HighestIsLatestAttemptIndicator),
						(new.NumberOfAttempts, old.NumberOfAttempts),
						(new.FirstScore, old.FirstScore),
						(new.FirstGrade, old.FirstGrade),
						(new.FirstAttemptDateTime, old.FirstAttemptDateTime),
						(new.FirstIsLatestAttemptIndicator, old.FirstIsLatestAttemptIndicator),
						(new.FirstDateTimeCreated, old.FirstDateTimeCreated),
						(new.FirstDateTimeModified, old.FirstDateTimeModified),
						(new.AssignmentPrimaryKey, old.AssignmentPrimaryKey),
						(new.AssignmentStatus, old.AssignmentStatus)
				) AS A (new_value, old_value)
				WHERE new_value IS NOT NULL AND new_value <> old_value
			) OR (new.CourseUsersPrimaryKey IS NOT NULL AND old.CourseUsersPrimaryKey IS NULL)
			  OR (new.RowStatus IS NOT NULL AND old.RowStatus IS NULL)
			  OR (new.HighestScore IS NOT NULL AND old.HighestScore IS NULL)
			  OR (new.HighestGrade IS NOT NULL AND old.HighestGrade IS NULL)
			  OR (new.HighestAttemptDateTime IS NOT NULL AND old.HighestAttemptDateTime IS NULL)
			  OR (new.ManualScore IS NOT NULL AND old.ManualScore IS NULL)
			  OR (new.ManualGrade IS NOT NULL AND old.ManualGrade IS NULL)
			  OR (new.ManualDateTime IS NOT NULL AND old.ManualDateTime IS NULL)
			  OR (new.ExemptIndicator IS NOT NULL AND old.ExemptIndicator IS NULL)
			  OR (new.HighestDateTimeCreated IS NOT NULL AND old.HighestDateTimeCreated IS NULL)
			  OR (new.HighestDateTimeModified IS NOT NULL AND old.HighestDateTimeModified IS NULL)
			  OR (new.HighestIsLatestAttemptIndicator IS NOT NULL AND old.HighestIsLatestAttemptIndicator IS NULL)
			  OR (new.NumberOfAttempts IS NOT NULL AND old.NumberOfAttempts IS NULL)
			  OR (new.FirstScore IS NOT NULL AND old.FirstScore IS NULL)
			  OR (new.FirstGrade IS NOT NULL AND old.FirstGrade IS NULL)
			  OR (new.FirstAttemptDateTime IS NOT NULL AND old.FirstAttemptDateTime IS NULL)
			  OR (new.FirstIsLatestAttemptIndicator IS NOT NULL AND old.FirstIsLatestAttemptIndicator IS NULL)
			  OR (new.FirstDateTimeCreated IS NOT NULL AND old.FirstDateTimeCreated IS NULL)
			  OR (new.FirstDateTimeModified IS NOT NULL AND old.FirstDateTimeModified IS NULL)
			  OR (new.AssignmentPrimaryKey IS NOT NULL AND old.AssignmentPrimaryKey IS NULL)
			  OR (new.AssignmentStatus IS NOT NULL AND old.AssignmentStatus IS NULL);

			-- Update LS_ODS Grades Table To Inactivate Changed Grades Records 
			UPDATE old
			SET old.ActiveFlag = 0
			FROM LS_ODS.Grades old
			INNER JOIN @ChangedGrades new ON old.GradePrimaryKey = new.GradePrimaryKey;

			-- Update LS_ODS Grades Table To Inactivate Grades with Duplicated D2L Assignments
			UPDATE Grades
			SET Grades.[ActiveFlag] = 0
			FROM LS_ODS.Grades Grades
			WHERE EXISTS (
				SELECT 1
				FROM (
					SELECT asg.CoursePrimaryKey, asg.WeekNumber, asg.AssignmentTitle
					FROM LS_ODS.Assignments asg
					GROUP BY asg.CoursePrimaryKey, asg.WeekNumber, asg.AssignmentTitle
					HAVING COUNT(*) > 1
				) AS da
				INNER JOIN LS_ODS.Assignments asg ON da.CoursePrimaryKey = asg.CoursePrimaryKey
												 AND da.WeekNumber = asg.WeekNumber
												 AND da.AssignmentTitle = asg.AssignmentTitle
				INNER JOIN dbo.COURSE_MAIN cm ON cm.PK1 = asg.CoursePrimaryKey AND cm.SourceSystem = 'D2L'
				LEFT JOIN stage.Assignments sasg ON sasg.AssignmentPrimaryKey = asg.AssignmentPrimaryKey
				WHERE sasg.AssignmentPrimaryKey IS NULL
				AND Grades.AssignmentPrimaryKey = asg.AssignmentPrimaryKey
			);

			-- Add Changed Grades Records To LS_ODS Grades Table 
			INSERT INTO LS_ODS.Grades (
				GradePrimaryKey,
				CourseUsersPrimaryKey,
				RowStatus,
				HighestScore,
				HighestGrade,
				HighestAttemptDateTime,
				ManualScore,
				ManualGrade,
				ManualDateTime,
				ExemptIndicator,
				HighestDateTimeCreated,
				HighestDateTimeModified,
				HighestIsLatestAttemptIndicator,
				NumberOfAttempts,
				FirstScore,
				FirstGrade,
				FirstAttemptDateTime,
				FirstIsLatestAttemptIndicator,
				FirstDateTimeCreated,
				FirstDateTimeModified,
				AssignmentPrimaryKey,
				AssignmentStatus,
				SourceSystem
			)
			SELECT new.GradePrimaryKey,
				   new.CourseUsersPrimaryKey,
				   new.RowStatus,
				   new.HighestScore,
				   new.HighestGrade,
				   new.HighestAttemptDateTime,
				   new.ManualScore,
				   new.ManualGrade,
				   new.ManualDateTime,
				   new.ExemptIndicator,
				   new.HighestDateTimeCreated,
				   new.HighestDateTimeModified,
				   new.HighestIsLatestAttemptIndicator,
				   new.NumberOfAttempts,
				   new.FirstScore,
				   new.FirstGrade,
				   new.FirstAttemptDateTime,
				   new.FirstIsLatestAttemptIndicator,
				   new.FirstDateTimeCreated,
				   new.FirstDateTimeModified,
				   new.AssignmentPrimaryKey,
				   new.AssignmentStatus,
				   new.SourceSystem
			FROM stage.Grades new
			INNER JOIN @ChangedGrades changed ON new.GradePrimaryKey = changed.GradePrimaryKey;

			-- Performance Fix for LS_ODS Grades Table CRUD; Index Balancing
			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_LSiDashActiveFlagLargeIncludeDESC')
			BEGIN
				DROP INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades;
			END;

			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_LSiDashAssignmentPrimaryKeyDESC')
			BEGIN
				DROP INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades;
			END;

			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC')
			BEGIN
				DROP INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades;
			END;

			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_LSiDashCourseUsersPrimaryKeyDESC')
			BEGIN
				DROP INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades;
			END;

			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_CourseUsersPKAssignPKActiveFG')
			BEGIN
				DROP INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades;
			END;

			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = '_dta_index_Grades_10_1223675407__K2_K23_K21_4')
			BEGIN
				DROP INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades;
			END;

			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_ODS_010')
			BEGIN
				DROP INDEX idx_ODS_010 ON LS_ODS.Grades;
			END;

			-- Add Indexes back for LS_ODS Grades Table
			CREATE NONCLUSTERED INDEX idx_ODS_010
			ON LS_ODS.Grades (
				GradePrimaryKey ASC,
				ActiveFlag ASC
			)
			INCLUDE (
				CourseUsersPrimaryKey,
				RowStatus,
				HighestScore,
				HighestGrade,
				HighestAttemptDateTime,
				ManualScore,
				ManualGrade,
				ManualDateTime,
				ExemptIndicator,
				HighestDateTimeCreated,
				HighestDateTimeModified,
				HighestIsLatestAttemptIndicator,
				NumberOfAttempts,
				FirstScore,
				FirstGrade,
				FirstAttemptDateTime,
				FirstIsLatestAttemptIndicator,
				FirstDateTimeModified,
				AssignmentPrimaryKey,
				AssignmentStatus,
				SourceSystem
			)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF,
				  ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
				 ) ON [PRIMARY];

			CREATE NONCLUSTERED INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4
			ON LS_ODS.Grades (
				CourseUsersPrimaryKey ASC,
				ActiveFlag ASC,
				AssignmentPrimaryKey ASC
			)
			INCLUDE (HighestScore)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF,
				  ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
				 ) ON [PRIMARY];

			CREATE NONCLUSTERED INDEX idx_LSiDashCourseUsersPrimaryKeyDESC
			ON LS_ODS.Grades (CourseUsersPrimaryKey DESC)
			INCLUDE (
				RowStatus,
				HighestScore,
				HighestGrade,
				HighestAttemptDateTime,
				ManualScore,
				ManualGrade,
				ManualDateTime,
				ExemptIndicator,
				HighestDateTimeCreated,
				HighestDateTimeModified,
				HighestIsLatestAttemptIndicator,
				NumberOfAttempts,
				FirstScore,
				FirstGrade,
				FirstAttemptDateTime,
				FirstIsLatestAttemptIndicator,
				FirstDateTimeCreated,
				FirstDateTimeModified,
				AssignmentPrimaryKey,
				AssignmentStatus,
				SourceSystem,
				ActiveFlag
			);

			CREATE NONCLUSTERED INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC
			ON LS_ODS.Grades (
				CourseUsersPrimaryKey DESC,
				AssignmentPrimaryKey DESC,
				ActiveFlag DESC
			)
			INCLUDE (
				RowStatus,
				HighestScore,
				HighestGrade,
				HighestAttemptDateTime,
				ManualScore,
				ManualGrade,
				ManualDateTime,
				ExemptIndicator,
				HighestDateTimeCreated,
				HighestDateTimeModified,
				HighestIsLatestAttemptIndicator,
				NumberOfAttempts,
				FirstScore,
				FirstGrade,
				FirstAttemptDateTime,
				FirstIsLatestAttemptIndicator,
				FirstDateTimeCreated,
				FirstDateTimeModified,
				AssignmentStatus,
				SourceSystem
			);

			CREATE NONCLUSTERED INDEX idx_LSiDashAssignmentPrimaryKeyDESC
			ON LS_ODS.Grades (
				AssignmentPrimaryKey DESC
			)
			INCLUDE (
				CourseUsersPrimaryKey,
				RowStatus,
				HighestScore,
				HighestGrade,
				HighestAttemptDateTime,
				ManualScore,
				ManualGrade,
				ManualDateTime,
				ExemptIndicator,
				HighestDateTimeCreated,
				HighestDateTimeModified,
				HighestIsLatestAttemptIndicator,
				NumberOfAttempts,
				FirstScore,
				FirstGrade,
				FirstAttemptDateTime,
				FirstIsLatestAttemptIndicator,
				FirstDateTimeCreated,
				FirstDateTimeModified,
				AssignmentStatus,
				SourceSystem,
				ActiveFlag
			);

			CREATE NONCLUSTERED INDEX idx_LSiDashActiveFlagLargeIncludeDESC
			ON LS_ODS.Grades (
				ActiveFlag DESC
			)
			INCLUDE (
				CourseUsersPrimaryKey,
				RowStatus,
				HighestScore,
				HighestGrade,
				HighestAttemptDateTime,
				ManualScore,
				ManualGrade,
				ManualDateTime,
				ExemptIndicator,
				HighestDateTimeCreated,
				HighestDateTimeModified,
				HighestIsLatestAttemptIndicator,
				NumberOfAttempts,
				FirstScore,
				FirstGrade,
				FirstAttemptDateTime,
				FirstIsLatestAttemptIndicator,
				FirstDateTimeCreated,
				FirstDateTimeModified,
				AssignmentPrimaryKey,
				AssignmentStatus,
				SourceSystem
			);

			-- Add Changed Grades Records To LS_ODS Grades Table
			INSERT INTO LS_ODS.Grades (
				GradePrimaryKey,
				CourseUsersPrimaryKey,
				RowStatus,
				HighestScore,
				HighestGrade,
				HighestAttemptDateTime,
				ManualScore,
				ManualGrade,
				ManualDateTime,
				ExemptIndicator,
				HighestDateTimeCreated,
				HighestDateTimeModified,
				HighestIsLatestAttemptIndicator,
				NumberOfAttempts,
				FirstScore,
				FirstGrade,
				FirstAttemptDateTime,
				FirstIsLatestAttemptIndicator,
				FirstDateTimeCreated,
				FirstDateTimeModified,
				AssignmentPrimaryKey,
				AssignmentStatus,
				SourceSystem
			)
			SELECT new.GradePrimaryKey,
				   new.CourseUsersPrimaryKey,
				   new.RowStatus,
				   new.HighestScore,
				   new.HighestGrade,
				   new.HighestAttemptDateTime,
				   new.ManualScore,
				   new.ManualGrade,
				   new.ManualDateTime,
				   new.ExemptIndicator,
				   new.HighestDateTimeCreated,
				   new.HighestDateTimeModified,
				   new.HighestIsLatestAttemptIndicator,
				   new.NumberOfAttempts,
				   new.FirstScore,
				   new.FirstGrade,
				   new.FirstAttemptDateTime,
				   new.FirstIsLatestAttemptIndicator,
				   new.FirstDateTimeCreated,
				   new.FirstDateTimeModified,
				   new.AssignmentPrimaryKey,
				   new.AssignmentStatus,
				   new.SourceSystem
			FROM stage.Grades new
			INNER JOIN @ChangedGrades changed ON new.GradePrimaryKey = changed.GradePrimaryKey;

			-- Performance Fix for LS_ODS Grades Table CRUD; Index Balancing
			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_LSiDashActiveFlagLargeIncludeDESC')
			BEGIN
				DROP INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades;
			END;

			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_LSiDashAssignmentPrimaryKeyDESC')
			BEGIN
				DROP INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades;
			END;

			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC')
			BEGIN
				DROP INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades;
			END;

			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_LSiDashCourseUsersPrimaryKeyDESC')
			BEGIN
				DROP INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades;
			END;

			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_CourseUsersPKAssignPKActiveFG')
			BEGIN
				DROP INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades;
			END;

			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = '_dta_index_Grades_10_1223675407__K2_K23_K21_4')
			BEGIN
				DROP INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades;
			END;

			IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_ODS_010')
			BEGIN
				DROP INDEX idx_ODS_010 ON LS_ODS.Grades;
			END;

			-- Add Indexes back for LS_ODS Grades Table
			CREATE NONCLUSTERED INDEX idx_ODS_010
			ON LS_ODS.Grades (
				GradePrimaryKey ASC,
				ActiveFlag ASC
			)
			INCLUDE (
				CourseUsersPrimaryKey,
				RowStatus,
				HighestScore,
				HighestGrade,
				HighestAttemptDateTime,
				ManualScore,
				ManualGrade,
				ManualDateTime,
				ExemptIndicator,
				HighestDateTimeCreated,
				HighestDateTimeModified,
				HighestIsLatestAttemptIndicator,
				NumberOfAttempts,
				FirstScore,
				FirstGrade,
				FirstAttemptDateTime,
				FirstIsLatestAttemptIndicator,
				FirstDateTimeModified,
				AssignmentPrimaryKey,
				AssignmentStatus,
				SourceSystem
			)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF,
				  ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
				 ) ON [PRIMARY];

			CREATE NONCLUSTERED INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4
			ON LS_ODS.Grades (
				CourseUsersPrimaryKey ASC,
				ActiveFlag ASC,
				AssignmentPrimaryKey ASC
			)
			INCLUDE (HighestScore)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF,
				  ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
				 ) ON [PRIMARY];

			CREATE NONCLUSTERED INDEX idx_LSiDashCourseUsersPrimaryKeyDESC
			ON LS_ODS.Grades (CourseUsersPrimaryKey DESC)
			INCLUDE (
				RowStatus,
				HighestScore,
				HighestGrade,
				HighestAttemptDateTime,
				ManualScore,
				ManualGrade,
				ManualDateTime,
				ExemptIndicator,
				HighestDateTimeCreated,
				HighestDateTimeModified,
				HighestIsLatestAttemptIndicator,
				NumberOfAttempts,
				FirstScore,
				FirstGrade,
				FirstAttemptDateTime,
				FirstIsLatestAttemptIndicator,
				FirstDateTimeCreated,
				FirstDateTimeModified,
				AssignmentPrimaryKey,
				AssignmentStatus,
				SourceSystem,
				ActiveFlag
			);

			CREATE NONCLUSTERED INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC
			ON LS_ODS.Grades (
				CourseUsersPrimaryKey DESC,
				AssignmentPrimaryKey DESC,
				ActiveFlag DESC
			)
			INCLUDE (
				RowStatus,
				HighestScore,
				HighestGrade,
				HighestAttemptDateTime,
				ManualScore,
				ManualGrade,
				ManualDateTime,
				ExemptIndicator,
				HighestDateTimeCreated,
				HighestDateTimeModified,
				HighestIsLatestAttemptIndicator,
				NumberOfAttempts,
				FirstScore,
				FirstGrade,
				FirstAttemptDateTime,
				FirstIsLatestAttemptIndicator,
				FirstDateTimeCreated,
				FirstDateTimeModified,
				AssignmentStatus,
				SourceSystem
			);

			CREATE NONCLUSTERED INDEX idx_LSiDashAssignmentPrimaryKeyDESC
			ON LS_ODS.Grades (
				AssignmentPrimaryKey DESC
			)
			INCLUDE (
				CourseUsersPrimaryKey,
				RowStatus,
				HighestScore,
				HighestGrade,
				HighestAttemptDateTime,
				ManualScore,
				ManualGrade,
				ManualDateTime,
				ExemptIndicator,
				HighestDateTimeCreated,
				HighestDateTimeModified,
				HighestIsLatestAttemptIndicator,
				NumberOfAttempts,
				FirstScore,
				FirstGrade,
				FirstAttemptDateTime,
				FirstIsLatestAttemptIndicator,
				FirstDateTimeCreated,
				FirstDateTimeModified,
				AssignmentStatus,
				SourceSystem,
				ActiveFlag
			);

			CREATE NONCLUSTERED INDEX idx_LSiDashActiveFlagLargeIncludeDESC
			ON LS_ODS.Grades (
				ActiveFlag DESC
			)
			INCLUDE (
				CourseUsersPrimaryKey,
				RowStatus,
				HighestScore,
				HighestGrade,
				HighestAttemptDateTime,
				ManualScore,
				ManualGrade,
				ManualDateTime,
				ExemptIndicator,
				HighestDateTimeCreated,
				HighestDateTimeModified,
				HighestIsLatestAttemptIndicator,
				NumberOfAttempts,
				FirstScore,
				FirstGrade,
				FirstAttemptDateTime,
				FirstIsLatestAttemptIndicator,
				FirstDateTimeCreated,
				FirstDateTimeModified,
				AssignmentPrimaryKey,
				AssignmentStatus,
				SourceSystem
			);

        EXEC LS_ODS.AddODSLoadLog 'Updated Grades Records That Have Changed', 0;

        --**************************************************************************************************************************************** 
        --Add new Student records 
        --**************************************************************************************************************************************** 
        --Insert New Student Records To Students Table 
        -- Create necessary indexes if they don't exist
			IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_Students_StudentCourseUserKeys')
			BEGIN
				CREATE NONCLUSTERED INDEX idx_Students_StudentCourseUserKeys
				ON LS_ODS.Students (StudentCourseUserKeys);
			END

			-- Insert new records into LS_ODS.Students table where StudentCourseUserKeys do not already exist
			INSERT INTO LS_ODS.Students (
				StudentPrimaryKey,
				DateTimeCreated,
				DateTimeModified,
				RowStatus,
				BatchUniqueIdentifier,
				BlackboardUsername,
				SyStudentId,
				FirstName,
				LastName,
				Campus,
				AdEnrollSchedId,
				AdClassSchedId,
				CourseUsersPrimaryKey,
				LastLoginDateTime,
				TimeInClass,
				LastI3InteractionNumberMainPhone,
				LastI3InteractionDateTimeMainPhone,
				DaysSinceLastI3InteractionMainPhone,
				LastI3InteractionNumberWorkPhone,
				LastI3InteractionDateTimeWorkPhone,
				DaysSinceLastI3InteractionWorkPhone,
				LastI3InteractionNumberMobilePhone,
				LastI3InteractionDateTimeMobilePhone,
				DaysSinceLastI3InteractionMobilePhone,
				LastI3InteractionNumberOtherPhone,
				LastI3InteractionDateTimeOtherPhone,
				DaysSinceLastI3InteractionOtherPhone,
				Week1Grade,
				Week2Grade,
				Week3Grade,
				Week4Grade,
				Week5Grade,
				SelfTestsCount,
				AssessmentsCount,
				AssignmentsCount,
				DiscussionsCount,
				Week1CompletionRate,
				Week2CompletionRate,
				Week3CompletionRate,
				Week4CompletionRate,
				Week5CompletionRate,
				VAStudent,
				NoticeName,
				NoticeDueDate,
				VABenefitName,
				ClassStatus,
				Week1LDA,
				Week2LDA,
				Week3LDA,
				Week4LDA,
				Week5LDA,
				Week1CompletedAssignments,
				Week2CompletedAssignments,
				Week3CompletedAssignments,
				Week4CompletedAssignments,
				Week5CompletedAssignments,
				CoursePercentage,
				TotalWorkPercentage,
				AdEnrollId,
				IsRetake,
				StudentCourseUserKeys,
				CurrentCourseGrade,
				ProgramCode,
				ProgramName,
				ProgramVersionCode,
				ProgramVersionName,
				MondayTimeInClass,
				TuesdayTimeInClass,
				WednesdayTimeInClass,
				ThursdayTimeInClass,
				FridayTimeInClass,
				SaturdayTimeInClass,
				SundayTimeInClass,
				Week1CompletionRateFixed,
				Week2CompletionRateFixed,
				Week3CompletionRateFixed,
				Week4CompletionRateFixed,
				Week5CompletionRateFixed,
				StudentNumber,
				SourceSystem
			)
			SELECT DISTINCT
				new.StudentPrimaryKey,
				new.DateTimeCreated,
				new.DateTimeModified,
				new.RowStatus,
				new.BatchUniqueIdentifier,
				new.BlackboardUsername,
				new.SyStudentId,
				new.FirstName,
				new.LastName,
				new.Campus,
				new.AdEnrollSchedId,
				new.AdClassSchedId,
				new.CourseUsersPrimaryKey,
				new.LastLoginDateTime,
				new.TimeInClass,
				new.LastI3InteractionNumberMainPhone,
				new.LastI3InteractionDateTimeMainPhone,
				new.DaysSinceLastI3InteractionMainPhone,
				new.LastI3InteractionNumberWorkPhone,
				new.LastI3InteractionDateTimeWorkPhone,
				new.DaysSinceLastI3InteractionWorkPhone,
				new.LastI3InteractionNumberMobilePhone,
				new.LastI3InteractionDateTimeMobilePhone,
				new.DaysSinceLastI3InteractionMobilePhone,
				new.LastI3InteractionNumberOtherPhone,
				new.LastI3InteractionDateTimeOtherPhone,
				new.DaysSinceLastI3InteractionOtherPhone,
				new.Week1Grade,
				new.Week2Grade,
				new.Week3Grade,
				new.Week4Grade,
				new.Week5Grade,
				new.SelfTestsCount,
				new.AssessmentsCount,
				new.AssignmentsCount,
				new.DiscussionsCount,
				new.Week1CompletionRate,
				new.Week2CompletionRate,
				new.Week3CompletionRate,
				new.Week4CompletionRate,
				new.Week5CompletionRate,
				new.VAStudent,
				new.NoticeName,
				new.NoticeDueDate,
				new.VABenefitName,
				new.ClassStatus,
				new.Week1LDA,
				new.Week2LDA,
				new.Week3LDA,
				new.Week4LDA,
				new.Week5LDA,
				new.Week1CompletedAssignments,
				new.Week2CompletedAssignments,
				new.Week3CompletedAssignments,
				new.Week4CompletedAssignments,
				new.Week5CompletedAssignments,
				new.CoursePercentage,
				new.TotalWorkPercentage,
				new.AdEnrollId,
				new.IsRetake,
				new.StudentCourseUserKeys,
				new.CurrentCourseGrade,
				new.ProgramCode,
				new.ProgramName,
				new.ProgramVersionCode,
				new.ProgramVersionName,
				new.MondayTimeInClass,
				new.TuesdayTimeInClass,
				new.WednesdayTimeInClass,
				new.ThursdayTimeInClass,
				new.FridayTimeInClass,
				new.SaturdayTimeInClass,
				new.SundayTimeInClass,
				new.Week1CompletionRateFixed,
				new.Week2CompletionRateFixed,
				new.Week3CompletionRateFixed,
				new.Week4CompletionRateFixed,
				new.Week5CompletionRateFixed,
				new.StudentNumber,
				new.SourceSystem
			FROM stage.Students new
			WHERE NOT EXISTS (
				SELECT 1
				FROM LS_ODS.Students old
				WHERE old.StudentCourseUserKeys = new.StudentCourseUserKeys
			);

			-- Create index after insertion if necessary
			CREATE NONCLUSTERED INDEX idx_Students_StudentCourseUserKeys
			ON LS_ODS.Students (StudentCourseUserKeys);


        EXEC LS_ODS.AddODSLoadLog 'Added New Students Records', 0;

        --**************************************************************************************************************************************** 
        --Add new Course records 
        --**************************************************************************************************************************************** 
        --Insert New Course Records To Courses Table 
        INSERT INTO LS_ODS.Courses
        (
            CoursePrimaryKey,
            DateTimeCreated,
            DateTimeModified,
            RowStatus,
            BatchUniqueIdentifier,
            CourseCode,
            CourseName,
            SectionNumber,
            SectionStart,
            SectionEnd,
            AdClassSchedId,
            WeekNumber,
            Week1AssignmentCount,
            Week2AssignmentCount,
            Week3AssignmentCount,
            Week4AssignmentCount,
            Week5AssignmentCount,
            PrimaryInstructor,
            SecondaryInstructor,
            Week1StartDate,
            Week2StartDate,
            Week3StartDate,
            Week4StartDate,
            Week5StartDate,
            ExtensionWeekStartDate,
            IsOrganization,
            AcademicFacilitator,
            PrimaryInstructorId,
            SecondaryInstructorId,
            AcademicFacilitatorId,
            DayNumber,
            CengageCourseIndicator,
            SourceSystem
        )
        SELECT new.CoursePrimaryKey,
               new.DateTimeCreated,
               new.DateTimeModified,
               new.RowStatus,
               new.BatchUniqueIdentifier,
               new.CourseCode,
               new.CourseName,
               new.SectionNumber,
               new.SectionStart,
               new.SectionEnd,
               new.AdClassSchedId,
               new.WeekNumber,
               new.Week1AssignmentCount,
               new.Week2AssignmentCount,
               new.Week3AssignmentCount,
               new.Week4AssignmentCount,
               new.Week5AssignmentCount,
               new.PrimaryInstructor,
               new.SecondaryInstructor,
               new.Week1StartDate,
               new.Week2StartDate,
               new.Week3StartDate,
               new.Week4StartDate,
               new.Week5StartDate,
               new.ExtensionWeekStartDate,
               new.IsOrganization,
               new.AcademicFacilitator,
               new.PrimaryInstructorId,
               new.SecondaryInstructorId,
               new.AcademicFacilitatorId,
               new.DayNumber,
               new.CengageCourseIndicator,
               new.SourceSystem
        FROM stage.Courses new
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM LS_ODS.Courses old
            WHERE old.CoursePrimaryKey = new.CoursePrimaryKey
        );

        EXEC LS_ODS.AddODSLoadLog 'Added New Course Records', 0;

        --**************************************************************************************************************************************** 
        --Add new Assignment records 
        --**************************************************************************************************************************************** 
        --Insert New Assignment Records To Assignments Table 
        INSERT INTO LS_ODS.Assignments
        (
            AssignmentPrimaryKey,
            CoursePrimaryKey,
            WeekNumber,
            AssignmentTitle,
            DueDate,
            PossiblePoints,
            DateTimeCreated,
            DateTimeModified,
            ScoreProviderHandle,
            CourseContentsPrimaryKey1,
            AlternateTitle,
            IsReportable,
            CountsAsSubmission,
            AssignmentType,
            SourceSystem
        )
        SELECT new.AssignmentPrimaryKey,
               new.CoursePrimaryKey,
               new.WeekNumber,
               new.AssignmentTitle,
               new.DueDate,
               new.PossiblePoints,
               new.DateTimeCreated,
               new.DateTimeModified,
               new.ScoreProviderHandle,
               new.CourseContentsPrimaryKey1,
               new.AlternateTitle,
               new.IsReportable,
               new.CountsAsSubmission,
               new.AssignmentType,
               new.SourceSystem
        FROM stage.Assignments new
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM LS_ODS.Assignments old
            WHERE old.AssignmentPrimaryKey = new.AssignmentPrimaryKey
        );

        EXEC LS_ODS.AddODSLoadLog 'Added New Assignment Records', 0;

        --**************************************************************************************************************************************** 
        --Add new Grade records 
        --**************************************************************************************************************************************** 
        --Insert New Grade Records Into Grades Table 
        INSERT INTO LS_ODS.Grades
        (
            GradePrimaryKey,
            CourseUsersPrimaryKey,
            RowStatus,
            HighestScore,
            HighestGrade,
            HighestAttemptDateTime,
            ManualScore,
            ManualGrade,
            ManualDateTime,
            ExemptIndicator,
            HighestDateTimeCreated,
            HighestDateTimeModified,
            HighestIsLatestAttemptIndicator,
            NumberOfAttempts,
            FirstScore,
            FirstGrade,
            FirstAttemptDateTime,
            FirstIsLatestAttemptIndicator,
            FirstDateTimeCreated,
            FirstDateTimeModified,
            AssignmentPrimaryKey,
            AssignmentStatus,
            SourceSystem
        )
        SELECT DISTINCT
            new.GradePrimaryKey,
            new.CourseUsersPrimaryKey,
            new.RowStatus,
            new.HighestScore,
            new.HighestGrade,
            new.HighestAttemptDateTime,
            new.ManualScore,
            new.ManualGrade,
            new.ManualDateTime,
            new.ExemptIndicator,
            new.HighestDateTimeCreated,
            new.HighestDateTimeModified,
            new.HighestIsLatestAttemptIndicator,
            new.NumberOfAttempts,
            new.FirstScore,
            new.FirstGrade,
            new.FirstAttemptDateTime,
            new.FirstIsLatestAttemptIndicator,
            new.FirstDateTimeCreated,
            new.FirstDateTimeModified,
            new.AssignmentPrimaryKey,
            new.AssignmentStatus,
            new.SourceSystem
        FROM stage.Grades new
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM LS_ODS.Grades old
            WHERE old.GradePrimaryKey = new.GradePrimaryKey
        );

        EXEC LS_ODS.AddODSLoadLog 'Added New Grade Records', 0;

        --**************************************************************************************************************************************** 
        --Remove all records in the Students table with no StudentCourseUserKey 
        --**************************************************************************************************************************************** 
        DELETE FROM LS_ODS.Students
        WHERE StudentCourseUserKeys IS NULL;

        EXEC LS_ODS.AddODSLoadLog 'Removed Student Records With No Valid StudentCourseUserKey Value',
                                  0;

        --**************************************************************************************************************************************** 
        --Handle Grade records with negative primary keys 
        --These come from Documents, Weekly Roadmaps, and various other "assignments" that are not true assignments. 
        --The negative value appears because the assignment has not been released to the student for use (adaptive release). 
        --We do not need to report on these value so we can just delete them from the database. 
        --**************************************************************************************************************************************** 
        DELETE FROM LS_ODS.Grades
        WHERE GradePrimaryKey < 0
              AND GradePrimaryKey NOT
              BETWEEN -514999999 AND -514000000;

        EXEC LS_ODS.AddODSLoadLog 'Removed Grade Records With Negative Primary Keys',
                                  0;

        --**************************************************************************************************************************************** 
        --Process Course Activity Counts for BI Reporting needs 
        --**************************************************************************************************************************************** 
        EXEC LS_ODS.ProcessCourseActivityCounts;

        EXEC LS_ODS.AddODSLoadLog 'Processed Course Activity Counts', 0;

        --**************************************************************************************************************************************** 
        --Create a distinct list of all courses to ensure any course no longer in the GradeExtract is disabled 
        --**************************************************************************************************************************************** 
        -- Create necessary indexes if they don't exist
		IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_Courses_ActiveFlag')
		BEGIN
			CREATE NONCLUSTERED INDEX idx_Courses_ActiveFlag
			ON LS_ODS.Courses (ActiveFlag);
		END

		-- Declare table variable
		DECLARE @DisabledCourses TABLE
		(
			CoursePrimaryKey INT,
			AdClassSchedId INT
		);

		-- Common Table Expressions (CTEs)
		WITH cActiveCourses AS (
			SELECT DISTINCT
				CAST(gei.CoursePK1 AS INT) AS CoursePrimaryKey,
				CAST(REPLACE(gei.CourseEPK, 'AdCourse_', '') AS INT) AS AdClassSchedId
			FROM stage.GradeExtractImport gei
			WHERE LEFT(gei.UserEPK, 9) = 'SyStudent'
				AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8
				AND LEFT(gei.CourseEPK, 8) = 'AdCourse'
				AND (
					  gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%'
					  OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%'
					  OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%'
					  OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%'
				   )
				AND gei.UserFirstName NOT LIKE 'BBAFL%'
				AND gei.UserEPK NOT LIKE '%PART1%'
				AND gei.UserEPK NOT LIKE '%PART2%'
				AND gei.UserEPK NOT LIKE '%PART3%'
				AND gei.UserEPK NOT LIKE '%PART4%'
				AND gei.USEREPK NOT LIKE '%PART5%'
		),
		cAllCourses AS (
			SELECT DISTINCT
				c.CoursePrimaryKey,
				c.AdClassSchedId
			FROM LS_ODS.Courses c
			WHERE c.ActiveFlag = 1
		)

		-- Insert disabled courses into table variable
		INSERT INTO @DisabledCourses (CoursePrimaryKey, AdClassSchedId)
		SELECT ac.CoursePrimaryKey, ac.AdClassSchedId
		FROM cAllCourses ac
		WHERE NOT EXISTS (
			SELECT 1
			FROM cActiveCourses acc
			WHERE ac.AdClassSchedId = acc.AdClassSchedId
			AND ac.CoursePrimaryKey = acc.CoursePrimaryKey
		);

		-- Update ActiveFlag in LS_ODS.Courses
		UPDATE c
		SET c.ActiveFlag = 0
		FROM LS_ODS.Courses c
		WHERE EXISTS (
			SELECT 1
			FROM @DisabledCourses dc
			WHERE c.CoursePrimaryKey = dc.CoursePrimaryKey
			AND c.AdClassSchedId = dc.AdClassSchedId
		);

		-- Create index after updates if necessary
		CREATE NONCLUSTERED INDEX idx_Courses_ActiveFlag
		ON LS_ODS.Courses (ActiveFlag);


        EXEC LS_ODS.AddODSLoadLog 'Removed Disable Courses', 0;

        --**************************************************************************************************************************************** 
        --Create a distinct list of all student/section combinations to ensure any student moved from one section to another has the old section disabled 
        --**************************************************************************************************************************************** 
-- Create necessary indexes if they don't exist
		IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_Students_ActiveFlag')
		BEGIN
			CREATE NONCLUSTERED INDEX idx_Students_ActiveFlag
			ON LS_ODS.Students (ActiveFlag);
		END

		-- Declare table variable
		DECLARE @DisabledStudentCourseCombinations TABLE
		(
			SyStudentId INT,
			AdEnrollSchedId INT,
			AdClassSchedId INT
		);

		-- Common Table Expressions (CTEs)
		WITH cActiveStudentCourseCombinations AS (
			SELECT DISTINCT
				REPLACE(gei.UserEPK, 'SyStudent_', '') AS SyStudentId,
				CAST(CAST(es.AdEnrollSchedID AS VARCHAR(100)) AS INT) AS AdEnrollSchedId,
				CAST(REPLACE(gei.CourseEPK, 'AdCourse_', '') AS INT) AS AdClassSchedId
			FROM stage.GradeExtractImport gei
			LEFT JOIN CV_Prod.dbo.AdEnrollSched es
				ON REPLACE(gei.CourseEPK, 'AdCourse_', '') = CAST(es.AdClassSchedID AS VARCHAR(50))
				   AND REPLACE(gei.UserEPK, 'SyStudent_', '') = CAST(es.SyStudentID AS VARCHAR(50))
			LEFT JOIN CV_Prod.dbo.AdClassSched cs
				ON CAST(REPLACE(gei.CourseEPK, 'AdCourse_', '') AS INT) = cs.AdClassSchedID
			WHERE LEFT(gei.UserEPK, 9) = 'SyStudent'
				AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8
				AND LEFT(gei.CourseEPK, 8) = 'AdCourse'
				AND (
					  gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%'
					  OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%'
					  OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%'
					  OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%'
				   )
				AND gei.UserFirstName NOT LIKE 'BBAFL%'
				AND gei.UserEPK NOT LIKE '%PART1%'
				AND gei.UserEPK NOT LIKE '%PART2%'
				AND gei.UserEPK NOT LIKE '%PART3%'
				AND gei.UserEPK NOT LIKE '%PART4%'
				AND gei.USEREPK NOT LIKE '%PART5%'
		),
		cAllStudentCourseCombinations AS (
			SELECT DISTINCT
				s.SyStudentId,
				s.AdEnrollSchedId,
				s.AdClassSchedId
			FROM LS_ODS.Students s
			WHERE s.ActiveFlag = 1
		)

		-- Insert disabled student course combinations into table variable
		INSERT INTO @DisabledStudentCourseCombinations (SyStudentId, AdEnrollSchedId, AdClassSchedId)
		SELECT cAllStudentsCourses.SyStudentId,
			   cAllStudentsCourses.AdEnrollSchedId,
			   cAllStudentsCourses.AdClassSchedId
		FROM cAllStudentCourseCombinations cAllStudentsCourses
		WHERE NOT EXISTS (
			SELECT 1
			FROM cActiveStudentCourseCombinations cActiveStudentsCourses
			WHERE cAllStudentsCourses.SyStudentId = cActiveStudentsCourses.SyStudentId
				AND cAllStudentsCourses.AdEnrollSchedId = cActiveStudentsCourses.AdEnrollSchedId
				AND cAllStudentsCourses.AdClassSchedId <> cActiveStudentsCourses.AdClassSchedId
		);

		-- Update ActiveFlag in LS_ODS.Students
		UPDATE s
		SET s.ActiveFlag = 0
		FROM LS_ODS.Students s
		WHERE EXISTS (
			SELECT 1
			FROM @DisabledStudentCourseCombinations dssc
			WHERE s.SyStudentId = dssc.SyStudentId
				AND s.AdEnrollSchedId = dssc.AdEnrollSchedId
				AND s.AdClassSchedId = dssc.AdClassSchedId
		);

		-- Create index after updates if necessary
		CREATE NONCLUSTERED INDEX idx_Students_ActiveFlag
		ON LS_ODS.Students (ActiveFlag);
        EXEC LS_ODS.AddODSLoadLog 'Removed Disabled Student/Course Combinations',
                                  0;

        --**************************************************************************************************************************************** 
        --Disable all students with no matching CampusVue Enrollment records 
        --**************************************************************************************************************************************** 
        IF OBJECT_ID('tempdb..#NonMatchedStudents') IS NOT NULL
            DROP TABLE #NonMatchedStudents;

        CREATE TABLE #NonMatchedStudents
        (
            SyStudentId INT,
            AdEnrollSchedId INT,
            AdClassSchedId INT
        );

        INSERT INTO #NonMatchedStudents
        (
            SyStudentId,
            AdEnrollSchedId,
            AdClassSchedId
        )
        SELECT s.SyStudentId,
               s.AdEnrollSchedId,
               s.AdClassSchedId
        FROM LS_ODS.Students s;

        DELETE s
        FROM #NonMatchedStudents s
            INNER JOIN CV_Prod.dbo.AdEnrollSched es
                ON s.SyStudentId = es.SyStudentId
                   AND s.AdEnrollSchedId = es.AdEnrollSchedId
                   AND s.AdClassSchedId = es.AdClassSchedId;

        UPDATE s
        SET ActiveFlag = 0
        FROM LS_ODS.Students s
            INNER JOIN #NonMatchedStudents s2
                ON s.SyStudentId = s2.SyStudentId
                   AND s.AdEnrollSchedId = s2.AdEnrollSchedId
                   AND s.AdClassSchedId = s2.AdClassSchedId;

        EXEC LS_ODS.AddODSLoadLog 'Removed Students With No CampusVue Enrollment Records',
                                  0;

        --**************************************************************************************************************************************** 
        --Move old Students records to Audit table 
        --**************************************************************************************************************************************** 
        INSERT INTO Archive.Students
        (
            StudentPrimaryKey,
            DateTimeCreated,
            DateTimeModified,
            RowStatus,
            BatchUniqueIdentifier,
            BlackboardUsername,
            SyStudentId,
            FirstName,
            LastName,
            Campus,
            AdEnrollSchedId,
            AdClassSchedId,
            CourseUsersPrimaryKey,
            LastLoginDateTime,
            TimeInClass,
            LastI3InteractionNumberMainPhone,
            LastI3InteractionDateTimeMainPhone,
            DaysSinceLastI3InteractionMainPhone,
            LastI3InteractionNumberWorkPhone,
            LastI3InteractionDateTimeWorkPhone,
            DaysSinceLastI3InteractionWorkPhone,
            LastI3InteractionNumberMobilePhone,
            LastI3InteractionDateTimeMobilePhone,
            DaysSinceLastI3InteractionMobilePhone,
            LastI3InteractionNumberOtherPhone,
            LastI3InteractionDateTimeOtherPhone,
            DaysSinceLastI3InteractionOtherPhone,
            Week1Grade,
            Week2Grade,
            Week3Grade,
            Week4Grade,
            Week5Grade,
            SelfTestsCount,
            AssessmentsCount,
            AssignmentsCount,
            DiscussionsCount,
            Week1CompletionRate,
            Week2CompletionRate,
            Week3CompletionRate,
            Week4CompletionRate,
            Week5CompletionRate,
            ActiveFlag,
            UMADateTimeAdded,
            VAStudent,
            NoticeName,
            NoticeDueDate,
            VABenefitName,
            ClassStatus,
            ODSPrimaryKey,
            Week1LDA,
            Week2LDA,
            Week3LDA,
            Week4LDA,
            Week5LDA,
            Week1CompletedAssignments,
            Week2CompletedAssignments,
            Week3CompletedAssignments,
            Week4CompletedAssignments,
            Week5CompletedAssignments,
            CoursePercentage,
            TotalWorkPercentage,
            AdEnrollId,
            IsRetake,
            StudentCourseUserKeys,
            CurrentCourseGrade,
            ProgramCode,
            ProgramName,
            ProgramVersionCode,
            ProgramVersionName,
            MondayTimeInClass,
            TuesdayTimeInClass,
            WednesdayTimeInClass,
            ThursdayTimeInClass,
            FridayTimeInClass,
            SaturdayTimeInClass,
            SundayTimeInClass,
            Week1CompletionRateFixed,
            Week2CompletionRateFixed,
            Week3CompletionRateFixed,
            Week4CompletionRateFixed,
            Week5CompletionRateFixed,
            StudentNumber,
            IsOrphanRecord,
            SourceSystem
        )
        SELECT s.StudentPrimaryKey,
               s.DateTimeCreated,
               s.DateTimeModified,
               s.RowStatus,
               s.BatchUniqueIdentifier,
               s.BlackboardUsername,
               s.SyStudentId,
               s.FirstName,
               s.LastName,
               s.Campus,
               s.AdEnrollSchedId,
               s.AdClassSchedId,
               s.CourseUsersPrimaryKey,
               s.LastLoginDateTime,
               s.TimeInClass,
               s.LastI3InteractionNumberMainPhone,
               s.LastI3InteractionDateTimeMainPhone,
               s.DaysSinceLastI3InteractionMainPhone,
               s.LastI3InteractionNumberWorkPhone,
               s.LastI3InteractionDateTimeWorkPhone,
               s.DaysSinceLastI3InteractionWorkPhone,
               s.LastI3InteractionNumberMobilePhone,
               s.LastI3InteractionDateTimeMobilePhone,
               s.DaysSinceLastI3InteractionMobilePhone,
               s.LastI3InteractionNumberOtherPhone,
               s.LastI3InteractionDateTimeOtherPhone,
               s.DaysSinceLastI3InteractionOtherPhone,
               s.Week1Grade,
               s.Week2Grade,
               s.Week3Grade,
               s.Week4Grade,
               s.Week5Grade,
               s.SelfTestsCount,
               s.AssessmentsCount,
               s.AssignmentsCount,
               s.DiscussionsCount,
               s.Week1CompletionRate,
               s.Week2CompletionRate,
               s.Week3CompletionRate,
               s.Week4CompletionRate,
               s.Week5CompletionRate,
               s.ActiveFlag,
               s.UMADateTimeAdded,
               s.VAStudent,
               s.NoticeName,
               s.NoticeDueDate,
               s.VABenefitName,
               s.ClassStatus,
               s.ODSPrimaryKey,
               s.Week1LDA,
               s.Week2LDA,
               s.Week3LDA,
               s.Week4LDA,
               s.Week5LDA,
               s.Week1CompletedAssignments,
               s.Week2CompletedAssignments,
               s.Week3CompletedAssignments,
               s.Week4CompletedAssignments,
               s.Week5CompletedAssignments,
               s.CoursePercentage,
               s.TotalWorkPercentage,
               s.AdEnrollId,
               s.IsRetake,
               s.StudentCourseUserKeys,
               s.CurrentCourseGrade,
               s.ProgramCode,
               s.ProgramName,
               s.ProgramVersionCode,
               s.ProgramVersionName,
               s.MondayTimeInClass,
               s.TuesdayTimeInClass,
               s.WednesdayTimeInClass,
               s.ThursdayTimeInClass,
               s.FridayTimeInClass,
               s.SaturdayTimeInClass,
               s.SundayTimeInClass,
               s.Week1CompletionRateFixed,
               s.Week2CompletionRateFixed,
               s.Week3CompletionRateFixed,
               s.Week4CompletionRateFixed,
               s.Week5CompletionRateFixed,
               s.StudentNumber,
               0,
               s.SourceSystem
        FROM LS_ODS.Students s
        WHERE s.ActiveFlag = 0;

        DELETE FROM LS_ODS.Students
        WHERE ActiveFlag = 0;

        EXEC LS_ODS.AddODSLoadLog 'Moved Old Student Records To Archive Table', 0;

        --**************************************************************************************************************************************** 
        --Move old Courses records to Audit table 
        --**************************************************************************************************************************************** 
        INSERT INTO Archive.Courses
        (
            CoursePrimaryKey,
            DateTimeCreated,
            DateTimeModified,
            RowStatus,
            BatchUniqueIdentifier,
            CourseCode,
            CourseName,
            SectionNumber,
            SectionStart,
            SectionEnd,
            AdClassSchedId,
            WeekNumber,
            Week1AssignmentCount,
            Week2AssignmentCount,
            Week3AssignmentCount,
            Week4AssignmentCount,
            Week5AssignmentCount,
            ActiveFlag,
            UMADateTimeAdded,
            ODSPrimaryKey,
            PrimaryInstructor,
            SecondaryInstructor,
            Week1StartDate,
            Week2StartDate,
            Week3StartDate,
            Week4StartDate,
            Week5StartDate,
            ExtensionWeekStartDate,
            IsOrganziation,
            AcademicFacilitator,
            PrimaryInstructorId,
            SecondaryInstructorId,
            AcademicFacilitatorId,
            DayNumber,
            CengageCourseIndicator,
            SourceSystem
        )
        SELECT c.CoursePrimaryKey,
               c.DateTimeCreated,
               c.DateTimeModified,
               c.RowStatus,
               c.BatchUniqueIdentifier,
               c.CourseCode,
               c.CourseName,
               c.SectionNumber,
               c.SectionStart,
               c.SectionEnd,
               c.AdClassSchedId,
               c.WeekNumber,
               c.Week1AssignmentCount,
               c.Week2AssignmentCount,
               c.Week3AssignmentCount,
               c.Week4AssignmentCount,
               c.Week5AssignmentCount,
               c.ActiveFlag,
               c.UMADateTimeAdded,
               c.ODSPrimaryKey,
               c.PrimaryInstructor,
               c.SecondaryInstructor,
               c.Week1StartDate,
               c.Week2StartDate,
               c.Week3StartDate,
               c.Week4StartDate,
               c.Week5StartDate,
               c.ExtensionWeekStartDate,
               c.IsOrganization,
               c.AcademicFacilitator,
               c.PrimaryInstructorId,
               c.SecondaryInstructorId,
               c.AcademicFacilitatorId,
               c.DayNumber,
               c.CengageCourseIndicator,
               c.SourceSystem
        FROM LS_ODS.Courses c
        WHERE c.ActiveFlag = 0;

        DELETE FROM LS_ODS.Courses
        WHERE ActiveFlag = 0;

        EXEC LS_ODS.AddODSLoadLog 'Moved Old Course Records To Archive Table', 0;

        --**************************************************************************************************************************************** 
        --Move old Assignments records to Audit table 
        --**************************************************************************************************************************************** 
        INSERT INTO Archive.Assignments
        (
            AssignmentPrimaryKey,
            CoursePrimaryKey,
            WeekNumber,
            AssignmentTitle,
            DueDate,
            PossiblePoints,
            DateTimeCreated,
            DateTimeModified,
            ScoreProviderHandle,
            ActiveFlag,
            UMADateTimeAdded,
            CourseContentsPrimaryKey1,
            ODSPrimaryKey,
            AlternateTitle,
            IsReportable,
            CountsAsSubmission,
            AssignmentType,
            SourceSystem
        )
        SELECT a.AssignmentPrimaryKey,
               a.CoursePrimaryKey,
               a.WeekNumber,
               a.AssignmentTitle,
               a.DueDate,
               a.PossiblePoints,
               a.DateTimeCreated,
               a.DateTimeModified,
               a.ScoreProviderHandle,
               a.ActiveFlag,
               a.UMADateTimeAdded,
               a.CourseContentsPrimaryKey1,
               a.ODSPrimaryKey,
               a.AlternateTitle,
               a.IsReportable,
               a.CountsAsSubmission,
               a.AssignmentType,
               a.SourceSystem
        FROM LS_ODS.Assignments a
        WHERE a.ActiveFlag = 0;

        DELETE FROM LS_ODS.Assignments
        WHERE ActiveFlag = 0;

        EXEC LS_ODS.AddODSLoadLog 'Moved Old Assignments Records To Archive Table',
                                  0;


        ----**************************************************************************************************************************************** 
        ----Move old Grades records to Audit table 

        ----**************************************************************************************************************************************** 

        --Performance Fix for LS_ODS Grades Table CRUD; Index Balancing
        IF EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'idx_LSiDashActiveFlagLargeIncludeDESC'
        )
        BEGIN
            DROP INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades;
        END;

        IF EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'idx_LSiDashAssignmentPrimaryKeyDESC'
        )
        BEGIN
            DROP INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades;
        END;

        IF EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC'
        )
        BEGIN
            DROP INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC
            ON LS_ODS.Grades;
        END;

        IF EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'idx_LSiDashCourseUsersPrimaryKeyDESC'
        )
        BEGIN
            DROP INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades;
        END;

        IF EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'idx_CourseUsersPKAssignPKActiveFG'
        )
        BEGIN
            DROP INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades;
        END;

        IF EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = '_dta_index_Grades_10_1223675407__K2_K23_K21_4'
        )
        BEGIN
            DROP INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades;
        END;

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_ODS_010')
        BEGIN
            DROP INDEX idx_ODS_010 ON LS_ODS.Grades;
        END;


        --Merge into audit table     

        DROP TABLE IF EXISTS #LSODSGRADE

        SELECT *
        INTO #LSODSGRADE
        FROM LS_ODS.Grades
        WHERE ActiveFlag = 0

        DELETE FROM #LSODSGRADE
        WHERE ODSPrimaryKey IN (
                                   SELECT ODSPrimaryKey FROM Audit.Grades
                               )

        MERGE INTO audit.Grades AS trg
        USING #LSODSGRADE AS src
        ON src.GradePrimaryKey = trg.GradePrimaryKey
           AND src.CourseUsersPrimaryKey = trg.CourseUsersPrimaryKey
           AND src.HighestScore = trg.HighestScore
           AND src.HighestGrade = trg.HighestGrade
           AND src.AssignmentPrimaryKey = trg.AssignmentPrimaryKey
           AND src.RowStatus = trg.RowStatus
           AND src.AssignmentStatus = trg.AssignmentStatus
        WHEN NOT MATCHED BY TARGET AND src.ActiveFlag = 0 THEN
            INSERT
            (
                [GradePrimaryKey],
                [CourseUsersPrimaryKey],
                [RowStatus],
                [HighestScore],
                [HighestGrade],
                [HighestAttemptDateTime],
                [ManualScore],
                [ManualGrade],
                [ManualDateTime],
                [ExemptIndicator],
                [HighestDateTimeCreated],
                [HighestDateTimeModified],
                [HighestIsLatestAttemptIndicator],
                [NumberOfAttempts],
                [FirstScore],
                [FirstGrade],
                [FirstAttemptDateTime],
                [FirstIsLatestAttemptIndicator],
                [FirstDateTimeCreated],
                [FirstDateTimeModified],
                [AssignmentPrimaryKey],
                [AssignmentStatus],
                [ActiveFlag],
                [UMADateTimeAdded],
                [ODSPrimaryKey],
                SourceSystem
            )
            VALUES
            (src.[GradePrimaryKey],
             src.[CourseUsersPrimaryKey],
             src.[RowStatus],
             src.[HighestScore],
             src.[HighestGrade],
             src.[HighestAttemptDateTime],
             src.[ManualScore],
             src.[ManualGrade],
             src.[ManualDateTime],
             src.[ExemptIndicator],
             src.[HighestDateTimeCreated],
             src.[HighestDateTimeModified],
             src.[HighestIsLatestAttemptIndicator],
             src.[NumberOfAttempts],
             src.[FirstScore],
             src.[FirstGrade],
             src.[FirstAttemptDateTime],
             src.[FirstIsLatestAttemptIndicator],
             src.[FirstDateTimeCreated],
             src.[FirstDateTimeModified],
             src.[AssignmentPrimaryKey],
             src.[AssignmentStatus],
             src.[ActiveFlag],
             src.[UMADateTimeAdded],
             src.[ODSPrimaryKey],
             src.SourceSystem
            )
        WHEN MATCHED THEN
            UPDATE SET trg.[GradePrimaryKey] = src.[GradePrimaryKey],
                       trg.[CourseUsersPrimaryKey] = src.[CourseUsersPrimaryKey],
                       trg.[RowStatus] = src.[RowStatus],
                       trg.[HighestScore] = src.[HighestScore],
                       trg.[HighestGrade] = src.[HighestGrade],
                       trg.[HighestAttemptDateTime] = src.[HighestAttemptDateTime],
                       trg.[ManualScore] = src.[ManualScore],
                       trg.[ManualGrade] = src.[ManualGrade],
                       trg.[ManualDateTime] = src.[ManualDateTime],
                       trg.[ExemptIndicator] = src.[ExemptIndicator],
                       trg.[HighestDateTimeCreated] = src.[HighestDateTimeCreated],
                       trg.[HighestDateTimeModified] = src.[HighestDateTimeModified],
                       trg.[HighestIsLatestAttemptIndicator] = src.[HighestIsLatestAttemptIndicator],
                       trg.[NumberOfAttempts] = src.[NumberOfAttempts],
                       trg.[FirstScore] = src.[FirstScore],
                       trg.[FirstGrade] = src.[FirstGrade],
                       trg.[FirstAttemptDateTime] = src.[FirstAttemptDateTime],
                       trg.[FirstIsLatestAttemptIndicator] = src.[FirstIsLatestAttemptIndicator],
                       trg.[FirstDateTimeCreated] = src.[FirstDateTimeCreated],
                       trg.[FirstDateTimeModified] = src.[FirstDateTimeModified],
                       trg.[AssignmentPrimaryKey] = src.[AssignmentPrimaryKey],
                       trg.[AssignmentStatus] = src.[AssignmentStatus],
                       trg.[ActiveFlag] = src.[ActiveFlag],
                       trg.[UMADateTimeAdded] = src.[UMADateTimeAdded],
                       trg.[ODSPrimaryKey] = src.[ODSPrimaryKey],
                       trg.SourceSystem = src.SourceSystem;


        DELETE FROM LS_ODS.Grades
        WHERE ActiveFlag = 0;



        --Performance Fix for LS_ODS Grades Table CRUD; Index Balancing
        CREATE NONCLUSTERED INDEX idx_ODS_010
        ON LS_ODS.Grades
        (
            GradePrimaryKey ASC,
            ActiveFlag ASC
        )
        INCLUDE
        (
            CourseUsersPrimaryKey,
            RowStatus,
            HighestScore,
            HighestGrade,
            HighestAttemptDateTime,
            ManualScore,
            ManualGrade,
            ManualDateTime,
            ExemptIndicator,
            HighestDateTimeCreated,
            HighestDateTimeModified,
            HighestIsLatestAttemptIndicator,
            NumberOfAttempts,
            FirstScore,
            FirstGrade,
            FirstAttemptDateTime,
            FirstIsLatestAttemptIndicator,
            FirstDateTimeModified,
            AssignmentPrimaryKey,
            AssignmentStatus,
            SourceSystem
        )
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
             ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4
        ON LS_ODS.Grades
        (
            CourseUsersPrimaryKey ASC,
            ActiveFlag ASC,
            AssignmentPrimaryKey ASC
        )
        INCLUDE (HighestScore)
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
             ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX idx_LSiDashCourseUsersPrimaryKeyDESC
        ON LS_ODS.Grades (CourseUsersPrimaryKey DESC)
        INCLUDE
        (
            GradePrimaryKey,
            AssignmentPrimaryKey,
            ActiveFlag,
            AssignmentStatus,
            HighestGrade,
            HighestScore,
            HighestAttemptDateTime,
            ManualScore,
            ManualGrade,
            HighestDateTimeCreated,
            NumberOfAttempts
        )
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95
             ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC
        ON LS_ODS.Grades
        (
            CourseUsersPrimaryKey DESC,
            AssignmentPrimaryKey DESC,
            ActiveFlag DESC
        )
        INCLUDE
        (
            GradePrimaryKey,
            HighestScore,
            HighestDateTimeCreated,
            AssignmentStatus
        )
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95
             ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX idx_LSiDashAssignmentPrimaryKeyDESC
        ON LS_ODS.Grades (AssignmentPrimaryKey DESC)
        INCLUDE
        (
            GradePrimaryKey,
            CourseUsersPrimaryKey,
            ActiveFlag,
            AssignmentStatus,
            HighestGrade,
            HighestScore,
            HighestAttemptDateTime,
            ManualScore,
            ManualGrade,
            HighestDateTimeCreated,
            NumberOfAttempts
        )
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95
             ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX idx_LSiDashActiveFlagLargeIncludeDESC
        ON LS_ODS.Grades (ActiveFlag DESC)
        INCLUDE
        (
            GradePrimaryKey,
            AssignmentPrimaryKey,
            CourseUsersPrimaryKey,
            AssignmentStatus,
            HighestGrade,
            HighestScore,
            HighestAttemptDateTime,
            ManualScore,
            ManualGrade,
            HighestDateTimeCreated,
            NumberOfAttempts
        )
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95
             ) ON [PRIMARY];

        CREATE NONCLUSTERED INDEX idx_CourseUsersPKAssignPKActiveFG
        ON LS_ODS.Grades
        (
            CourseUsersPrimaryKey ASC,
            AssignmentPrimaryKey ASC,
            ActiveFlag ASC
        )
        INCLUDE
        (
            GradePrimaryKey,
            HighestScore,
            HighestGrade,
            HighestAttemptDateTime,
            ManualScore,
            ManualGrade,
            HighestDateTimeCreated,
            NumberOfAttempts,
            AssignmentStatus
        )
        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF,
              ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON
             ) ON [PRIMARY];

        EXEC LS_ODS.AddODSLoadLog 'Moved Old Grades Records To Archive Table', 0;



        --If Weekly Course Graded Activity and Weekly Course Grades steps are running after 9am, we should let LS know.


        --**************************************************************************************************************************************** 
        --Remove all duplicates from each of the ODS tables 
        --**************************************************************************************************************************************** 
        WITH cteStudent
        AS (SELECT s.StudentPrimaryKey,
                   s.DateTimeCreated,
                   s.DateTimeModified,
                   s.RowStatus,
                   s.BatchUniqueIdentifier,
                   s.BlackboardUsername,
                   s.SyStudentId,
                   s.FirstName,
                   s.LastName,
                   s.Campus,
                   s.AdEnrollSchedId,
                   s.AdClassSchedId,
                   s.CourseUsersPrimaryKey,
                   s.LastLoginDateTime,
                   s.TimeInClass,
                   s.LastI3InteractionNumberMainPhone,
                   s.LastI3InteractionDateTimeMainPhone,
                   s.DaysSinceLastI3InteractionMainPhone,
                   s.LastI3InteractionNumberWorkPhone,
                   s.LastI3InteractionDateTimeWorkPhone,
                   s.DaysSinceLastI3InteractionWorkPhone,
                   s.LastI3InteractionNumberMobilePhone,
                   s.LastI3InteractionDateTimeMobilePhone,
                   s.DaysSinceLastI3InteractionMobilePhone,
                   s.LastI3InteractionNumberOtherPhone,
                   s.LastI3InteractionDateTimeOtherPhone,
                   s.DaysSinceLastI3InteractionOtherPhone,
                   s.Week1Grade,
                   s.Week2Grade,
                   s.Week3Grade,
                   s.Week4Grade,
                   s.Week5Grade,
                   s.SelfTestsCount,
                   s.AssessmentsCount,
                   s.AssignmentsCount,
                   s.DiscussionsCount,
                   s.ActivitiesCount,
                   s.Week1CompletionRate,
                   s.Week2CompletionRate,
                   s.Week3CompletionRate,
                   s.Week4CompletionRate,
                   s.Week5CompletionRate,
                   s.ActiveFlag,
                   s.UMADateTimeAdded,
                   s.VAStudent,
                   s.NoticeName,
                   s.NoticeDueDate,
                   s.VABenefitName,
                   s.ClassStatus,
                   s.Week1LDA,
                   s.Week2LDA,
                   s.Week3LDA,
                   s.Week4LDA,
                   s.Week5LDA,
                   s.Week1CompletedAssignments,
                   s.Week2CompletedAssignments,
                   s.Week3CompletedAssignments,
                   s.Week4CompletedAssignments,
                   s.Week5CompletedAssignments,
                   s.CoursePercentage,
                   s.TotalWorkPercentage,
                   s.AdEnrollId,
                   s.IsRetake,
                   s.StudentCourseUserKeys,
                   s.CurrentCourseGrade,
                   s.ProgramCode,
                   s.ProgramName,
                   s.ProgramVersionCode,
                   s.ProgramVersionName,
                   s.MondayTimeInClass,
                   s.TuesdayTimeInClass,
                   s.WednesdayTimeInClass,
                   s.ThursdayTimeInClass,
                   s.FridayTimeInClass,
                   s.SaturdayTimeInClass,
                   s.SundayTimeInClass,
                   s.Week1CompletionRateFixed,
                   s.Week2CompletionRateFixed,
                   s.Week3CompletionRateFixed,
                   s.Week4CompletionRateFixed,
                   s.Week5CompletionRateFixed,
                   s.StudentNumber,
                   s.SourceSystem,
                   ROW_NUMBER() OVER (PARTITION BY s.StudentPrimaryKey,
                                                   s.DateTimeCreated,
                                                   s.DateTimeModified,
                                                   s.RowStatus,
                                                   s.BatchUniqueIdentifier,
                                                   s.BlackboardUsername,
                                                   s.SyStudentId,
                                                   s.FirstName,
                                                   s.LastName,
                                                   s.Campus,
                                                   s.AdEnrollSchedId,
                                                   s.AdClassSchedId,
                                                   s.CourseUsersPrimaryKey,
                                                   s.LastLoginDateTime,
                                                   s.TimeInClass,
                                                   s.LastI3InteractionNumberMainPhone,
                                                   s.LastI3InteractionDateTimeMainPhone,
                                                   s.DaysSinceLastI3InteractionMainPhone,
                                                   s.LastI3InteractionNumberWorkPhone,
                                                   s.LastI3InteractionDateTimeWorkPhone,
                                                   s.DaysSinceLastI3InteractionWorkPhone,
                                                   s.LastI3InteractionNumberMobilePhone,
                                                   s.LastI3InteractionDateTimeMobilePhone,
                                                   s.DaysSinceLastI3InteractionMobilePhone,
                                                   s.LastI3InteractionNumberOtherPhone,
                                                   s.LastI3InteractionDateTimeOtherPhone,
                                                   s.DaysSinceLastI3InteractionOtherPhone,
                                                   s.Week1Grade,
                                                   s.Week2Grade,
                                                   s.Week3Grade,
                                                   s.Week4Grade,
                                                   s.Week5Grade,
                                                   s.SelfTestsCount,
                                                   s.AssessmentsCount,
                                                   s.AssignmentsCount,
                                                   s.DiscussionsCount,
                                                   s.ActivitiesCount,
                                                   s.Week1CompletionRate,
                                                   s.Week2CompletionRate,
                                                   s.Week3CompletionRate,
                                                   s.Week4CompletionRate,
                                                   s.Week5CompletionRate,
                                                   s.ActiveFlag,
                                                   s.UMADateTimeAdded,
                                                   s.VAStudent,
                                                   s.NoticeName,
                                                   s.NoticeDueDate,
                                                   s.VABenefitName,
                                                   s.ClassStatus,
                                                   s.Week1LDA,
                                                   s.Week2LDA,
                                                   s.Week3LDA,
                                                   s.Week4LDA,
                                                   s.Week5LDA,
                                                   s.Week1CompletedAssignments,
                                                   s.Week2CompletedAssignments,
                                                   s.Week3CompletedAssignments,
                                                   s.Week4CompletedAssignments,
                                                   s.Week5CompletedAssignments,
                                                   s.CoursePercentage,
                                                   s.TotalWorkPercentage,
                                                   s.AdEnrollId,
                                                   s.IsRetake,
                                                   s.StudentCourseUserKeys,
                                                   s.CurrentCourseGrade,
                                                   s.ProgramCode,
                                                   s.ProgramName,
                                                   s.ProgramVersionCode,
                                                   s.ProgramVersionName,
                                                   s.MondayTimeInClass,
                                                   s.TuesdayTimeInClass,
                                                   s.WednesdayTimeInClass,
                                                   s.ThursdayTimeInClass,
                                                   s.FridayTimeInClass,
                                                   s.SaturdayTimeInClass,
                                                   s.SundayTimeInClass,
                                                   s.Week1CompletionRateFixed,
                                                   s.Week2CompletionRateFixed,
                                                   s.Week3CompletionRateFixed,
                                                   s.Week4CompletionRateFixed,
                                                   s.Week5CompletionRateFixed,
                                                   s.StudentNumber,
                                                   s.SourceSystem
                                      ORDER BY s.UMADateTimeAdded
                                     ) 'RowNumber'
            FROM LS_ODS.Students s
           )
        DELETE cteStudent
        WHERE RowNumber > 1;

        EXEC LS_ODS.AddODSLoadLog 'Student Duplicate Check And Deletion Complete',
                                  0;

       -- Create necessary indexes if they don't exist
			IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_Courses_CoursePrimaryKey')
			BEGIN
				CREATE NONCLUSTERED INDEX idx_Courses_CoursePrimaryKey
				ON LS_ODS.Courses (CoursePrimaryKey);
			END

			-- Common Table Expression (CTE) with ROW_NUMBER() and DELETE using EXISTS
			WITH cteToDelete AS (
				SELECT c.CoursePrimaryKey,
					   c.DateTimeCreated,
					   c.DateTimeModified,
					   c.RowStatus,
					   c.BatchUniqueIdentifier,
					   c.CourseCode,
					   c.CourseName,
					   c.SectionNumber,
					   c.SectionStart,
					   c.SectionEnd,
					   c.AdClassSchedId,
					   c.WeekNumber,
					   c.Week1AssignmentCount,
					   c.Week2AssignmentCount,
					   c.Week3AssignmentCount,
					   c.Week4AssignmentCount,
					   c.Week5AssignmentCount,
					   c.TotalAssignmentCount,
					   c.ActiveFlag,
					   c.UMADateTimeAdded,
					   c.PrimaryInstructor,
					   c.SecondaryInstructor,
					   c.Week1StartDate,
					   c.Week2StartDate,
					   c.Week3StartDate,
					   c.Week4StartDate,
					   c.Week5StartDate,
					   c.ExtensionWeekStartDate,
					   c.IsOrganization,
					   c.AcademicFacilitator,
					   c.PrimaryInstructorId,
					   c.SecondaryInstructorId,
					   c.AcademicFacilitatorId,
					   c.DayNumber,
					   c.CengageCourseIndicator,
					   c.SourceSystem,
					   ROW_NUMBER() OVER (
						   PARTITION BY c.CoursePrimaryKey,
										c.DateTimeCreated,
										c.DateTimeModified,
										c.RowStatus,
										c.BatchUniqueIdentifier,
										c.CourseCode,
										c.CourseName,
										c.SectionNumber,
										c.SectionStart,
										c.SectionEnd,
										c.AdClassSchedId,
										c.WeekNumber,
										c.Week1AssignmentCount,
										c.Week2AssignmentCount,
										c.Week3AssignmentCount,
										c.Week4AssignmentCount,
										c.Week5AssignmentCount,
										c.TotalAssignmentCount,
										c.ActiveFlag,
										c.UMADateTimeAdded,
										c.PrimaryInstructor,
										c.SecondaryInstructor,
										c.Week1StartDate,
										c.Week2StartDate,
										c.Week3StartDate,
										c.Week4StartDate,
										c.Week5StartDate,
										c.ExtensionWeekStartDate,
										c.IsOrganization,
										c.AcademicFacilitator,
										c.PrimaryInstructorId,
										c.SecondaryInstructorId,
										c.AcademicFacilitatorId,
										c.DayNumber,
										c.CengageCourseIndicator,
										c.SourceSystem
						   ORDER BY c.UMADateTimeAdded
					   ) AS RowNumber
				FROM LS_ODS.Courses c
			)

			-- Delete duplicate rows using EXISTS
			DELETE c
			FROM LS_ODS.Courses c
			WHERE EXISTS (
				SELECT 1
				FROM cteToDelete d
				WHERE d.CoursePrimaryKey = c.CoursePrimaryKey
				  AND d.DateTimeCreated = c.DateTimeCreated
				  AND d.DateTimeModified = c.DateTimeModified
				  AND d.RowStatus = c.RowStatus
				  AND d.BatchUniqueIdentifier = c.BatchUniqueIdentifier
				  AND d.CourseCode = c.CourseCode
				  AND d.CourseName = c.CourseName
				  AND d.SectionNumber = c.SectionNumber
				  AND d.SectionStart = c.SectionStart
				  AND d.SectionEnd = c.SectionEnd
				  AND d.AdClassSchedId = c.AdClassSchedId
				  AND d.WeekNumber = c.WeekNumber
				  AND d.Week1AssignmentCount = c.Week1AssignmentCount
				  AND d.Week2AssignmentCount = c.Week2AssignmentCount
				  AND d.Week3AssignmentCount = c.Week3AssignmentCount
				  AND d.Week4AssignmentCount = c.Week4AssignmentCount
				  AND d.Week5AssignmentCount = c.Week5AssignmentCount
				  AND d.TotalAssignmentCount = c.TotalAssignmentCount
				  AND d.ActiveFlag = c.ActiveFlag
				  AND d.UMADateTimeAdded = c.UMADateTimeAdded
				  AND d.PrimaryInstructor = c.PrimaryInstructor
				  AND d.SecondaryInstructor = c.SecondaryInstructor
				  AND d.Week1StartDate = c.Week1StartDate
				  AND d.Week2StartDate = c.Week2StartDate
				  AND d.Week3StartDate = c.Week3StartDate
				  AND d.Week4StartDate = c.Week4StartDate
				  AND d.Week5StartDate = c.Week5StartDate
				  AND d.ExtensionWeekStartDate = c.ExtensionWeekStartDate
				  AND d.IsOrganization = c.IsOrganization
				  AND d.AcademicFacilitator = c.AcademicFacilitator
				  AND d.PrimaryInstructorId = c.PrimaryInstructorId
				  AND d.SecondaryInstructorId = c.SecondaryInstructorId
				  AND d.AcademicFacilitatorId = c.AcademicFacilitatorId
				  AND d.DayNumber = c.DayNumber
				  AND d.CengageCourseIndicator = c.CengageCourseIndicator
				  AND d.SourceSystem = c.SourceSystem
				  AND d.RowNumber > 1
			);

			-- Create index after deletion if necessary
			CREATE NONCLUSTERED INDEX idx_Courses_CoursePrimaryKey
			ON LS_ODS.Courses (CoursePrimaryKey);


        EXEC LS_ODS.AddODSLoadLog 'Course Duplicate Check And Deletion Complete',
                                  0;

        WITH cteAssignment
        AS (SELECT a.AssignmentPrimaryKey,
                   a.CoursePrimaryKey,
                   a.WeekNumber,
                   a.AssignmentTitle,
                   a.DueDate,
                   a.PossiblePoints,
                   a.DateTimeCreated,
                   a.DateTimeModified,
                   a.ScoreProviderHandle,
                   a.ActiveFlag,
                   a.UMADateTimeAdded,
                   a.CourseContentsPrimaryKey1,
                   a.AlternateTitle,
                   a.IsReportable,
                   a.CountsAsSubmission,
                   a.AssignmentType,
                   a.SourceSystem,
                   ROW_NUMBER() OVER (PARTITION BY a.AssignmentPrimaryKey,
                                                   a.CoursePrimaryKey,
                                                   a.WeekNumber,
                                                   a.AssignmentTitle,
                                                   a.DueDate,
                                                   a.PossiblePoints,
                                                   a.DateTimeCreated,
                                                   a.DateTimeModified,
                                                   a.ScoreProviderHandle,
                                                   a.ActiveFlag,
                                                   a.UMADateTimeAdded,
                                                   a.CourseContentsPrimaryKey1,
                                                   a.AlternateTitle,
                                                   a.IsReportable,
                                                   a.CountsAsSubmission,
                                                   a.AssignmentType,
                                                   a.SourceSystem
                                      ORDER BY a.UMADateTimeAdded
                                     ) 'RowNumber'
            FROM LS_ODS.Assignments a
           )
        DELETE FROM cteAssignment
        WHERE RowNumber > 1;

        EXEC LS_ODS.AddODSLoadLog 'Assignment Duplicate Check And Deletion Complete',
                                  0;

        IF EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'idx_LSiDashActiveFlagLargeIncludeDESC'
        )
        BEGIN
            DROP INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades;
        END;

        IF EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'idx_LSiDashAssignmentPrimaryKeyDESC'
        )
        BEGIN
            DROP INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades;
        END;

        IF EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC'
        )
        BEGIN
            DROP INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC
            ON LS_ODS.Grades;
        END;

        IF EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'idx_LSiDashCourseUsersPrimaryKeyDESC'
        )
        BEGIN
            DROP INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades;
        END;

        IF EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'idx_CourseUsersPKAssignPKActiveFG'
        )
        BEGIN
            DROP INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades;
        END;

        IF EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE name = '_dta_index_Grades_10_1223675407__K2_K23_K21_4'
        )
        BEGIN
            DROP INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades;
        END;

        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_ODS_010')
        BEGIN
            DROP INDEX idx_ODS_010 ON LS_ODS.Grades;
        END;



        WITH cteGrade
        AS (SELECT g.GradePrimaryKey,
                   g.CourseUsersPrimaryKey,
                   g.RowStatus,
                   g.HighestScore,
                   g.HighestGrade,
                   g.HighestAttemptDateTime,
                   g.ManualScore,
                   g.ManualGrade,
                   g.ManualDateTime,
                   g.ExemptIndicator,
                   g.HighestDateTimeCreated,
                   g.HighestDateTimeModified,
                   g.HighestIsLatestAttemptIndicator,
                   g.NumberOfAttempts,
                   g.FirstScore,
                   g.FirstGrade,
                   g.FirstAttemptDateTime,
                   g.FirstIsLatestAttemptIndicator,
                   g.FirstDateTimeCreated,
                   g.FirstDateTimeModified,
                   g.AssignmentPrimaryKey,
                   g.AssignmentStatus,
                   g.ActiveFlag,
                   g.UMADateTimeAdded,
                   g.SourceSystem,
                   ROW_NUMBER() OVER (PARTITION BY g.GradePrimaryKey,
                                                   g.CourseUsersPrimaryKey,
                                                   g.RowStatus,
                                                   g.HighestScore,
                                                   g.HighestGrade,
                                                   g.HighestAttemptDateTime,
                                                   g.ManualScore,
                                                   g.ManualGrade,
                                                   g.ManualDateTime,
                                                   g.ExemptIndicator,
                                                   g.HighestDateTimeCreated,
                                                   g.HighestDateTimeModified,
                                                   g.HighestIsLatestAttemptIndicator,
                                                   g.NumberOfAttempts,
                                                   g.FirstScore,
                                                   g.FirstGrade,
                                                   g.FirstAttemptDateTime,
                                                   g.FirstIsLatestAttemptIndicator,
                                                   g.FirstDateTimeCreated,
                                                   g.FirstDateTimeModified,
                                                   g.AssignmentPrimaryKey,
                                                   g.AssignmentStatus,
                                                   g.ActiveFlag,
                                                   g.UMADateTimeAdded,
                                                   g.SourceSystem
                                      ORDER BY g.UMADateTimeAdded
                                     ) 'RowNumber'
            FROM LS_ODS.Grades g
           )
        DELETE FROM cteGrade
        WHERE RowNumber > 1;

        --Set Active Flag For All Grades Records To Active Flag = 0 For Duplicate Check 
        UPDATE LS_ODS.Grades
        SET ActiveFlag = 0;

        --Update The Most Recent Grade Record To Have Active Flag = 1 
       -- Ensure necessary indexes are created if they don't exist
			IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_Grades_CourseUsersPrimaryKey')
			BEGIN
				CREATE NONCLUSTERED INDEX idx_Grades_CourseUsersPrimaryKey
				ON LS_ODS.Grades (CourseUsersPrimaryKey);
			END

			IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_Grades_AssignmentPrimaryKey')
			BEGIN
				CREATE NONCLUSTERED INDEX idx_Grades_AssignmentPrimaryKey
				ON LS_ODS.Grades (AssignmentPrimaryKey);
			END

			IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_Audit_Grades_ODSPrimaryKey')
			BEGIN
				CREATE NONCLUSTERED INDEX idx_Audit_Grades_ODSPrimaryKey
				ON Audit.Grades (ODSPrimaryKey);
			END

			-- Common Table Expression (CTE) to find records to delete from #LSODSGRADE1
			WITH cteToDelete AS (
				SELECT g.ODSPrimaryKey
				FROM #LSODSGRADE1 g
				WHERE EXISTS (
					SELECT 1
					FROM Audit.Grades a
					WHERE a.ODSPrimaryKey = g.ODSPrimaryKey
				)
			)

			-- Delete records from #LSODSGRADE1 based on the CTE
			DELETE FROM #LSODSGRADE1
			WHERE ODSPrimaryKey IN (
				SELECT ODSPrimaryKey
				FROM cteToDelete
			);

			-- MERGE statement to synchronize #LSODSGRADE1 with audit.Grades
			MERGE INTO Audit.Grades AS trg
			USING #LSODSGRADE1 AS src
			ON src.ODSPrimaryKey = trg.ODSPrimaryKey
			WHEN NOT MATCHED BY TARGET THEN
				INSERT (
					[GradePrimaryKey],
					[CourseUsersPrimaryKey],
					[RowStatus],
					[HighestScore],
					[HighestGrade],
					[HighestAttemptDateTime],
					[ManualScore],
					[ManualGrade],
					[ManualDateTime],
					[ExemptIndicator],
					[HighestDateTimeCreated],
					[HighestDateTimeModified],
					[HighestIsLatestAttemptIndicator],
					[NumberOfAttempts],
					[FirstScore],
					[FirstGrade],
					[FirstAttemptDateTime],
					[FirstIsLatestAttemptIndicator],
					[FirstDateTimeCreated],
					[FirstDateTimeModified],
					[AssignmentPrimaryKey],
					[AssignmentStatus],
					[ActiveFlag],
					[UMADateTimeAdded],
					[ODSPrimaryKey],
					[SourceSystem]
				)
				VALUES (
					src.[GradePrimaryKey],
					src.[CourseUsersPrimaryKey],
					src.[RowStatus],
					src.[HighestScore],
					src.[HighestGrade],
					src.[HighestAttemptDateTime],
					src.[ManualScore],
					src.[ManualGrade],
					src.[ManualDateTime],
					src.[ExemptIndicator],
					src.[HighestDateTimeCreated],
					src.[HighestDateTimeModified],
					src.[HighestIsLatestAttemptIndicator],
					src.[NumberOfAttempts],
					src.[FirstScore],
					src.[FirstGrade],
					src.[FirstAttemptDateTime],
					src.[FirstIsLatestAttemptIndicator],
					src.[FirstDateTimeCreated],
					src.[FirstDateTimeModified],
					src.[AssignmentPrimaryKey],
					src.[AssignmentStatus],
					src.[ActiveFlag],
					src.[UMADateTimeAdded],
					src.[ODSPrimaryKey],
					src.[SourceSystem]
				)
			WHEN MATCHED THEN
				UPDATE SET
					trg.[GradePrimaryKey] = src.[GradePrimaryKey],
					trg.[CourseUsersPrimaryKey] = src.[CourseUsersPrimaryKey],
					trg.[RowStatus] = src.[RowStatus],
					trg.[HighestScore] = src.[HighestScore],
					trg.[HighestGrade] = src.[HighestGrade],
					trg.[HighestAttemptDateTime] = src.[HighestAttemptDateTime],
					trg.[ManualScore] = src.[ManualScore],
					trg.[ManualGrade] = src.[ManualGrade],
					trg.[ManualDateTime] = src.[ManualDateTime],
					trg.[ExemptIndicator] = src.[ExemptIndicator],
					trg.[HighestDateTimeCreated] = src.[HighestDateTimeCreated],
					trg.[HighestDateTimeModified] = src.[HighestDateTimeModified],
					trg.[HighestIsLatestAttemptIndicator] = src.[HighestIsLatestAttemptIndicator],
					trg.[NumberOfAttempts] = src.[NumberOfAttempts],
					trg.[FirstScore] = src.[FirstScore],
					trg.[FirstGrade] = src.[FirstGrade],
					trg.[FirstAttemptDateTime] = src.[FirstAttemptDateTime],
					trg.[FirstIsLatestAttemptIndicator] = src.[FirstIsLatestAttemptIndicator],
					trg.[FirstDateTimeCreated] = src.[FirstDateTimeCreated],
					trg.[FirstDateTimeModified] = src.[FirstDateTimeModified],
					trg.[AssignmentPrimaryKey] = src.[AssignmentPrimaryKey],
					trg.[AssignmentStatus] = src.[AssignmentStatus],
					trg.[ActiveFlag] = src.[ActiveFlag],
					trg.[UMADateTimeAdded] = src.[UMADateTimeAdded],
					trg.[ODSPrimaryKey] = src.[ODSPrimaryKey],
					trg.[SourceSystem] = src.[SourceSystem];

			-- Delete records from LS_ODS.Grades where ActiveFlag = 0
			DELETE FROM LS_ODS.Grades
			WHERE ActiveFlag = 0;

			-- Create indexes after deletions if necessary
			CREATE NONCLUSTERED INDEX idx_Grades_GradePrimaryKey
			ON LS_ODS.Grades (GradePrimaryKey);

			CREATE NONCLUSTERED INDEX idx_Grades_ActiveFlag
			ON LS_ODS.Grades (ActiveFlag);


        EXEC LS_ODS.AddODSLoadLog 'Grade Duplicate Check And Deletion Complete',
                                  0;

        --**************************************************************************************************************************************** 
        --Remove Orphaned Student Records - These are students who were in course X, started course Y then received a failing grade for course X. 
        --	The student is removed from course Y and re-enrolled in another course X.  If the student had no activity in the course Y to turn 
        --	them Active in that course, the course enrollment record in CampusVue is removed.  This leaves the record for course Y orphaned in our 
        --	data set.  This proces will move those records to the Archive table with a IsOrphanRecord flag set to true. 
        --**************************************************************************************************************************************** 
        INSERT INTO Archive.Students
        (
            StudentPrimaryKey,
            DateTimeCreated,
            DateTimeModified,
            RowStatus,
            BatchUniqueIdentifier,
            BlackboardUsername,
            SyStudentId,
            FirstName,
            LastName,
            Campus,
            AdEnrollSchedId,
            AdClassSchedId,
            CourseUsersPrimaryKey,
            LastLoginDateTime,
            TimeInClass,
            LastI3InteractionNumberMainPhone,
            LastI3InteractionDateTimeMainPhone,
            DaysSinceLastI3InteractionMainPhone,
            LastI3InteractionNumberWorkPhone,
            LastI3InteractionDateTimeWorkPhone,
            DaysSinceLastI3InteractionWorkPhone,
            LastI3InteractionNumberMobilePhone,
            LastI3InteractionDateTimeMobilePhone,
            DaysSinceLastI3InteractionMobilePhone,
            LastI3InteractionNumberOtherPhone,
            LastI3InteractionDateTimeOtherPhone,
            DaysSinceLastI3InteractionOtherPhone,
            Week1Grade,
            Week2Grade,
            Week3Grade,
            Week4Grade,
            Week5Grade,
            SelfTestsCount,
            AssessmentsCount,
            AssignmentsCount,
            DiscussionsCount,
            Week1CompletionRate,
            Week2CompletionRate,
            Week3CompletionRate,
            Week4CompletionRate,
            Week5CompletionRate,
            ActiveFlag,
            UMADateTimeAdded,
            VAStudent,
            NoticeName,
            NoticeDueDate,
            VABenefitName,
            ClassStatus,
            ODSPrimaryKey,
            Week1LDA,
            Week2LDA,
            Week3LDA,
            Week4LDA,
            Week5LDA,
            Week1CompletedAssignments,
            Week2CompletedAssignments,
            Week3CompletedAssignments,
            Week4CompletedAssignments,
            Week5CompletedAssignments,
            CoursePercentage,
            TotalWorkPercentage,
            AdEnrollId,
            IsRetake,
            StudentCourseUserKeys,
            CurrentCourseGrade,
            ProgramCode,
            ProgramName,
            ProgramVersionCode,
            ProgramVersionName,
            MondayTimeInClass,
            TuesdayTimeInClass,
            WednesdayTimeInClass,
            ThursdayTimeInClass,
            FridayTimeInClass,
            SaturdayTimeInClass,
            SundayTimeInClass,
            Week1CompletionRateFixed,
            Week2CompletionRateFixed,
            Week3CompletionRateFixed,
            Week4CompletionRateFixed,
            Week5CompletionRateFixed,
            StudentNumber,
            IsOrphanRecord,
            SourceSystem
        )
        SELECT s.StudentPrimaryKey,
               s.DateTimeCreated,
               s.DateTimeModified,
               s.RowStatus,
               s.BatchUniqueIdentifier,
               s.BlackboardUsername,
               s.SyStudentId,
               s.FirstName,
               s.LastName,
               s.Campus,
               s.AdEnrollSchedId,
               s.AdClassSchedId,
               s.CourseUsersPrimaryKey,
               s.LastLoginDateTime,
               s.TimeInClass,
               s.LastI3InteractionNumberMainPhone,
               s.LastI3InteractionDateTimeMainPhone,
               s.DaysSinceLastI3InteractionMainPhone,
               s.LastI3InteractionNumberWorkPhone,
               s.LastI3InteractionDateTimeWorkPhone,
               s.DaysSinceLastI3InteractionWorkPhone,
               s.LastI3InteractionNumberMobilePhone,
               s.LastI3InteractionDateTimeMobilePhone,
               s.DaysSinceLastI3InteractionMobilePhone,
               s.LastI3InteractionNumberOtherPhone,
               s.LastI3InteractionDateTimeOtherPhone,
               s.DaysSinceLastI3InteractionOtherPhone,
               s.Week1Grade,
               s.Week2Grade,
               s.Week3Grade,
               s.Week4Grade,
               s.Week5Grade,
               s.SelfTestsCount,
               s.AssessmentsCount,
               s.AssignmentsCount,
               s.DiscussionsCount,
               s.Week1CompletionRate,
               s.Week2CompletionRate,
               s.Week3CompletionRate,
               s.Week4CompletionRate,
               s.Week5CompletionRate,
               s.ActiveFlag,
               s.UMADateTimeAdded,
               s.VAStudent,
               s.NoticeName,
               s.NoticeDueDate,
               s.VABenefitName,
               s.ClassStatus,
               s.ODSPrimaryKey,
               s.Week1LDA,
               s.Week2LDA,
               s.Week3LDA,
               s.Week4LDA,
               s.Week5LDA,
               s.Week1CompletedAssignments,
               s.Week2CompletedAssignments,
               s.Week3CompletedAssignments,
               s.Week4CompletedAssignments,
               s.Week5CompletedAssignments,
               s.CoursePercentage,
               s.TotalWorkPercentage,
               s.AdEnrollId,
               s.IsRetake,
               s.StudentCourseUserKeys,
               s.CurrentCourseGrade,
               s.ProgramCode,
               s.ProgramName,
               s.ProgramVersionCode,
               s.ProgramVersionName,
               s.MondayTimeInClass,
               s.TuesdayTimeInClass,
               s.WednesdayTimeInClass,
               s.ThursdayTimeInClass,
               s.FridayTimeInClass,
               s.SaturdayTimeInClass,
               s.SundayTimeInClass,
               s.Week1CompletionRateFixed,
               s.Week2CompletionRateFixed,
               s.Week3CompletionRateFixed,
               s.Week4CompletionRateFixed,
               s.Week5CompletionRateFixed,
               s.StudentNumber,
               1,
               s.SourceSystem
        FROM LS_ODS.Students s
        WHERE s.AdEnrollId IS NULL
              AND s.UMADateTimeAdded > '2015-09-24';

        DELETE s
        FROM LS_ODS.Students s
        WHERE s.AdEnrollId IS NULL
              AND s.UMADateTimeAdded > '2015-09-24';

        EXEC LS_ODS.AddODSLoadLog 'Removed Orphaned Student Records', 0;

        --**************************************************************************************************************************************** 
        --Calculate LDAs Handling Holidays Properly 
        --**************************************************************************************************************************************** 
       DECLARE @BeginRangeDate DATE = '2012-04-01';
		DECLARE @EndRangeDate DATE = DATEADD(DAY, -1, GETDATE());
		DECLARE @TodaysDate DATE = GETDATE();

		-- Use the numbers table to generate the date range
		DECLARE @AllDates TABLE (TheDate DATE);
		INSERT INTO @AllDates (TheDate)
		SELECT DATEADD(DAY, Number - 1, @BeginRangeDate)
		FROM dbo.Numbers
		WHERE Number <= DATEDIFF(DAY, @BeginRangeDate, @EndRangeDate) + 1;

		TRUNCATE TABLE LS_ODS.LDACounts;

		DECLARE @HolidayCounter TABLE (TheDate DATE, IsSchoolDay INT);
		WITH cteHolidays (HolidayDate) AS (
			SELECT DATEADD(DAY, v.number, ca.StartDate) AS HolidayDate
			FROM CV_Prod.dbo.AdCalendar ca
			JOIN CV_Prod.dbo.SyCampusList cl ON ca.SyCampusGrpID = cl.SyCampusGrpID AND cl.SyCampusID = 9
			CROSS JOIN master.dbo.spt_values v
			WHERE v.type = 'P' AND v.number BETWEEN 0 AND DATEDIFF(DAY, ca.StartDate, ca.EndDate)
		)
		INSERT INTO @HolidayCounter (TheDate, IsSchoolDay)
		SELECT ad.TheDate,
			   CASE WHEN h.HolidayDate IS NULL THEN 1 ELSE 0 END AS IsSchoolDay
		FROM @AllDates ad
		LEFT JOIN cteHolidays h ON ad.TheDate = h.HolidayDate;

		WITH cteHolidayCounts (TheDate, HolidayCounter) AS (
			SELECT ad.TheDate,
				   SUM(CASE WHEN hc.IsSchoolDay = 1 THEN 0 ELSE 1 END) AS HolidayCounter
			FROM @HolidayCounter ad
			JOIN @HolidayCounter hc ON ad.TheDate <= hc.TheDate
			GROUP BY ad.TheDate
		)
		INSERT INTO LS_ODS.LDACounts (TheDate, IsSchoolDay, HolidayCounter, LDACount, LDACountMinusHolidayCounter, LDACountMinusHolidayCounterAddHolidayDate)
		SELECT DISTINCT
			   ad.TheDate,
			   hc.IsSchoolDay,
			   hc.HolidayCounter,
			   DATEDIFF(DAY, ad.TheDate, @TodaysDate) AS LDACount,
			   DATEDIFF(DAY, ad.TheDate, @TodaysDate) - hc.HolidayCounter AS LDACountMinusHolidayCounter,
			   DATEDIFF(DAY, ad.TheDate, @TodaysDate) - hc.HolidayCounter + CASE WHEN hc.IsSchoolDay = 0 THEN 1 ELSE 0 END AS LDACountMinusHolidayCounterAddHolidayDate
		FROM @AllDates ad
		JOIN @HolidayCounter hc ON ad.TheDate = hc.TheDate
		JOIN cteHolidayCounts hct ON ad.TheDate = hct.TheDate;

		-- Remote query part
		BEGIN TRANSACTION;
			DELETE FROM [COL-CVU-P-SQ01].FREEDOM.LMS_Data.LDACounts; --PROD
			-- DELETE FROM [MLK-CVU-D-SQ01].FREEDOM.LMS_Data.LDACounts; --DEV

			INSERT INTO [COL-CVU-P-SQ01].FREEDOM.LMS_Data.LDACounts --PROD
			-- INSERT INTO [MLK-CVU-D-SQ01].FREEDOM.LMS_Data.LDACounts --DEV
			(TheDate, IsSchoolDay, HolidayCounter, LDACount, LDACountMinusHolidayCounter, LDACountMinusHolidayCounterAddHolidayDate)
			SELECT TheDate, IsSchoolDay, HolidayCounter, LDACount, LDACountMinusHolidayCounter, LDACountMinusHolidayCounterAddHolidayDate
			FROM LS_ODS.LDACounts;
		COMMIT TRANSACTION;

        EXEC LS_ODS.AddODSLoadLog 'LDA Counts Calculation Complete', 0;

        --**************************************************************************************************************************************** 
        --Update the tables needed for iDash to reduce high number of logical reads 
        --**************************************************************************************************************************************** 
        --CourseWeeklyGradedActivity 
       -- Truncate the target table
		TRUNCATE TABLE LS_ODS.CourseWeeklyGradedActivity;

		-- Insert data into the target table
		INSERT INTO LS_ODS.CourseWeeklyGradedActivity
		(
			StudentId,
			EnrollSchedId,
			ClassSchedId,
			CoursePrimaryKey,
			AssignmentPrimaryKey,
			GradePrimaryKey,
			WeekNumber,
			Assignment,
			Grade,
			[Status],
			DateTaken,
			Week1Grade,
			Week2Grade,
			Week3Grade,
			Week4Grade,
			Week5Grade,
			Attempts,
			DateOfLastAttempt,
			PossiblePoints
		)
		SELECT 
			st.SyStudentId AS StudentId,
			st.AdEnrollSchedId AS EnrollSchedId,
			st.AdClassSchedId AS ClassSchedId,
			co.CoursePrimaryKey AS CoursePrimaryKey,
			asg.AssignmentPrimaryKey AS AssignmentPrimaryKey,
			gr.GradePrimaryKey AS GradePrimaryKey,
			asg.WeekNumber AS WeekNumber,
			asg.AssignmentTitle AS Assignment,
			CASE
				WHEN asg.PossiblePoints IS NOT NULL AND asg.PossiblePoints > 0 THEN
					CASE
						WHEN gr.ManualScore IS NULL OR gr.ManualScore = 0.00 THEN
							((gr.HighestScore / asg.PossiblePoints) * 100)
						ELSE
							((gr.ManualScore / asg.PossiblePoints) * 100)
					END
				ELSE 0
			END AS Grade,
			gr.AssignmentStatus AS Status,
			CONVERT(VARCHAR(10), gr.HighestDateTimeCreated, 101) AS DateTaken,
			st.Week1Grade AS Week1Grade,
			st.Week2Grade AS Week2Grade,
			st.Week3Grade AS Week3Grade,
			st.Week4Grade AS Week4Grade,
			st.Week5Grade AS Week5Grade,
			gr.NumberOfAttempts AS Attempts,
			gr.HighestAttemptDateTime AS DateOfLastAttempt,
			COALESCE(gr.ManualGrade, gr.HighestGrade, '0.00') + '/' + CAST(asg.PossiblePoints AS VARCHAR(4)) AS PointsPossible
		FROM 
			LS_ODS.Students st
			INNER JOIN LS_ODS.Courses co ON co.AdClassSchedId = st.AdClassSchedId
			INNER JOIN LS_ODS.Assignments asg ON asg.CoursePrimaryKey = co.CoursePrimaryKey
			LEFT JOIN LS_ODS.Grades gr ON asg.AssignmentPrimaryKey = gr.AssignmentPrimaryKey AND st.CourseUsersPrimaryKey = gr.CourseUsersPrimaryKey
		WHERE 
			st.AdEnrollSchedId IS NOT NULL;

		-- Log the operation
		EXEC LS_ODS.AddODSLoadLog 'Processed Course Weekly Graded Activity', 0;

        --CourseWeeklyGrades 
        -- Truncate the CourseWeeklyGrades table to ensure it's empty before insertion
			TRUNCATE TABLE LS_ODS.CourseWeeklyGrades;

			-- Declare a table variable to store instructors' data
			DECLARE @Instructors TABLE
			(
				AdClassSchedId INT,
				AcademicFacilitator VARCHAR(50),
				CoInstructor VARCHAR(50)
			);

			-- Insert unique AdClassSchedId values into the @Instructors table
			INSERT INTO @Instructors (AdClassSchedId)
			SELECT DISTINCT ins.AdClassSchedId
			FROM iDash.Instructors_vw ins;

			-- Common Table Expression to find the primary academic facilitator for each class
			WITH cteAcademicFacilitator AS 
			(
				SELECT 
					ins.AdClassSchedId,
					ins.InstructorName AS AcademicFacilitator,
					ROW_NUMBER() OVER (PARTITION BY ins.AdClassSchedId ORDER BY ins.DisplayOrder) AS RowNumber
				FROM iDash.Instructors_vw ins
				WHERE ins.InstructorTypeCode = 'SECONDARY'
			)
			-- Update the AcademicFacilitator in the @Instructors table
			UPDATE ins
			SET ins.AcademicFacilitator = af.AcademicFacilitator
			FROM @Instructors ins
			INNER JOIN cteAcademicFacilitator af
				ON ins.AdClassSchedId = af.AdClassSchedId
				AND af.RowNumber = 1;

			-- Common Table Expression to find the primary co-instructor for each class
			WITH cteCoInstructor AS 
			(
				SELECT 
					ins.AdClassSchedId,
					ins.InstructorName AS CoInstructor,
					ROW_NUMBER() OVER (PARTITION BY ins.AdClassSchedId ORDER BY ins.DisplayOrder) AS RowNumber
				FROM iDash.Instructors_vw ins
				WHERE ins.InstructorTypeCode = 'COINSTR'
			)
			-- Update the CoInstructor in the @Instructors table
			UPDATE ins
			SET ins.CoInstructor = ci.CoInstructor
			FROM @Instructors ins
			INNER JOIN cteCoInstructor ci
				ON ins.AdClassSchedId = ci.AdClassSchedId
				AND ci.RowNumber = 1;

			-- Insert data into CourseWeeklyGrades table
			INSERT INTO LS_ODS.CourseWeeklyGrades
			(
				StudentId,
				EnrollSchedId,
				AdClassSchedID,
				Week1Dates,
				Week2Dates,
				Week3Dates,
				Week4Dates,
				Week5Dates,
				Week1Grade,
				Week2Grade,
				Week3Grade,
				Week4Grade,
				Week5Grade,
				Week1SubRate,
				Week2SubRate,
				Week3SubRate,
				Week4SubRate,
				Week5SubRate,
				CurrentNumericGrade,
				ClassTime,
				SelfTestCount,
				AssessmentCount,
				AssignmentCount,
				DiscussionCount,
				ActivityCount,
				CurrentCourseLetterGrade,
				CourseSubmissionRate,
				AcademicFacilitator,
				CoInstructor
			)
			SELECT 
				st.SyStudentId AS StudentId,
				st.AdEnrollSchedId AS EnrollSchedId,
				st.AdClassSchedId AS AdClassSchedID,
				CONVERT(VARCHAR(5), co.Week1StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week1StartDate), 101) AS Week1Dates,
				CONVERT(VARCHAR(5), co.Week2StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week2StartDate), 101) AS Week2Dates,
				CONVERT(VARCHAR(5), co.Week3StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week3StartDate), 101) AS Week3Dates,
				CONVERT(VARCHAR(5), co.Week4StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week4StartDate), 101) AS Week4Dates,
				CONVERT(VARCHAR(5), co.Week5StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week5StartDate), 101) AS Week5Dates,
				st.Week1Grade * 100 AS Week1Grade,
				st.Week2Grade * 100 AS Week2Grade,
				st.Week3Grade * 100 AS Week3Grade,
				st.Week4Grade * 100 AS Week4Grade,
				st.Week5Grade * 100 AS Week5Grade,
				st.Week1CompletionRate * 100 AS Week1SubRate,
				st.Week2CompletionRate * 100 AS Week2SubRate,
				st.Week3CompletionRate * 100 AS Week3SubRate,
				st.Week4CompletionRate * 100 AS Week4SubRate,
				st.Week5CompletionRate * 100 AS Week5SubRate,
				st.Week5Grade * 100 AS CurrentNumericGrade,
				st.TimeInClass AS ClassTime,
				st.SelfTestsCount AS SelfTestCount,
				st.AssessmentsCount AS AssessmentCount,
				st.AssignmentsCount AS AssignmentCount,
				st.DiscussionsCount AS DiscussionCount,
				st.ActivitiesCount AS ActivityCount,
				CASE
					WHEN (st.Week5Grade * 100) >= 90 THEN 'A'
					WHEN (st.Week5Grade * 100) >= 80 THEN 'B'
					WHEN (st.Week5Grade * 100) >= 70 THEN 'C'
					WHEN (st.Week5Grade * 100) >= 60 THEN 'D'
					ELSE 'F'
				END AS CurrentCourseLetterGrade,
				st.CoursePercentage * 100 AS CourseSubmissionRate,
				ins.AcademicFacilitator,
				ins.CoInstructor
			FROM LS_ODS.Students st
			LEFT JOIN LS_ODS.Courses co ON st.AdClassSchedId = co.AdClassSchedId
			LEFT JOIN @Instructors ins ON co.AdClassSchedId = ins.AdClassSchedId
			WHERE st.AdEnrollSchedId IS NOT NULL;

        EXEC LS_ODS.AddODSLoadLog 'Processed Course Weekly Grades', 0;


        --Wait a short time to ensure the data is all written before report generation starts 
        WAITFOR DELAY '00:01';
        /*Send ODS email after steo number 54 */
        /*	EXECUTE LS_ODS.ODS_Email_2 */


        --**************************************************************************************************************************************** 
        --Process the ActiveSubmissionSummary table 
        --**************************************************************************************************************************************** 
        EXEC LS_ODS.ProcessActiveSubmissionSummary;

        EXEC LS_ODS.AddODSLoadLog 'Active Submission Summary Procesing Complete',
                                  0;

        --**************************************************************************************************************************************** 
        --Process the Total Course Points Earned table 
        --**************************************************************************************************************************************** 
        EXEC LS_ODS.UpsertTotalCoursePointsEarned;

        EXEC LS_ODS.AddODSLoadLog 'Total Course Points Earned Procesing Complete',
                                  0;

        --**************************************************************************************************************************************** 
        --Populate the table(s) needed for ICD-10 Reporting 
        --**************************************************************************************************************************************** 
        EXEC dbo.ProcessICD10Data;

        EXEC LS_ODS.AddODSLoadLog 'ICD Reporting Table Update Complete', 0;


        --		 --**************************************************************************************************************************************** 
        --	--Daily ODS Update Email to Leadership 
        --	--**************************************************************************************************************************************** 

        --	DECLARE @EmailProfileUpdate VARCHAR(500); 
        --	DECLARE @EmailRecipientsUpdate VARCHAR(500); 
        --	DECLARE @EmailSubjectUpdate VARCHAR(500); 
        --	DECLARE @EmailBodyUpdate NVARCHAR(MAX); 
        --	DECLARE @EmailFormatUpdate VARCHAR(50); 
        --	SET @EmailProfileUpdate = 'EDM_DB_ALERT'; 
        --	SET @EmailRecipientsUpdate = 'jrobertson@ultimatemedical.edu;samdavis@ultimatemedical.edu;arhodes@ultimatemedical.edu;bharlow@ultimatemedical.edu;
        --							      jpyszkowski@ultimatemedical.edu;mpiercebyrd@ultimatemedical.edu;gmueller@ultimatemedical.edu;jgugliuzza@Ultimatemedical.edu;nerasala@ultimatemedical.edu;
        --								  smondor@ultimatemedical.edu';		
        --	SET @EmailSubjectUpdate = 'ODS Status Update - ' + CONVERT(VARCHAR(50), DATEADD(DAY, 0, DATEDIFF(DAY, 0, GETDATE())), 101); 
        --	SET @EmailFormatUpdate = 'HTML'; 
        --	SET @EmailBodyUpdate =	N'The daily D2L data import into the Operational Data Store has successfully processed grade updates. We expect the overall process to complete successfully.<br /><br />' +	 
        --							N' 
        --							'; 

        --EXEC msdb.dbo.sp_send_dbmail @profile_name = @EmailProfileUpdate, @blind_copy_recipients = @EmailRecipientsUpdate, @subject = @EmailSubjectUpdate, @body = @EmailBodyUpdate, @body_format = @EmailFormatUpdate; 


        --**************************************************************************************************************************************** 
        --Load Sandbox Reporting Data 
        --**************************************************************************************************************************************** 
        EXEC dbo.ProcessSandboxReportingData;

        EXEC LS_ODS.AddODSLoadLog 'Sandbox Reporting Table Update Complete', 0;

        --**************************************************************************************************************************************** 
        --Load RHIT Reporting Data 
        --**************************************************************************************************************************************** 
        EXEC dbo.ProcessRHITData;

        EXEC LS_ODS.AddODSLoadLog 'RHIT Table Update Complete', 0;

        --**************************************************************************************************************************************** 
        --Load PTCE Reporting Data 
        --**************************************************************************************************************************************** 
        EXEC dbo.ProcessPTCEData;

        EXEC LS_ODS.AddODSLoadLog 'PTCE Table Update Complete', 0;

        --**************************************************************************************************************************************** 
        --Load Program Certification Data 
        --**************************************************************************************************************************************** 
        EXEC LS_ODS.ProcessProgramCertificationData;

        EXEC LS_ODS.AddODSLoadLog 'Program Certification Tables Update Complete',
                                  0;

        --**************************************************************************************************************************************** 
        --Load Course Aggregate Data 
        --**************************************************************************************************************************************** 
        EXEC LS_ODS.ProcessCourseAggregateData;

        EXEC LS_ODS.AddODSLoadLog 'Course Aggregate Tables Update Complete', 0;

        --**************************************************************************************************************************************** 
        --Load Assignment Attempt Counts 
        --**************************************************************************************************************************************** 
        EXEC LS_ODS.MergeAssignmentAttemptCounts;

        EXEC LS_ODS.AddODSLoadLog 'Assignment Attempt Counts Update Complete', 0;

        --**************************************************************************************************************************************** 
        --Load GAR Data 
        --**************************************************************************************************************************************** 
        EXEC dbo.ProcessGRADEDACTIVITYData_Extended;

        EXEC LS_ODS.AddODSLoadLog 'GAR Data Processing Complete', 0;

        --**************************************************************************************************************************************** 
        --Load VA Report Data 
        --**************************************************************************************************************************************** 
        EXEC dbo.ProcessVABenefitReportOct2015Policy;

        EXEC LS_ODS.AddODSLoadLog 'VA Report Data Processing Complete', 0;

        ----Wait a short time to ensure the data is all written before report generation starts 
        --WAITFOR DELAY '00:01'; 

        --**************************************************************************************************************************************** 
        --Generate SSRS Reports archive.gr
        --**************************************************************************************************************************************** 
        EXEC LS_ODS.AddODSLoadLog 'SSRS Reports Creation Started', 0;

        --Execute GradedActivityReport_Extended ssrs report 
        --EXEC [MLK-SSR-P-SQ01].msdb.dbo.sp_start_job N'DF1B7244-D9F7-4C27-A103-ED589EB60A02';  --2008 Server
        EXEC [MLK-REP-P-SQ02].msdb.dbo.sp_start_job N'E5401A80-B99C-4840-83DE-57DDFDCD6C81'; --2016 Server

        --Execute VA Report - New Policy ssrs report 
        --EXEC [MLK-SSR-P-SQ01].msdb.dbo.sp_start_job N'B640B3D8-41EA-45C7-A605-6490D8643B0A';  --2008 Server
        EXEC [MLK-REP-P-SQ02].msdb.dbo.sp_start_job N'F98F0617-E4F1-4F1F-A384-B6EE78BA9EF5'; --2016 Server

        --Wait a short time to ensure the reports are created before the metric collector starts 
        WAITFOR DELAY '00:02';

        EXEC LS_ODS.AddODSLoadLog 'SSRS Reports Creation Complete', 0;

        ----TRUNCATE TABLE [stage].[GradeExtractImport_d2l]; 

        --**************************************************************************************************************************************** 
        --Update the UpdateLog table to allow for proper status reporting 
        --**************************************************************************************************************************************** 
        UPDATE UpdateLog
        SET LastUpdated = GETDATE()
        WHERE TableName = 'LMS ODS Load'
              AND UpdateType = 'Process';

        EXEC LS_ODS.AddODSLoadLog 'ODS Load Process Complete', 0;


        --		 --**************************************************************************************************************************************** 
        --	--send email incase if duplicates are eliminated during merge process 
        --	--**************************************************************************************************************************************** 
        DECLARE @countofrecords INT;
        SET @countofrecords =
        (
            SELECT COUNT(*)
            FROM Stage.ODS_Duplicates
            WHERE PROCCESED_ON = CONVERT(DATE, GETDATE())
        )
        IF @countofrecords > 0
        BEGIN
            DECLARE @tableHTML NVARCHAR(MAX) = N'';

            SELECT @tableHTML += N'<tr><td>' + CAST(PrimaryKey AS NVARCHAR(10)) + N'</td><td>' + STEP_FAILED_ON
                                 + N'</td></tr>'
            FROM Stage.ODS_Duplicates
            WHERE PROCCESED_ON = CONVERT(DATE, GETDATE());
            --SET @tableHTML = N'<html><body><p>Dear Team ,</p>';
            --SET @tableHTML+=N'<html><body><p>Please review the duplicates found in todays ODS process ,</p>'
            SET @tableHTML = N'<table border="1"><tr><th>ID</th><th>Name</th></tr>' + @tableHTML + N'</table>';

            EXEC msdb.dbo.sp_send_dbmail @profile_name = 'EDM_DB_ALERT',
                                         @recipients = 'ppoonati@ultimatemedical.edu',
                                         @subject = 'Duplicate records found in todays ODS Run ',
                                         @body = @tableHTML,
                                         @body_format = 'HTML';

        END

    END TRY

    --		 --**************************************************************************************************************************************** 
    --	--Catch block, send email incase of ODS failure
    --	--**************************************************************************************************************************************** 

    BEGIN CATCH
        DROP TABLE IF EXISTS #tempmail
        CREATE TABLE #tempmail
        (
            EventDetails VARCHAR(240),
            EventDateTime DATETIME
        );
        DECLARE @emailsubject nvarchar(240);
        DECLARE @html_body NVARCHAR(MAX);
        DECLARE @ERRORBODY NVARCHAR(MAX)
            = 'Error message: ' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + 'Error number: '
              + CAST(ERROR_NUMBER() AS NVARCHAR) + CHAR(13) + CHAR(10) + 'Error severity: '
              + CAST(ERROR_SEVERITY() AS NVARCHAR) + CHAR(13) + CHAR(10) + 'Error state: '
              + CAST(ERROR_STATE() AS NVARCHAR) + CHAR(13) + CHAR(10) + 'Error procedure: '
              + COALESCE(ERROR_PROCEDURE(), 'N/A') + CHAR(13) + CHAR(10) + 'Error line number: '
              + CAST(ERROR_LINE() AS NVARCHAR) + CHAR(13) + CHAR(10);
        SET @html_body = N'<html><body><p>Dear Team ,</p>';
        SET @html_body += N'<html><body><p>ODS failed due to below error ,</p>'
        SET @html_body += @ERRORBODY
        SET @html_body += N'<p>Here are the steps that have been processed today:</p>';
        SET @emailsubject
            = 'ODS Process failure-' + CONVERT(VARCHAR(50), DATEADD(DAY, 0, DATEDIFF(DAY, 0, GETDATE())), 101);
        -- Execute the SQL statement and insert the results as a table in the HTML body
        DECLARE @table_html NVARCHAR(MAX);
        SET @table_html = N'<table><thead><tr><th>EventDetails</th><th>EventDateTime</th></tr></thead><tbody>';
        INSERT INTO #tempmail
        (
            EventDetails,
            EventDateTime
        )
        SELECT EventDetails,
               EventDateTime
        FROM LS_ODS.ODSLoadLog
        WHERE CONVERT(date, EventDateTime) = CONVERT(date, GETDATE());
        SELECT @table_html += N'<tr><td>' + EventDetails + N'</td><td>' + CONVERT(VARCHAR, EventDateTime)
                              + N'</td></tr>'
        FROM #tempmail;
        SET @table_html += N'</tbody></table>';

        -- Add the table to the HTML body and close the HTML tags
        SET @html_body += @table_html + N'<p>Best regards,</p><p>EDM TEAM </p></body></html>';

        -- Send the email
        EXEC msdb.dbo.sp_send_dbmail @profile_name = 'EDM_DB_ALERT',
                                     @recipients = 'edmteam@ultimatemedical.edu',
                                     @subject = @emailsubject,
                                     @body = @html_body,
                                     @body_format = 'HTML';

        DECLARE @errorMessage varchar(4000)
        DECLARE @procName varchar(255)
        SELECT @errorMessage = error_message()
        SELECT @procName = OBJECT_NAME(@@PROCID)
        SELECT @procName
        RAISERROR('%sODS failed due to %s', 16, 1, @procName, @errorMessage)

    END CATCH

END;


