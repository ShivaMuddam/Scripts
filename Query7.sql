 USE [LMS_REPORTING]
GO

/****** Object:  StoredProcedure [LS_ODS].[ODS_Process_New_v1]    Script Date: 6/3/2024 4:42:58 PM ******/
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
-- MW - Altered GEI_CTE to add data data manipulation that was previously done later in stored proc, dropped idx_ODS_019, added more logging, commented out clean up we moved to cte, removed index idx_GEI_0002
**/
CREATE
	OR

ALTER PROCEDURE [LS_ODS].[ODS_Process]
AS
BEGIN
	SET NOCOUNT ON;

	/**************************************************************************************************************************************** 
							Declare any global variables 
****************************************************************************************************************************************/
	DECLARE @CurrentDateTime DATETIME;

	SET @CurrentDatetime = GETDATE();

	/**************************************************************************************************************************************** 
						Instiantiate any global variables 
****************************************************************************************************************************************/
	SELECT *
	FROM stage.GradeExtractImport_d2l WITH (NOLOCK)

	EXEC LS_ODS.AddODSLoadLog 'ODS Load Process Started', 0;

	--**************************************************************************************************************************************** 
	EXEC LS_ODS.AddODSLoadLog 'Load Grade Extact Import related data from D2L table stage.GradeExtractImport_d2l', 0;

	--**************************************************************************************************************************************** 
	BEGIN TRY
		DECLARE @CountD2LGEI AS INT

		SELECT @CountD2LGEI = COUNT(*)
		FROM [stage].[GradeExtractImport_d2l] WITH (NOLOCK)

		IF @CountD2LGEI > 0
		BEGIN
			IF EXISTS (
					SELECT 1
					FROM sys.indexes
					WHERE name = 'idx_GEI_0001'
					)
			BEGIN
				DROP INDEX idx_GEI_0001 ON [stage].[GradeExtractImport];
			END;

			IF EXISTS (
					SELECT 1
					FROM sys.indexes
					WHERE name = 'idx_ODS_019'
					)
			BEGIN
				DROP INDEX idx_ODS_019 ON [stage].[GradeExtractImport];
			END;

			--**************************************************************************************************************************************** 
			EXEC LS_ODS.AddODSLoadLog 'Dupes deletion from stage.GradeExtractImport_d2l', 0;

			--**************************************************************************************************************************************** 
			IF OBJECT_ID('tempdb..#SGEI') IS NOT NULL
				DROP TABLE #SGEI;

			/* Import the gradeexractimport into temp table #SGEI*/
			-- Create the temporary table #SGEI and populate data
			SELECT *
			INTO #SGEI
			FROM [stage].[GradeExtractImport_d2l];

			-- Create index on the temporary table for better performance
			CREATE INDEX idx_SGEI_UserPK1_CoursePK1_AssignmentPK1_MembershipPK1_GradePK1 ON #SGEI (UserPK1, CoursePK1, AssignmentPK1, MembershipPK1, GradePK1);

			-- Create index on the ODS_Duplicates table if it doesn't already exist
			IF NOT EXISTS (
					SELECT *
					FROM sys.indexes
					WHERE object_id = OBJECT_ID('Stage.ODS_Duplicates')
						AND name = 'idx_ODS_Duplicates_ProcessedOn_PK1'
					)
			BEGIN
				CREATE INDEX idx_ODS_Duplicates_ProcessedOn_PK1 ON Stage.ODS_Duplicates (PROCCESED_ON, PK1);
			END

			-- Insert duplicates into the Stage.ODS_Duplicates table
			WITH cte
			AS (
				SELECT *, ROW_NUMBER() OVER (
						PARTITION BY UserPK1, CoursePK1, AssignmentPK1, MembershipPK1, GradePK1 ORDER BY (
								SELECT NULL
								)
						) AS rn
				FROM #SGEI
				)
			INSERT INTO Stage.ODS_Duplicates (PrimaryKey, STEP_FAILED_ON, PROCCESED_ON)
			SELECT PK1 AS PrimaryKey, 'Grade_Merge' AS STEP_FAILED_ON, CONVERT(DATE, GETDATE()) AS PROCCESED_ON
			FROM cte
			WHERE rn > 1;

			-- Delete duplicates from the temp table #SGEI that have been identified as dupes and logged in Stage.ODS_Duplicates table on the current date
			DELETE
			FROM #SGEI
			WHERE EXISTS (
					SELECT 1
					FROM Stage.ODS_Duplicates d
					WHERE d.PROCCESED_ON = CONVERT(DATE, GETDATE())
						AND d.PK1 = #SGEI.PK1
					);

			--****************************************************************************************************************************************
			EXEC LS_ODS.AddODSLoadLog 'Stage_GEI_CTE Data Processing', 0;

			IF NOT EXISTS (
					SELECT *
					FROM sys.indexes
					WHERE object_id = OBJECT_ID('[stage].[GradeExtractImport]')
						AND name = 'idx_GEI_0001'
					)
			BEGIN
				CREATE NONCLUSTERED INDEX [idx_GEI_0001] ON [stage].[GradeExtractImport] ([GradeDisplayGrade] ASC) INCLUDE ([GradeDisplayScore])
					WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = ON) ON [PRIMARY];
			END

			IF NOT EXISTS (
					SELECT *
					FROM sys.indexes
					WHERE object_id = OBJECT_ID('[stage].[GradeExtractImport]')
						AND name = 'idx_ODS_019'
					)
			BEGIN
				CREATE NONCLUSTERED INDEX [idx_ODS_019] ON [stage].[GradeExtractImport] ([AssignmentDisplayColumnName] ASC) INCLUDE ([UserPK1], [UserEPK], [CourseTitle])
					WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY];
			END

			--**************************************************************************************************************************************** 
			WITH Stage_GEI_CTE
			AS (
				SELECT ISNULL([UserPK1], '') AS [UserPK1], [UserEPK], [UserLastName], [UserFirstName], [UserUserId], ISNULL([CoursePK1], '') AS [CoursePK1], [CourseEPK], [CourseCourseId], REPLACE(CourseTitle, '"', ',') AS [CourseTitle], ISNULL([MembershipPK1], '') AS [MembershipPK1], ISNULL([AssignmentPK1], '') AS [AssignmentPK1], [AssignmentIsExternalGradeIndicator], REPLACE(REPLACE([AssignmentDisplayColumnName], 'Assessment', 'Test'), 'Interactive', 'Module') AS [AssignmentDisplayColumnName], REPLACE([AssignmentPointsPossible], '"', '') AS [AssignmentPointsPossible], REPLACE(REPLACE([AssignmentDisplayTitle], 'Assessment', 'Test'), 'Interactive', 'Module') AS [AssignmentDisplayTitle], ISNULL([GradePK1], '') AS [GradePK1], [GradeAttemptDate], [GradeAttemptStatus], IIF([GradeManualScore] LIKE '%E%', NULL, [GradeManualGrade]) AS [GradeManualGrade], IIF([GradeManualScore] LIKE '%E%', NULL, [GradeManualScore]) AS [GradeManualScore], CASE 
						WHEN [GradeDisplayGrade] = 'Complete'
							THEN CAST([GradeDisplayScore] AS VARCHAR(50)) + '0'
						WHEN [GradeDisplayScore] LIKE '%E%'
							THEN NULL
						ELSE [GradeDisplayGrade]
						END AS [GradeDisplayGrade], IIF([GradeDisplayScore] LIKE '%E%', NULL, [GradeDisplayScore]) AS [GradeDisplayScore], [GradeExemptIndicator], [GradeOverrideDate], [SourceSystem]
				FROM [stage].[GradeExtractImport]
				WHERE [SourceSystem] = 'D2L'
				)
			MERGE INTO Stage_GEI_CTE AS target
			USING #SGEI AS source
				ON ISNULL(source.[UserPK1], '') = ISNULL(target.[UserPK1], '')
					AND ISNULL(source.[CoursePK1], '') = ISNULL(target.[CoursePK1], '')
					AND ISNULL(source.[AssignmentPK1], '') = ISNULL(target.[AssignmentPK1], '')
					AND ISNULL(source.[MembershipPk1], '') = ISNULL(target.[MembershipPk1], '')
					AND ISNULL(source.[GradePK1], '') = ISNULL(target.[GradePK1], '')
			WHEN MATCHED
				AND NOT EXISTS (
					SELECT source.[UserEPK], source.[UserLastName], source.[UserFirstName], source.[UserUserId], source.[CourseEPK], source.[CourseCourseId], source.[CourseTitle], source.[AssignmentIsExternalGradeIndicator], source.[AssignmentDisplayColumnName], source.[AssignmentPointsPossible], source.[AssignmentDisplayTitle], source.[GradeAttemptDate], source.[GradeAttemptStatus], source.[GradeManualGrade], source.[GradeManualScore], source.[GradeDisplayGrade], source.[GradeDisplayScore], source.[GradeExemptIndicator], source.[GradeOverrideDate], source.[SourceSystem]
					
					INTERSECT
					
					SELECT target.[UserEPK], target.[UserLastName], target.[UserFirstName], target.[UserUserId], target.[CourseEPK], target.[CourseCourseId], target.[CourseTitle], target.[AssignmentIsExternalGradeIndicator], target.[AssignmentDisplayColumnName], target.[AssignmentPointsPossible], target.[AssignmentDisplayTitle], target.[GradeAttemptDate], target.[GradeAttemptStatus], target.[GradeManualGrade], target.[GradeManualScore], target.[GradeDisplayGrade], target.[GradeDisplayScore], target.[GradeExemptIndicator], target.[GradeOverrideDate], target.[SourceSystem]
					)
				THEN
					UPDATE
					SET [UserEPK] = source.[UserEPK], [UserLastName] = source.[UserLastName], [UserFirstName] = source.[UserFirstName], [UserUserId] = source.[UserUserId], [CourseEPK] = source.[CourseEPK], [CourseCourseId] = source.[CourseCourseId], [CourseTitle] = source.[CourseTitle], [AssignmentIsExternalGradeIndicator] = source.[AssignmentIsExternalGradeIndicator], [AssignmentDisplayColumnName] = source.[AssignmentDisplayColumnName], [AssignmentPointsPossible] = source.[AssignmentPointsPossible], [AssignmentDisplayTitle] = source.[AssignmentDisplayTitle], [GradeAttemptDate] = source.[GradeAttemptDate], [GradeAttemptStatus] = source.[GradeAttemptStatus], [GradeManualGrade] = source.[GradeManualGrade], [GradeManualScore] = source.[GradeManualScore], [GradeDisplayGrade] = source.[GradeDisplayGrade], [GradeDisplayScore] = source.[GradeDisplayScore], [GradeExemptIndicator] = source.[GradeExemptIndicator], [GradeOverrideDate] = source.[GradeOverrideDate], [SourceSystem] = source.[SourceSystem]
			WHEN NOT MATCHED
				THEN
					INSERT ([UserPK1], [UserEPK], [UserLastName], [UserFirstName], [UserUserId], [CoursePK1], [CourseEPK], [CourseCourseId], [CourseTitle], [MembershipPK1], [AssignmentPK1], [AssignmentIsExternalGradeIndicator], [AssignmentDisplayColumnName], [AssignmentPointsPossible], [AssignmentDisplayTitle], [GradePK1], [GradeAttemptDate], [GradeAttemptStatus], [GradeManualGrade], [GradeManualScore], [GradeDisplayGrade], [GradeDisplayScore], [GradeExemptIndicator], [GradeOverrideDate], [SourceSystem])
					VALUES (source.[UserPK1], source.[UserEPK], source.[UserLastName], source.[UserFirstName], source.[UserUserId], source.[CoursePK1], source.[CourseEPK], source.[CourseCourseId], source.[CourseTitle], source.[MembershipPK1], source.[AssignmentPK1], source.[AssignmentIsExternalGradeIndicator], source.[AssignmentDisplayColumnName], source.[AssignmentPointsPossible], source.[AssignmentDisplayTitle], source.[GradePK1], source.[GradeAttemptDate], source.[GradeAttemptStatus], source.[GradeManualGrade], source.[GradeManualScore], source.[GradeDisplayGrade], source.[GradeDisplayScore], source.[GradeExemptIndicator], source.[GradeOverrideDate], source.[SourceSystem])
			WHEN NOT MATCHED BY SOURCE
				THEN
					DELETE;

			/* M.Mullane 2024-06-03  temporarily not truncating table to save for dev purposes */
			/*TRUNCATE TABLE [stage].[GradeExtractImport_d2l];  */
			EXEC LS_ODS.AddODSLoadLog 'Finished Loading GradeExtract Data from D2L source table Stage.GradeExtractImport_d2l', 0;
		END
		ELSE
		BEGIN
			EXEC LS_ODS.AddODSLoadLog 'No GradeExtractImport D2L Data Is Available For Today', 0;

			THROW 51000, 'No GradeExtractImport D2L Data Is Available For Today', 1;
		END

		/*THIS IS THE END OF THE PORTION OF THE ODS PROCESS THAT HAS BEEN TAKING THE LONGEST TIME "GRADE EXTRACT IMPORT"*/
		--**************************************************************************************************************************************** 
		--Translate new student-facing reporting values to internal-facing values------comment out? melissa
		--**************************************************************************************************************************************** 
		/*
REPLACED WITH LOGIC IN Stage_GEI_CTE

		UPDATE stage.GradeExtractImport
		SET AssignmentDisplayColumnName = REPLACE(AssignmentDisplayColumnName, 'Assessment' , 'Test'),
			AssignmentDisplayTitle = REPLACE(AssignmentDisplayTitle, 'Assessment' , 'Test')
		WHERE AssignmentDisplayColumnName LIKE '%Assessment%'
			OR AssignmentDisplayTitle LIKE '%Assessment%';

		UPDATE stage.GradeExtractImport
		SET AssignmentDisplayColumnName = REPLACE(AssignmentDisplayColumnName, 'Interactive' , 'Module'),
			AssignmentDisplayTitle = REPLACE(AssignmentDisplayTitle, 'Interactive' , 'Module')
		WHERE AssignmentDisplayColumnName LIKE '%Interactive%'
			OR AssignmentDisplayTitle LIKE '%Interactive%';
*/
		--**************************************************************************************************************************************** 
		--Clean up bad value in Gen 3 Courses 
		--**************************************************************************************************************************************** 
		/*
REPLACED WITH LOGIC IN Stage_GEI_CTE

		UPDATE stage.GradeExtractImport 
		SET GradeDisplayGrade = CAST(GradeDisplayScore AS VARCHAR(50)) + '0' 
		WHERE GradeDisplayGrade = 'Complete'; 
 
		EXEC LS_ODS.AddODSLoadLog 'Cleaned Up Bad Gen 3 Course Values', 0;	 

*/
		--**************************************************************************************************************************************** 
		--Clean up bad max points values found in the GradeExtract file -------comment out melissa ?
		--**************************************************************************************************************************************** 
		DECLARE @Assignments TABLE (AssignmentPK1 INT, PointsPossible DECIMAL(18, 2), NumberOfAssignments INT);

		--Get list of all assignments, possible points and number of records that are the same 
		INSERT INTO @Assignments (AssignmentPK1, PointsPossible, NumberOfAssignments)
		SELECT gei.AssignmentPK1, REPLACE(gei.AssignmentPointsPossible, '"', '') 'PossiblePoints', COUNT(1) 'NumberOfAssignments'
		FROM stage.GradeExtractImport gei
		GROUP BY gei.AssignmentPK1, REPLACE(gei.AssignmentPointsPossible, '"', '');

		--Review list of assignments 
		--SELECT * FROM @Assignments; 
		DECLARE @Adjustments TABLE (AssignmentPK1 INT, PointsPossible DECIMAL(18, 2));

		--Compare the assignments to determine which have more than one value for points possible and store them 
		WITH cteMajorities (AssignmentPK1, MajorityCount)
		AS (
			SELECT a.AssignmentPK1, MAX(a.NumberOfAssignments) 'MajorityCount'
			FROM @Assignments a
			GROUP BY a.AssignmentPK1
			HAVING COUNT(a.AssignmentPK1) > 1
			)
		INSERT INTO @Adjustments (AssignmentPK1, PointsPossible)
		SELECT a.AssignmentPK1, a.PointsPossible
		FROM @Assignments a
		INNER JOIN cteMajorities m ON a.AssignmentPK1 = m.AssignmentPK1
			AND a.NumberOfAssignments = m.MajorityCount;

		--Review the list of assignments that need cleanup 
		--SELECT * FROM @Adjustments; 
		--Update the GradeExtractImport table to remove/overwrite all the assignments with "wrong" values for points possible 
		UPDATE gei
		SET gei.AssignmentPointsPossible = a.PointsPossible
		FROM stage.GradeExtractImport gei
		INNER JOIN @Adjustments a ON gei.AssignmentPK1 = a.AssignmentPK1;

		EXEC LS_ODS.AddODSLoadLog 'Cleaned Up Bad Assignment Max Points Values', 0;

		--**************************************************************************************************************************************** 
		EXEC LS_ODS.AddODSLoadLog 'Clean up missing Assignment Status values found in the GradeExtract file', 0;

		--**************************************************************************************************************************************** 
		WITH cteAssignmentStatuses (UserEPK, CourseEPK, GradePK1, FirstAttemptStatus)
		AS (
			SELECT u.BATCH_UID AS UserEPK, cm.BATCH_UID AS CourseEPK, gg.PK1 AS GradePk1, a.[STATUS] AS FirstAttemptStatus
			FROM dbo.GRADEBOOK_GRADE gg
			INNER JOIN dbo.COURSE_USERS cu ON gg.COURSE_USERS_PK1 = cu.PK1
			INNER JOIN dbo.USERS u ON cu.USERS_PK1 = u.PK1
			INNER JOIN dbo.COURSE_MAIN cm ON cu.CRSMAIN_PK1 = cm.PK1
			INNER JOIN dbo.GRADEBOOK_MAIN gm ON gg.GRADEBOOK_MAIN_PK1 = gm.PK1
			LEFT JOIN dbo.ATTEMPT a ON gg.FIRST_ATTEMPT_PK1 = a.PK1
			WHERE gg.HIGHEST_ATTEMPT_PK1 IS NULL
				AND a.[STATUS] IS NOT NULL
			)
		-- Update GradeAttemptStatus in stage.GradeExtractImport
		UPDATE gei
		SET gei.GradeAttemptStatus = cas.FirstAttemptStatus
		FROM stage.GradeExtractImport gei
		INNER JOIN cteAssignmentStatuses cas ON gei.UserEPK = cas.UserEPK
			AND gei.CourseEPK = cas.CourseEPK
			AND gei.GradePK1 = cas.GradePK1
		WHERE gei.GradeAttemptDate IS NOT NULL
			AND gei.GradeAttemptStatus IS NULL;

		-- Update GradeAttemptStatus to 6 where GradeAttemptDate is not null and GradeAttemptStatus is null
		UPDATE stage.GradeExtractImport
		SET GradeAttemptStatus = 6
		WHERE GradeAttemptDate IS NOT NULL
			AND GradeAttemptStatus IS NULL;

		EXEC LS_ODS.AddODSLoadLog 'Cleaned Up Missing Assignment Status Values', 0;

		/*
REPLACED WITH LOGIC IN Stage_GEI_CTE

		--Fix bad display score values -----comment out?melissa
		--UPDATE stage.GradeExtractImport
		--SET GradeDisplayGrade = NULL,
		--	GradeDisplayScore = NULL
		--WHERE GradeDisplayScore LIKE '%E%';
 
		--UPDATE stage.GradeExtractImport
		--SET GradeManualGrade = NULL,
		--	GradeManualScore = NULL
		--WHERE GradeManualScore LIKE '%E%';

		--Replace all double quotes to commas as expected (bug in GradebookExtract translates all commas to double quotes)
		UPDATE stage.GradeExtractImport
		SET CourseTitle = REPLACE(CourseTitle, '"', ',');
*/
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
		WITH cteVAStudents (SyStudentId, BenefitName)
		AS (
			SELECT s.SyStudentId, uv.FieldValue AS BenefitName
			FROM CV_Prod.dbo.syStudent s
			INNER JOIN CV_Prod.dbo.SyUserValues uv ON s.SyStudentId = uv.syStudentID
				AND uv.syUserDictID = 51
				AND LEFT(uv.FieldValue, 2) = 'VA'
			), cteNotices (SyStudentId, NoticeName, NoticeDueDate)
		AS (
			SELECT e.SyStudentId, t.Descrip AS NoticeName, MAX(e.DueDate) AS NoticeDueDate
			FROM CV_Prod.dbo.CmEvent e
			INNER JOIN CV_Prod.dbo.CmTemplate t ON e.CmTemplateID = t.CmTemplateID
			WHERE e.CmTemplateID IN (
					1404
					,1405
					)
				AND e.CmEventStatusID = 1
			GROUP BY e.SyStudentId, t.Descrip
			), cteRetakes (SyStudentId, AdEnrollId, AdCourseId, Tries)
		AS (
			SELECT A.SyStudentID, A.AdEnrollID, A.AdCourseID, COUNT(A.AdCourseID) AS Tries
			FROM CV_Prod.dbo.AdEnrollSched A
			INNER JOIN CV_Prod.dbo.AdEnroll B ON A.AdEnrollID = B.AdEnrollID
			INNER JOIN CV_Prod.dbo.SySchoolStatus ON B.SySchoolStatusID = SySchoolStatus.SySchoolStatusID
			INNER JOIN CV_Prod.dbo.syStatus ON SySchoolStatus.SyStatusID = syStatus.SyStatusID
			WHERE B.SyCampusID = 9
				AND A.[Status] IN (
					'P'
					,'C'
					) -- Posted, Current 
				AND (
					syStatus.Category IN (
						'A'
						,'T'
						)
					OR syStatus.Category = 'E'
					)
				AND A.RetakeFlag IS NOT NULL
				AND A.AdGradeLetterCode IN (
					'F'
					,''
					) -- Current, Retaken, Retake 
			GROUP BY A.SyStudentID, A.AdEnrollID, A.AdCourseID
			HAVING COUNT(A.AdCourseID) > 1
			)
		INSERT INTO stage.Students (StudentPrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, BlackboardUsername, SyStudentId, FirstName, LastName, Campus, AdEnrollSchedId, AdClassSchedId, CourseUsersPrimaryKey, VAStudent, NoticeName, NoticeDueDate, VABenefitName, ClassStatus, AdEnrollId, IsRetake, StudentCourseUserKeys, ProgramCode, ProgramName, ProgramVersionCode, ProgramVersionName, StudentNumber, SourceSystem)
		SELECT DISTINCT gei.UserPK1 'StudentPrimaryKey', u.DTCREATED 'DateTimeCreated', u.DTMODIFIED 'DateTimeModified', bs.[Description] 'RowStatus', gei.UserEPK 'BatchUniqueIdentifier', gei.UserUserId 'BlackboardUsername', REPLACE(gei.UserEPK, 'SyStudent_', '') 'SyStudentId', gei.UserFirstName 'FirstName', gei.UserLastName 'LastName', c.Descrip 'Campus', CAST(es.AdEnrollSchedID AS VARCHAR(100)) 'AdEnrollSchedId', REPLACE(gei.CourseEPK, 'AdCourse_', '') 'AdClassSchedId', gei.MembershipPK1 'CourseUsersPrimaryKey', CASE 
				WHEN vas.SyStudentId IS NOT NULL
					THEN 1
				ELSE 0
				END 'VAStudent', n.NoticeName 'NoticeName', n.NoticeDueDate 'NoticeDueDate', vas.BenefitName 'VABenefitName', es.[Status] 'ClassStatus', es.AdEnrollID 'AdEnrollId', CASE 
				WHEN r.Tries > 1
					THEN 1
				ELSE 0
				END 'IsRetake', CAST(gei.UserPK1 AS VARCHAR(50)) + CAST(gei.MembershipPK1 AS VARCHAR(50)) 'StudentCourseUserKeys', pr.Code 'ProgramCode', pr.Descrip 'ProgramName', pv.Code 'ProgramVersionCode', pv.Descrip 'ProgramVersionName', st.StuNum 'StudentNumber', gei.SourceSystem
		FROM stage.GradeExtractImport gei
		LEFT JOIN USERS u ON gei.UserPK1 = u.PK1
		LEFT JOIN stage.BlackboardStatuses bs ON u.ROW_STATUS = bs.PrimaryKey
			AND bs.[TYPE] = 'Row'
		LEFT JOIN CV_Prod.dbo.AdClassSched cs ON REPLACE(gei.CourseEPK, 'AdCourse_', '') = CAST(cs.AdClassSchedID AS VARCHAR(50))
		LEFT JOIN CV_Prod.dbo.SyCampus c ON cs.SyCampusID = c.SyCampusID
		LEFT JOIN CV_Prod.dbo.AdEnrollSched es ON REPLACE(gei.CourseEPK, 'AdCourse_', '') = CAST(es.AdClassSchedID AS VARCHAR(50))
			AND REPLACE(gei.UserEPK, 'SyStudent_', '') = CAST(es.SyStudentID AS VARCHAR(50))
			AND es.[Status] IN (
				'C'
				,'S'
				,'P'
				)
		LEFT JOIN cteVAStudents vas ON REPLACE(gei.UserEPK, 'SyStudent_', '') = CAST(vas.SyStudentId AS VARCHAR(50))
		LEFT JOIN cteNotices n ON REPLACE(gei.UserEPK, 'SyStudent_', '') = CAST(n.SyStudentId AS VARCHAR(50))
		LEFT JOIN cteRetakes r ON es.SyStudentID = r.SyStudentId
			AND es.AdEnrollID = r.AdEnrollId
			AND es.AdCourseID = r.AdCourseId
		LEFT JOIN CV_Prod.dbo.AdEnroll en ON es.AdEnrollId = en.AdEnrollID
		LEFT JOIN CV_Prod.dbo.AdProgram pr ON en.AdProgramID = pr.AdProgramID
		LEFT JOIN CV_Prod.dbo.AdProgramVersion pv ON en.adProgramVersionID = pv.AdProgramVersionID
		LEFT JOIN CV_Prod.dbo.SyStudent st ON en.SyStudentID = st.SyStudentId
		WHERE LEFT(gei.UserEPK, 9) = 'SyStudent' --Only Students 
			AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8 --Filter Out Test/Bad Students 
			AND LEFT(gei.CourseEPK, 8) = 'AdCourse' --Only Courses 
			AND (
				gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --Filter Out Test/Bad Courses 
				OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --To bring in CLW courses 
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%' --2/28/2024 CML: Captures EMT Courses
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%'
				) --2/28/2024 CML: Captures EMT Courses based out of CLW
			AND gei.UserFirstName NOT LIKE 'BBAFL%' --More Test Students
			AND gei.UserEPK NOT LIKE '%PART1%' --More Test Students
			AND gei.UserEPK NOT LIKE '%PART2%' --More Test Students
			AND gei.UserEPK NOT LIKE '%PART3%' --More Test Students
			AND gei.UserEPK NOT LIKE '%PART4%' --More Test Students
			AND gei.USEREPK NOT LIKE '%PART5%' --More Test Students
			;

		--AND gei.UserEPK <> 'SyStudent_2670907' AND gei.UserEPK <> 'SyStudent_4729014'             --Commented out this portion of the where 
		--clause which was excluding the loading of these students
		EXEC LS_ODS.AddODSLoadLog 'Loaded Students Working Table', 0;

		--**************************************************************************************************************************************** 
		--Fill the stage.Courses table with all the values from the raw import table 
		--**************************************************************************************************************************************** 
		WITH cteInstructors (AdClassSchedId, PrimaryInstructorId, PrimaryInstructor, SecondaryInstructorId, SecondaryInstructor)
		AS (
			SELECT cs.AdClassSchedID 'AdClassSchedId', spi.SyStaffID 'PrimaryInstructorId', spi.LastName + ', ' + spi.FirstName 'PrimaryInstructor', spi2.SyStaffID 'SecondaryInstructorId', spi2.LastName + ', ' + spi2.FirstName 'SecondaryInstructor'
			FROM CV_Prod.dbo.AdClassSched cs WITH (NOLOCK)
			LEFT JOIN CV_Prod.dbo.SyStaff spi ON cs.AdTeacherID = spi.SyStaffID
			LEFT JOIN CV_Prod.dbo.AdClassSchedInstructorAttributes t ON cs.AdClassSchedID = t.AdClassSchedID
				AND t.AdInstructorAttributesID = 2
			LEFT JOIN CV_Prod.dbo.SyStaff spi2 ON t.AdTeacherID = spi2.SyStaffID
			)
		INSERT INTO stage.Courses (CoursePrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, CourseCode, CourseName, SectionNumber, AdClassSchedId, PrimaryInstructor, SecondaryInstructor, IsOrganization, AcademicFacilitator, PrimaryInstructorId, SecondaryInstructorId, AcademicFacilitatorId, SourceSystem)
		SELECT DISTINCT gei.UserPK1 AS StudentPrimaryKey, u.DTCREATED AS DateTimeCreated, u.DTMODIFIED AS DateTimeModified, bs.[Description] AS RowStatus, gei.UserEPK AS BatchUniqueIdentifier, gei.UserUserId AS BlackboardUsername, REPLACE(gei.UserEPK, 'SyStudent_', '') AS SyStudentId, gei.UserFirstName AS FirstName, gei.UserLastName AS LastName, c.Descrip AS Campus, CAST(es.AdEnrollSchedID AS VARCHAR(100)) AS AdEnrollSchedId, REPLACE(gei.CourseEPK, 'AdCourse_', '') AS AdClassSchedId, gei.MembershipPK1 AS CourseUsersPrimaryKey, CASE 
				WHEN vas.SyStudentId IS NOT NULL
					THEN 1
				ELSE 0
				END AS VAStudent, n.NoticeName AS NoticeName, n.NoticeDueDate AS NoticeDueDate, vas.BenefitName AS VABenefitName, es.[Status] AS ClassStatus, es.AdEnrollID AS AdEnrollId, CASE 
				WHEN r.Tries > 1
					THEN 1
				ELSE 0
				END AS IsRetake, CAST(gei.UserPK1 AS VARCHAR(50)) + CAST(gei.MembershipPK1 AS VARCHAR(50)) AS StudentCourseUserKeys, pr.Code AS ProgramCode, pr.Descrip AS ProgramName, pv.Code AS ProgramVersionCode, pv.Descrip AS ProgramVersionName, st.StuNum AS StudentNumber, gei.SourceSystem
		FROM stage.GradeExtractImport gei
		LEFT JOIN USERS u ON gei.UserPK1 = u.PK1
		LEFT JOIN stage.BlackboardStatuses bs ON u.ROW_STATUS = bs.PrimaryKey
			AND bs.[TYPE] = 'Row'
		LEFT JOIN CV_Prod.dbo.AdClassSched cs ON REPLACE(gei.CourseEPK, 'AdCourse_', '') = CAST(cs.AdClassSchedID AS VARCHAR(50))
		LEFT JOIN CV_Prod.dbo.SyCampus c ON cs.SyCampusID = c.SyCampusID
		LEFT JOIN CV_Prod.dbo.AdEnrollSched es ON REPLACE(gei.CourseEPK, 'AdCourse_', '') = CAST(es.AdClassSchedID AS VARCHAR(50))
			AND REPLACE(gei.UserEPK, 'SyStudent_', '') = CAST(es.SyStudentID AS VARCHAR(50))
			AND es.[Status] IN (
				'C'
				,'S'
				,'P'
				)
		LEFT JOIN cteVAStudents vas ON REPLACE(gei.UserEPK, 'SyStudent_', '') = CAST(vas.SyStudentId AS VARCHAR(50))
		LEFT JOIN cteNotices n ON REPLACE(gei.UserEPK, 'SyStudent_', '') = CAST(n.SyStudentId AS VARCHAR(50))
		LEFT JOIN cteRetakes r ON es.SyStudentID = r.SyStudentId
			AND es.AdEnrollID = r.AdEnrollId
			AND es.AdCourseID = r.AdCourseId
		LEFT JOIN CV_Prod.dbo.AdEnroll en ON es.AdEnrollId = en.AdEnrollID
		LEFT JOIN CV_Prod.dbo.AdProgram pr ON en.AdProgramID = pr.AdProgramID
		LEFT JOIN CV_Prod.dbo.AdProgramVersion pv ON en.adProgramVersionID = pv.AdProgramVersionID
		LEFT JOIN CV_Prod.dbo.SyStudent st ON en.SyStudentID = st.SyStudentId
		WHERE LEFT(gei.UserEPK, 9) = 'SyStudent' -- Only Students 
			AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8 -- Filter Out Test/Bad Students 
			AND LEFT(gei.CourseEPK, 8) = 'AdCourse' -- Only Courses 
			AND (
				gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' -- Filter Out Test/Bad Courses 
				OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' -- To bring in CLW courses 
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%' -- 2/28/2024 CML: Captures EMT Courses
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%'
				) -- 2/28/2024 CML: Captures EMT Courses based out of CLW
			AND gei.UserFirstName NOT LIKE 'BBAFL%' -- More Test Students
			AND gei.UserEPK NOT LIKE '%PART1%' -- More Test Students
			AND gei.UserEPK NOT LIKE '%PART2%' -- More Test Students
			AND gei.UserEPK NOT LIKE '%PART3%' -- More Test Students
			AND gei.UserEPK NOT LIKE '%PART4%' -- More Test Students
			AND gei.USEREPK NOT LIKE '%PART5%' -- More Test Students;

		EXEC LS_ODS.AddODSLoadLog 'Loaded Courses Working Table', 0;

		--**************************************************************************************************************************************** 
		--Fill the stage.Assignments table with all the values from the raw import table 
		--**************************************************************************************************************************************** 
		-- Drop constraint PK_Assignments_2 if it exists
		IF EXISTS (
				SELECT 1
				FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
				WHERE CONSTRAINT_NAME = 'PK_Assignments_2'
					AND TABLE_SCHEMA = 'stage'
					AND TABLE_NAME = 'Assignments'
				)
		BEGIN
			ALTER TABLE [stage].[Assignments]

			DROP CONSTRAINT [PK_Assignments_2];
		END

		-- Drop index idx_Assignments_3 if it exists
		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_Assignments_3'
					AND object_id = OBJECT_ID('[stage].[Assignments]')
				)
		BEGIN
			DROP INDEX [idx_Assignments_3] ON [stage].[Assignments];
		END

		-- Drop index idx_ODS_004 if it exists
		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_ODS_004'
					AND object_id = OBJECT_ID('[stage].[Assignments]')
				)
		BEGIN
			DROP INDEX [idx_ODS_004] ON [stage].[Assignments];
		END

		-- Drop index idx_ODS_005 if it exists
		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_ODS_005'
					AND object_id = OBJECT_ID('[stage].[Assignments]')
				)
		BEGIN
			DROP INDEX [idx_ODS_005] ON [stage].[Assignments];
		END

		INSERT INTO stage.Assignments (AssignmentPrimaryKey, CoursePrimaryKey, WeekNumber, AssignmentTitle, DueDate, PossiblePoints, DateTimeCreated, DateTimeModified, ScoreProviderHandle, CourseContentsPrimaryKey1, AlternateTitle, IsReportable, CountsAsSubmission, SourceSystem)
		SELECT DISTINCT gei.AssignmentPK1 AS AssignmentPrimaryKey, gei.CoursePK1 AS CoursePrimaryKey, CASE 
				WHEN LEFT(gei.AssignmentDisplayColumnName, 4) = 'Week'
					AND ISNUMERIC(SUBSTRING(gei.AssignmentDisplayColumnName, 6, 2)) = 1
					THEN SUBSTRING(gei.AssignmentDisplayColumnName, 6, 2)
				WHEN LEFT(gei.AssignmentDisplayColumnName, 4) = 'Week'
					AND ISNUMERIC(SUBSTRING(gei.AssignmentDisplayColumnName, 6, 1)) = 1
					THEN SUBSTRING(gei.AssignmentDisplayColumnName, 6, 1)
				WHEN LEFT(gei.AssignmentDisplayColumnName, 3) = 'Wk '
					AND ISNUMERIC(SUBSTRING(gei.AssignmentDisplayColumnName, 4, 2)) = 1
					THEN SUBSTRING(gei.AssignmentDisplayColumnName, 4, 2)
				WHEN LEFT(gei.AssignmentDisplayColumnName, 3) = 'Wk '
					AND ISNUMERIC(SUBSTRING(gei.AssignmentDisplayColumnName, 4, 1)) = 1
					THEN SUBSTRING(gei.AssignmentDisplayColumnName, 4, 1)
				ELSE 0
				END AS WeekNumber, CASE 
				WHEN LEFT(gei.AssignmentDisplayColumnName, 4) = 'Week'
					THEN CASE 
							WHEN SUBSTRING(gei.AssignmentDisplayColumnName, 8, 2) = '- '
								THEN LTRIM(RTRIM(SUBSTRING(gei.AssignmentDisplayColumnName, 10, 1000)))
							ELSE LTRIM(RTRIM(SUBSTRING(gei.AssignmentDisplayColumnName, 8, 1000)))
							END
				WHEN LEFT(gei.AssignmentDisplayColumnName, 3) = 'Wk '
					THEN LTRIM(RTRIM(SUBSTRING(gei.AssignmentDisplayColumnName, 8, 1000)))
				ELSE gei.AssignmentDisplayColumnName
				END AS AssignmentTitle, gm.DUE_DATE AS DueDate, REPLACE(gei.AssignmentPointsPossible, '"', '') AS PossiblePoints, gm.DATE_ADDED AS DateTimeCreated, gm.DATE_MODIFIED AS DateTimeModified, gm.SCORE_PROVIDER_HANDLE AS ScoreProviderHandle, gm.COURSE_CONTENTS_PK1 AS CourseContentsPrimaryKey, gei.AssignmentDisplayTitle AS AlternateTitle, 1 AS IsReportable, 1 AS CountsAsSubmission, gei.SourceSystem
		FROM stage.GradeExtractImport gei
		LEFT JOIN GRADEBOOK_MAIN gm ON gei.AssignmentPK1 = gm.PK1
		WHERE LEFT(gei.UserEPK, 9) = 'SyStudent' -- Only Students 
			AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8 -- Filter Out Test/Bad Students 
			AND LEFT(gei.CourseEPK, 8) = 'AdCourse' -- Only Courses 
			AND (
				gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' -- Filter Out Test/Bad Courses 
				OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' -- To bring in CLW courses 
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%' -- Captures EMT Courses
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%' -- Captures EMT Courses based out of CLW
				)
			AND gei.UserFirstName NOT LIKE 'BBAFL%' -- Exclude test students
			AND gei.UserEPK NOT LIKE '%PART[1-5]%' -- Exclude more test students
			AND gei.AssignmentDisplayTitle NOT LIKE '% Extended %' -- Exclude specific assignment titles
			AND gei.AssignmentDisplayTitle NOT LIKE '%Grade %' -- Exclude specific assignment titles
			AND (
				(
					gei.AssignmentDisplayColumnName = 'Final Grade'
					AND gei.AssignmentIsExternalGradeIndicator = 'Y'
					)
				OR (
					gei.AssignmentDisplayColumnName <> 'Final Grade'
					AND gei.AssignmentIsExternalGradeIndicator = 'N'
					)
				);

		--Filter Out Final Grade Not Marked As External 
		UPDATE asg
		SET asg.AssignmentType = COALESCE(REPLACE(gt.NAME, '.name', ''), CASE 
					WHEN asg.ScoreProviderHandle IN (
							'resource/x-bb-assignment'
							,'resource/mcgraw-hill-assignment'
							)
						OR asg.AssignmentTitle LIKE '%Assign%'
						THEN 'Assignment'
					WHEN asg.ScoreProviderHandle = 'resource/x-bb-assessment'
						THEN 'Test'
					WHEN asg.ScoreProviderHandle = 'resource/x-bb-forumlink'
						THEN 'Discussion'
					WHEN asg.ScoreProviderHandle = 'resource/x-plugin-scormengine'
						THEN 'SCORM/AICC'
							-- D2L ScoreProviderHandle --
					WHEN asg.ScoreProviderHandle = 'resource/d2l/Assessment'
						THEN 'Assessment'
					WHEN asg.ScoreProviderHandle = 'resource/d2l/Assignments'
						THEN 'Assignment'
					WHEN asg.ScoreProviderHandle = 'resource/d2l/Discussions'
						THEN 'Discussion'
					WHEN asg.ScoreProviderHandle = 'resource/d2l/ExtraCredit'
						THEN 'Extra Credit'
					WHEN asg.ScoreProviderHandle = 'resource/d2l/RollCall'
						THEN 'Roll Call'
					WHEN asg.ScoreProviderHandle = 'resource/d2l/SCORM'
						THEN 'SCORM/AICC'
					ELSE 'Unknown'
					END)
		FROM stage.Assignments asg
		INNER JOIN stage.Courses co ON asg.CoursePrimaryKey = co.CoursePrimaryKey
		LEFT JOIN dbo.GRADEBOOK_MAIN gm ON asg.CourseContentsPrimaryKey1 = gm.COURSE_CONTENTS_PK1
			AND co.CoursePrimaryKey = gm.CRSMAIN_PK1
		LEFT JOIN dbo.GRADEBOOK_TYPE gt ON gm.GRADEBOOK_TYPE_PK1 = gt.PK1;

		-- Add the primary key constraint if it does not exist
		IF NOT EXISTS (
				SELECT *
				FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
				WHERE TABLE_SCHEMA = 'stage'
					AND TABLE_NAME = 'Assignments'
					AND CONSTRAINT_NAME = 'PK_Assignments_2'
				)
		BEGIN
			ALTER TABLE [stage].[Assignments] ADD CONSTRAINT [PK_Assignments_2] PRIMARY KEY CLUSTERED ([AssignmentPrimaryKey] ASC, [CoursePrimaryKey] ASC)
				WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY];
		END

		-- Create nonclustered index idx_Assignments_3 if it does not exist
		IF NOT EXISTS (
				SELECT *
				FROM sys.indexes
				WHERE name = 'idx_Assignments_3'
					AND object_id = OBJECT_ID('[stage].[Assignments]')
				)
		BEGIN
			CREATE NONCLUSTERED INDEX [idx_Assignments_3] ON [stage].[Assignments] ([CountsAsSubmission] ASC, [WeekNumber] ASC) INCLUDE ([CoursePrimaryKey], [AssignmentTitle], [PossiblePoints])
				WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY];
		END

		-- Create nonclustered index idx_ODS_004 if it does not exist
		IF NOT EXISTS (
				SELECT *
				FROM sys.indexes
				WHERE name = 'idx_ODS_004'
					AND object_id = OBJECT_ID('[stage].[Assignments]')
				)
		BEGIN
			CREATE NONCLUSTERED INDEX [idx_ODS_004] ON [stage].[Assignments] ([AssignmentTitle] ASC) INCLUDE ([AssignmentPrimaryKey], [CoursePrimaryKey])
				WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];
		END

		-- Create nonclustered index idx_ODS_005 if it does not exist
		IF NOT EXISTS (
				SELECT *
				FROM sys.indexes
				WHERE name = 'idx_ODS_005'
					AND object_id = OBJECT_ID('[stage].[Assignments]')
				)
		BEGIN
			CREATE NONCLUSTERED INDEX [idx_ODS_005] ON [stage].[Assignments] ([CoursePrimaryKey] ASC, [CountsAsSubmission] ASC, [WeekNumber] ASC) INCLUDE ([AssignmentTitle], [PossiblePoints])
				WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY];
		END

		EXEC LS_ODS.AddODSLoadLog 'Loaded Assignments Working Table', 0;

		--**************************************************************************************************************************************** 
		--Fill the stage.Grades table with all the values from the raw import table 
		--****************************************************************************************************************************************
		--All Assignments With A Primary Key
		INSERT INTO stage.Grades (GradePrimaryKey, CourseUsersPrimaryKey, RowStatus, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, ManualDateTime, ExemptIndicator, HighestDateTimeCreated, HighestDateTimeModified, HighestIsLatestAttemptIndicator, FirstScore, FirstGrade, FirstAttemptDateTime, FirstIsLatestAttemptIndicator, FirstDateTimeCreated, FirstDateTimeModified, AssignmentPrimaryKey, AssignmentStatus, SourceSystem)
		SELECT DISTINCT gei.GradePK1 AS GradePrimaryKey, cu.PK1 AS CourseUsersPrimaryKey, bs.[Description] AS RowStatus, gei.GradeDisplayScore AS HighestScore, gei.GradeDisplayGrade AS HighestGrade, gei.GradeAttemptDate AS HighestAttemptDateTime, gei.GradeManualScore AS ManualScore, gei.GradeManualGrade AS ManualGrade, gei.GradeOverrideDate AS ManualDateTime, gei.GradeExemptIndicator AS ExemptIndicator, ha.DATE_ADDED AS HighestDateTimeCreated, ha.DATE_MODIFIED AS HighestDateTimeModified, CASE 
				WHEN gg.HIGHEST_ATTEMPT_PK1 = gg.LAST_ATTEMPT_PK1
					THEN 1
				ELSE 0
				END AS HighestIsLatestAttemptIndicator, fa.SCORE AS FirstScore, fa.GRADE AS FirstGrade, fa.ATTEMPT_DATE AS FirstAttemptDateTime, CASE 
				WHEN gg.FIRST_ATTEMPT_PK1 = gg.LAST_ATTEMPT_PK1
					THEN 1
				ELSE 0
				END AS FirstIsLatestAttemptIndicator, fa.DATE_ADDED AS FirstDateTimeCreated, fa.DATE_MODIFIED AS FirstDateTimeModified, gei.AssignmentPK1 AS AssignmentPrimaryKey, CASE 
				WHEN gei.GradeAttemptStatus IS NULL
					AND gei.GradeAttemptDate IS NULL
					THEN 'NOT COMPLETE'
				ELSE gs.[Description]
				END AS AssignmentStatus, gei.SourceSystem
		FROM stage.GradeExtractImport gei
		LEFT JOIN COURSE_USERS cu ON gei.UserPK1 = cu.USERS_PK1
			AND gei.CoursePK1 = cu.CRSMAIN_PK1
		LEFT JOIN GRADEBOOK_GRADE gg ON gei.GradePK1 = gg.PK1
		LEFT JOIN stage.BlackboardStatuses bs ON gg.[STATUS] = bs.PrimaryKey
			AND bs.[Type] = 'Row'
		LEFT JOIN ATTEMPT ha ON gg.HIGHEST_ATTEMPT_PK1 = ha.PK1
		LEFT JOIN ATTEMPT fa ON gg.FIRST_ATTEMPT_PK1 = fa.PK1
		LEFT JOIN stage.BlackboardStatuses gs ON gei.GradeAttemptStatus = gs.PrimaryKey
			AND gs.[Type] = 'Grade'
		LEFT JOIN dbo.DATA_SOURCE ds ON ds.PK1 = cu.DATA_SRC_PK1
		WHERE LEFT(gei.UserEPK, 9) = 'SyStudent' -- Only Students 
			AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8 -- Filter Out Test/Bad Students 
			AND LEFT(gei.CourseEPK, 8) = 'AdCourse' -- Only Courses 
			AND (
				gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' -- Filter Out Test/Bad Courses 
				OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' -- To bring in CLW courses 
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%' -- Captures EMT Courses
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%'
				) -- Captures EMT Courses based out of CLW
			AND gei.UserFirstName NOT LIKE 'BBAFL%' -- More Test Students
			AND gei.UserEPK NOT LIKE '%PART[1-5]%' -- More Test Students
			AND gei.GradePK1 IS NOT NULL -- Filter Out All Grade Placeholders 
			AND ds.batch_uid NOT IN (
				'ENR_181008_02.txt'
				,'ENR_181008'
				,'ENR_181008_1558036.txt'
				);-- Exclude specified batch_uid values
			--Adding to deal with erroneous DSKs added
			--in the SIS Framework cleanup effort
			--IEHR Assignments With No Primary Key (SCORM)

		DECLARE @StartingValue INT;

		SET @StartingValue = COALESCE((
					SELECT MIN(gr.GradePrimaryKey)
					FROM LS_ODS.Grades gr
					WHERE gr.GradePrimaryKey BETWEEN - 514999999
							AND - 514000000
					), - 514000000);

		INSERT INTO stage.Grades (GradePrimaryKey, CourseUsersPrimaryKey, RowStatus, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, ManualDateTime, ExemptIndicator, HighestDateTimeCreated, HighestDateTimeModified, HighestIsLatestAttemptIndicator, FirstScore, FirstGrade, FirstAttemptDateTime, FirstIsLatestAttemptIndicator, FirstDateTimeCreated, FirstDateTimeModified, AssignmentPrimaryKey, AssignmentStatus, SourceSystem)
	SELECT
    @StartingValue - ROW_NUMBER() OVER (ORDER BY gei.UserPK1, gei.CoursePK1, gei.AssignmentPK1) AS 'GradePrimaryKey',
    cu.PK1 AS 'CourseUsersPrimaryKey',
    bs.[Description] AS 'RowStatus',
    gei.GradeDisplayScore AS 'HighestScore',
    gei.GradeDisplayGrade AS 'HighestGrade',
    gei.GradeAttemptDate AS 'HighestAttemptDateTime',
    gei.GradeManualScore AS 'ManualScore',
    gei.GradeManualGrade AS 'ManualGrade',
    gei.GradeOverrideDate AS 'ManualDateTime',
    gei.GradeExemptIndicator AS 'ExemptIndicator',
    ha.DATE_ADDED AS 'HighestDateTimeCreated',
    ha.DATE_MODIFIED AS 'HighestDateTimeModified',
    CASE WHEN gg.HIGHEST_ATTEMPT_PK1 = gg.LAST_ATTEMPT_PK1 THEN 1 ELSE 0 END AS 'HighestIsLatestAttemptIndicator',
    fa.SCORE AS 'FirstScore',
    fa.GRADE AS 'FirstGrade',
    fa.ATTEMPT_DATE AS 'FirstAttemptDateTime',
    CASE WHEN gg.FIRST_ATTEMPT_PK1 = gg.LAST_ATTEMPT_PK1 THEN 1 ELSE 0 END AS 'FirstIsLatestAttemptIndicator',
    fa.DATE_ADDED AS 'FirstDateTimeCreated',
    fa.DATE_MODIFIED AS 'FirstDateTimeModified',
    gei.AssignmentPK1 AS 'AssignmentPrimaryKey',
    CASE WHEN gei.GradeAttemptStatus IS NULL AND gei.GradeAttemptDate IS NULL THEN 'NOT COMPLETE' ELSE gs.[Description] END AS 'AssignmentStatus',
    gei.SourceSystem
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
    AND gei.UserEPK NOT LIKE '%PART[1-5]%' -- More Test Students
    AND gei.AssignmentDisplayColumnName LIKE '%IEHR%' -- IEHR Only
    AND gei.GradePK1 IS NULL -- SCORM IEHR Only
    AND gei.GradeManualGrade IS NOT NULL -- Student Has Completed The Assignment
    AND ds.batch_uid NOT IN (
        'ENR_181008_02.txt',
        'ENR_181008',
        'ENR_181008_1558036.txt'
    ); -- Exclude specific erroneous DSKs

--Adding to deal with erroneous DSKs added
			--in the SIS Framework cleanup effort

		EXEC LS_ODS.AddODSLoadLog 'Loaded Grades Working Table', 0;

		--**************************************************************************************************************************************** 
		--Update the IEHR Assignment statuses in the stage.Grades table 
		--**************************************************************************************************************************************** 
		UPDATE g
		SET g.AssignmentStatus = 'COMPLETED'
		FROM stage.Grades g
		INNER JOIN stage.Assignments a ON g.AssignmentPrimaryKey = a.AssignmentPrimaryKey
			--AND a.AssignmentTitle = 'IEHR Assign' 
			AND g.HighestScore IS NOT NULL
			AND g.HighestScore <> 0;

		EXEC LS_ODS.AddODSLoadLog 'Updated IEHR Assignment Statuses', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with calculated values 
		--**************************************************************************************************************************************** 
		WITH cteLastLogins (SyStudentId, LastLoginDateTime)
		AS (
			SELECT jq.SyStudentId, MAX(jq.LastLoginDateTime) 'LastLoginDateTime'
			FROM (
				--SELECT 
				--	sal.SyStudentID 'SyStudentId', 
				--	MAX(sal.EventTime) 'LastLoginDateTime' 
				--FROM RTSATWeb.dbo.StudentActivityLog sal WITH(NOLOCK) 
				--WHERE EventId = 1 
				--GROUP BY 
				--	sal.SyStudentID 
				--UNION ALL 
				SELECT us.SyStudentId 'SyStudentId', MAX(lo.LoginDateTime) 'LastLoginDateTime'
				FROM RTSAT.[Login] lo
				INNER JOIN RTSAT.[User] us ON lo.UserPK = us.UserPK
				GROUP BY us.SyStudentId
				) AS jq
			GROUP BY jq.SyStudentId
			)
		UPDATE s
		SET s.LastLoginDateTime = ll.LastLoginDateTime
		FROM stage.Students s
		INNER JOIN cteLastLogins ll ON s.SyStudentId = ll.SyStudentId;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Last Logins', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the time in class 
		--**************************************************************************************************************************************** 
		DECLARE @FilterDate DATE;

		SET @FilterDate = DATEADD(DAY, - 90, GETDATE());

		--Check for temp table and delete if it exists 
		IF OBJECT_ID('tempdb..#TimeInClassTemp') IS NOT NULL
			DROP TABLE #TimeInClassTemp;

		CREATE TABLE #TimeInClassTemp (USER_PK1 INT, COURSE_PK1 INT, [DayOfWeek] INT, TimeInClass NUMERIC(12, 2));

		WITH cteTimeInClass (USER_PK1, COURSE_PK1, [DayOfWeek], TIME_IN_CLASS)
		AS (
			SELECT iq.USER_PK1, iq.COURSE_PK1, iq.[DayOfWeek], CAST(SUM(DATEDIFF(ss, iq.SESSION_START, iq.SESSION_END)) AS NUMERIC(36, 12)) / CAST(3600 AS NUMERIC(36, 12)) 'TIME_IN_CLASS'
			FROM (
				SELECT aa.USER_PK1, aa.COURSE_PK1, DATEPART(WEEKDAY, aa.[TIMESTAMP]) 'DayOfWeek', aa.SESSION_ID, MIN(aa.[TIMESTAMP]) SESSION_START, MAX(aa.[TIMESTAMP]) SESSION_END
				FROM ACTIVITY_ACCUMULATOR aa
				WHERE aa.COURSE_PK1 IS NOT NULL
					AND aa.USER_PK1 IS NOT NULL
					AND aa.[TIMESTAMP] >= @FilterDate
				GROUP BY aa.USER_PK1, aa.COURSE_PK1, aa.SESSION_ID, DATEPART(WEEKDAY, aa.[TIMESTAMP])
				) iq
			GROUP BY iq.USER_PK1, iq.COURSE_PK1, iq.[DayOfWeek]
			)
		INSERT INTO #TimeInClassTemp (USER_PK1, COURSE_PK1, [DayOfWeek], TimeInClass)
		SELECT tic.USER_PK1, tic.COURSE_PK1, tic.[DayOfWeek], tic.TIME_IN_CLASS
		FROM cteTimeInClass tic;

		WITH cteTotal
		AS (
			SELECT tic.USER_PK1, tic.COURSE_PK1, SUM(tic.TimeInClass) 'TotalTimeInClass'
			FROM #TimeInClassTemp tic
			GROUP BY tic.USER_PK1, tic.COURSE_PK1
			)
		UPDATE s
		SET s.TimeInClass = tic.TotalTimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN cteTotal tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1;

		UPDATE s
		SET s.MondayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 2;

		UPDATE s
		SET s.TuesdayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 3;

		UPDATE s
		SET s.WednesdayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 4;

		UPDATE s
		SET s.ThursdayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 5;

		UPDATE s
		SET s.FridayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 6;

		UPDATE s
		SET s.SaturdayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 7;

		UPDATE s
		SET s.SundayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 1;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Times In Class', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with I3 interaction information 
		--**************************************************************************************************************************************** 
		--Define needed variables 
		DECLARE @I3CurrentDateTime DATETIME;
		DECLARE @LastUpdatedDateTime DATETIME;
		DECLARE @RemoteQuery NVARCHAR(4000);

		--Populate needed variables 
		SET @I3CurrentDateTime = GETDATE();

		--Create table to hold new/updated calls 
		DECLARE @Calls TABLE (PhoneNumber VARCHAR(50) NOT NULL PRIMARY KEY, LastInteractionDateTime DATETIME, SourceSystem VARCHAR(50) NULL);
		DECLARE @CallsBTB TABLE (PhoneNumber VARCHAR(50) NOT NULL PRIMARY KEY, LastInteractionDateTime DATETIME);
		DECLARE @CallsMCS TABLE (PhoneNumber VARCHAR(50) NOT NULL PRIMARY KEY, LastInteractionDateTime DATETIME);
		DECLARE @CallsCombined TABLE (PhoneNumber VARCHAR(50) NOT NULL, LastInteractionDateTime DATETIME, SourceSystem VARCHAR(50) NOT NULL, PRIMARY KEY (PhoneNumber, SourceSystem));

		--Get the most recent time the I3 Interactions was updated 
		SET @LastUpdatedDateTime = (
				SELECT MAX(i3.LastUpdatedDateTime)
				FROM LS_ODS.I3Interactions i3
				);

		--Back to Basics (BTB) Interaction Data
		INSERT INTO @CallsBTB (PhoneNumber, LastInteractionDateTime)
		SELECT REPLACE(btbcalldetail.RemoteNumber, '+', '') 'PhoneNumber', MAX(btbcalldetail.InitiatedDate) 'LastInterationDateTime'
		--FROM [MLK-TEL-D-SQ03].I3_IC_TEST.dbo.CallDetail_viw btbcalldetail															--UAT 
		--FROM [MLK-TEL-D-SQ03].I3_IC_DEV.dbo.CallDetail_viw btbcalldetail															--DEV 
		FROM [COL-TEL-P-SQ01].I3_IC_PROD.dbo.CallDetail_viw btbcalldetail --PROD  
		WHERE btbcalldetail.CallType = 'External'
			AND RTRIM(LTRIM(btbcalldetail.RemoteNumber)) <> ''
			AND btbcalldetail.CallDurationSeconds >= 90
			AND LEN(REPLACE(btbcalldetail.RemoteNumber, '+', '')) = 10
			AND ISNUMERIC(REPLACE(btbcalldetail.RemoteNumber, '+', '')) = 1
			AND btbcalldetail.InitiatedDate >= @LastUpdatedDateTime
		GROUP BY REPLACE(btbcalldetail.RemoteNumber, '+', '');

		--MCS Interaction Data
		SELECT @RemoteQuery = '
                           SELECT MAX(DATEADD(SECOND, I.StartDTOffset, I.InitiatedDateTimeUTC)) AS LastInterationDateTime
                                        ,CASE WHEN LEN(REPLACE(I.RemoteID, ''+'', '''')) = 0 OR REPLACE(I.RemoteID, ''+'', '''') IS NULL THEN ''-'' ELSE I.RemoteID END as RemoteNumber                                      
                           FROM    MCS_I3_IC.dbo.InteractionSummary I
                           where DATEADD(SECOND, I.StartDTOffset, I.InitiatedDateTimeUTC) > ''' + CAST(@LastUpdatedDateTime AS NVARCHAR(25)) + '''
                           AND ConnectedDateTimeUTC > ''' + CAST(@LastUpdatedDateTime AS NVARCHAR(25)) + '''
                           AND TerminatedDateTimeUTC > ''' + CAST(@LastUpdatedDateTime AS NVARCHAR(25)) + 
			'''
                           AND I.ConnectionType = 1
                           AND LEN(I.RemoteID) > 0
                           AND LEN(REPLACE(I.RemoteID, ''+'', '''')) = 10 
                           AND ISNUMERIC(REPLACE(I.RemoteID, ''+'', '''')) = 1 
                           AND CAST(ROUND(DATEDIFF(MILLISECOND, ConnectedDateTimeUTC, TerminatedDateTimeUTC) / 1000.000,0) AS BIGINT) > 90
                           and DATEDIFF(DAY, ConnectedDateTimeUTC, TerminatedDateTimeUTC)  < 23
                           GROUP BY I.RemoteID 
                           '

		INSERT INTO @CallsMCS (LastInteractionDateTime, PhoneNumber)
		EXEC [COL-MCS-P-SQ02].master.dbo.sp_executesql @Remotequery;

		--Add the new/updated calls into the table variable 
		INSERT INTO @CallsCombined (PhoneNumber, LastInteractionDateTime, SourceSystem)
		SELECT PhoneNumber, LastInteractionDateTime, 'BTB'
		FROM @CallsBTB
		
		UNION ALL
		
		SELECT PhoneNumber, LastInteractionDateTime, 'MCS'
		FROM @CallsMCS;

		--Add the new/updated calls into the table variable 
		WITH cteCalls
		AS (
			SELECT cc.PhoneNumber 'PhoneNumber', MAX(cc.LastInteractionDateTime) 'LastInterationDateTime'
			FROM @CallsCombined cc
			GROUP BY cc.PhoneNumber
			)
		INSERT INTO @Calls (PhoneNumber, LastInteractionDateTime, SourceSystem)
		SELECT cc.PhoneNumber, cc.LastInteractionDateTime, cc.SourceSystem
		FROM @CallsCombined cc
		INNER JOIN cteCalls ca ON cc.PhoneNumber = ca.PhoneNumber
			AND cc.LastInteractionDateTime = ca.LastInterationDateTime;

		--Update the phone numbers that have a new interaction date/time 
		UPDATE i3
		SET i3.LastInteractionDateTime = c.LastInteractionDateTime, i3.SourceSystem = c.SourceSystem, i3.LastUpdatedDateTime = @I3CurrentDateTime
		FROM LS_ODS.I3Interactions i3
		INNER JOIN @Calls c ON i3.PhoneNumber = c.PhoneNumber;

		--Add new phone numbers that don't exist in the interactions table 
		INSERT INTO LS_ODS.I3Interactions (
			PhoneNumber, LastInteractionDateTime, SourceSystem,
			--added the SourceSystem column to the table to track source of the interaction data
			LastUpdatedDateTime
			)
		SELECT c.PhoneNumber, c.LastInteractionDateTime, c.SourceSystem,
			--added the SourceSystem column to the table to track source of the interaction data
			@I3CurrentDateTime
		FROM @Calls c
		WHERE c.PhoneNumber NOT IN (
				SELECT i3.PhoneNumber
				FROM LS_ODS.I3Interactions i3
				);

		UPDATE s
		SET s.LastI3InteractionNumberMainPhone = mpi.PhoneNumber, s.LastI3InteractionDateTimeMainPhone = mpi.LastInteractionDateTime, s.DaysSinceLastI3InteractionMainPhone = DATEDIFF(DAY, mpi.LastInteractionDateTime, @CurrentDateTime), s.LastI3InteractionNumberWorkPhone = wpi.PhoneNumber, s.LastI3InteractionDateTimeWorkPhone = wpi.LastInteractionDateTime, s.DaysSinceLastI3InteractionWorkPhone = DATEDIFF(DAY, wpi.LastInteractionDateTime, @CurrentDateTime), s.LastI3InteractionNumberMobilePhone = mopi.PhoneNumber, s.LastI3InteractionDateTimeMobilePhone = mopi.LastInteractionDateTime, s.DaysSinceLastI3InteractionMobilePhone = DATEDIFF(DAY, mopi.LastInteractionDateTime, @CurrentDateTime), s.LastI3InteractionNumberOtherPhone = opi.PhoneNumber, s.LastI3InteractionDateTimeOtherPhone = opi.LastInteractionDateTime, s.DaysSinceLastI3InteractionOtherPhone = DATEDIFF(DAY, opi.LastInteractionDateTime, @CurrentDateTime)
		FROM stage.Students s
		INNER JOIN CV_Prod.dbo.SyStudent cvs ON s.SyStudentID = cvs.SyStudentId
		LEFT JOIN LS_ODS.I3Interactions mpi ON RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cvs.Phone, '-', ''), '*', ''), '(', ''), ')', ''), ' ', ''))) = mpi.PhoneNumber
		LEFT JOIN LS_ODS.I3Interactions wpi ON RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cvs.WorkPhone, '-', ''), '*', ''), '(', ''), ')', ''), ' ', ''))) = wpi.PhoneNumber
		LEFT JOIN LS_ODS.I3Interactions mopi ON RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cvs.MobileNumber, '-', ''), '*', ''), '(', ''), ')', ''), ' ', ''))) = mopi.PhoneNumber
		LEFT JOIN LS_ODS.I3Interactions opi ON RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cvs.OtherPhone, '-', ''), '*', ''), '(', ''), ')', ''), ' ', ''))) = opi.PhoneNumber;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Last I3 Interactions', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the weekly grades 
		--NEED TO PERFORM ANALYSIS TO ACCOUNT FOR 16 WEEK EMT PROGRAM -cml 2/28/2024
		--stage.Courses only has columns for weeks 1 - 5 -cml 2/28/2024
		--EMT has a week 6 grade and a Final Percentage only, only Final Percentage will be placed in week 5 based on current logic -cml 2/28/2024
		--**************************************************************************************************************************************** 
		WITH cteWeeklyGrades (StudentPrimaryKey, CoursePrimaryKey, WeekNumber, WeeklyGrade)
		AS (
			SELECT gei.UserPK1 'StudentPrimaryKey', gei.CoursePK1 'CoursePrimaryKey', CASE 
					WHEN gei.AssignmentDisplayTitle IN (
							'Week 1 Grade %'
							,'Week 1 Grade (%)'
							)
						THEN 1
					WHEN gei.AssignmentDisplayTitle IN (
							'Week 2 Grade %'
							,'Week 2 Grade (%)'
							)
						THEN 2
					WHEN gei.AssignmentDisplayTitle IN (
							'Week 3 Grade %'
							,'Week 3 Grade (%)'
							)
						THEN 3
					WHEN gei.AssignmentDisplayTitle IN (
							'Week 4 Grade %'
							,'Week 4 Grade (%)'
							)
						THEN 4
					ELSE 5
					END 'WeekNumber', (CAST(gei.GradeManualScore AS NUMERIC(12, 2)) / CAST(REPLACE(gei.AssignmentPointsPossible, '"', '') AS NUMERIC(12, 2))) 'WeeklyGrade'
			FROM stage.GradeExtractImport gei
			WHERE gei.AssignmentDisplayTitle IN (
					'Week 1 Grade %'
					,'Week 2 Grade %'
					,'Week 3 Grade %'
					,'Week 4 Grade %'
					,'Week 1 Grade (%)'
					,'Week 2 Grade (%)'
					,'Week 3 Grade (%)'
					,'Week 4 Grade (%)'
					,'Final Percentage'
					)
				AND CAST(REPLACE(gei.AssignmentPointsPossible, '"', '') AS NUMERIC(12, 2)) <> 0
			)
		UPDATE s
		SET s.Week1Grade = w1.WeeklyGrade, s.Week2Grade = w2.WeeklyGrade, s.Week3Grade = w3.WeeklyGrade, s.Week4Grade = w4.WeeklyGrade, s.Week5Grade = w5.WeeklyGrade
		FROM stage.Students s
		INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
		LEFT JOIN cteWeeklyGrades w1 ON s.StudentPrimaryKey = w1.StudentPrimaryKey
			AND c.CoursePrimaryKey = w1.CoursePrimaryKey
			AND w1.WeekNumber = 1
		LEFT JOIN cteWeeklyGrades w2 ON s.StudentPrimaryKey = w2.StudentPrimaryKey
			AND c.CoursePrimaryKey = w2.CoursePrimaryKey
			AND w2.WeekNumber = 2
		LEFT JOIN cteWeeklyGrades w3 ON s.StudentPrimaryKey = w3.StudentPrimaryKey
			AND c.CoursePrimaryKey = w3.CoursePrimaryKey
			AND w3.WeekNumber = 3
		LEFT JOIN cteWeeklyGrades w4 ON s.StudentPrimaryKey = w4.StudentPrimaryKey
			AND c.CoursePrimaryKey = w4.CoursePrimaryKey
			AND w4.WeekNumber = 4
		LEFT JOIN cteWeeklyGrades w5 ON s.StudentPrimaryKey = w5.StudentPrimaryKey
			AND c.CoursePrimaryKey = w5.CoursePrimaryKey
			AND w5.WeekNumber = 5;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Weekly Grades', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the count of practice exercises, tests, and assignments 
		--**************************************************************************************************************************************** 
		WITH cteCounts (CoursePrimaryKey, StudentPrimaryKey, PracticeExercisesCount, TestsCount, AssignmentsCount)
		AS (
			SELECT cm.PK1 'CoursePrimaryKey', cu.USERS_PK1 'StudentPrimaryKey', SUM(CASE 
						WHEN gm.TITLE LIKE '%Practice Exercise%'
							THEN 1
						ELSE 0
						END) 'PracticeExercisesCount', SUM(CASE 
						WHEN gm.TITLE LIKE '%Test%'
							THEN 1
						ELSE 0
						END) 'TestsCount', SUM(CASE 
						WHEN gm.TITLE LIKE '%Assignment%'
							THEN 1
						ELSE 0
						END) 'AssignmentsCount'
			FROM dbo.ATTEMPT a
			INNER JOIN GRADEBOOK_GRADE gg ON a.GRADEBOOK_GRADE_PK1 = gg.PK1
			INNER JOIN GRADEBOOK_MAIN gm ON gg.GRADEBOOK_MAIN_PK1 = gm.PK1
				AND gm.PK1 NOT IN (
					SELECT PK1
					FROM GRADEBOOK_MAIN
					WHERE TITLE LIKE '%IEHR%'
						AND COURSE_CONTENTS_PK1 IS NULL
					)
			INNER JOIN COURSE_USERS cu ON gg.COURSE_USERS_PK1 = cu.PK1
			INNER JOIN COURSE_MAIN cm ON cu.CRSMAIN_PK1 = cm.PK1
			GROUP BY cm.PK1, cu.USERS_PK1
			)
		UPDATE s
		SET s.SelfTestsCount = co.PracticeExercisesCount, s.AssessmentsCount = co.TestsCount, s.AssignmentsCount = co.AssignmentsCount
		FROM stage.Students s
		INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
		INNER JOIN cteCounts co ON s.StudentPrimaryKey = co.StudentPrimaryKey
			AND c.CoursePrimaryKey = co.CoursePrimaryKey;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Counts Of Practice Exercises, Tests And Assignments', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the count of discussion posts 
		--**************************************************************************************************************************************** 
		WITH cteCounts (CoursePrimaryKey, StudentPrimaryKey, DiscussionsCount)
		AS (
			SELECT cm.CRSMAIN_PK1 'CoursePrimaryKey', mm.USERS_PK1 'StudentPrimaryKey', COUNT(mm.PK1) 'DiscussionsCount'
			FROM MSG_MAIN mm
			INNER JOIN FORUM_MAIN fm ON mm.FORUMMAIN_PK1 = fm.PK1
			INNER JOIN CONFERENCE_MAIN cm ON fm.CONFMAIN_PK1 = cm.PK1
			GROUP BY cm.CRSMAIN_PK1, mm.USERS_PK1
			)
		UPDATE s
		SET s.DiscussionsCount = co.DiscussionsCount
		FROM stage.Students s
		INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
		INNER JOIN cteCounts co ON s.StudentPrimaryKey = co.StudentPrimaryKey
			AND c.CoursePrimaryKey = co.CoursePrimaryKey;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Counts Of Discussion Posts', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Courses table with section start date, section end date and the course week number 
		--**************************************************************************************************************************************** 
		UPDATE c
		SET c.SectionStart = cs.StartDate, c.SectionEnd = cs.EndDate, c.WeekNumber = CASE 
				WHEN DATEDIFF(WEEK, cs.StartDate, @CurrentDateTime) + 1 >= 7
					THEN 7
				ELSE DATEDIFF(WEEK, cs.StartDate, @CurrentDateTime) + 1
				END, c.DayNumber = CASE 
				WHEN DATEDIFF(DAY, cs.StartDate, @CurrentDateTime) >= 49
					THEN 49
				ELSE DATEDIFF(DAY, cs.StartDate, @CurrentDateTime)
				END
		FROM stage.Courses c
		INNER JOIN CV_Prod.dbo.AdClassSched cs ON c.AdClassSchedId = cs.AdClassSchedID;

		--**************************************************************************************************************************************** 
		--Update the stage.Grades table with Cengage values
		--**************************************************************************************************************************************** 
		UPDATE co
		SET co.CengageCourseIndicator = 1
		FROM stage.Courses co
		INNER JOIN Cengage.CourseLookup cl ON co.CourseCode = cl.CourseCode
			AND co.SectionStart BETWEEN cl.StartDate
				AND cl.EndDate;

		--Create a table to hold the holiday schedule as defined by CampusVue and populate it with the Christmas Break Online values 
		DECLARE @Holidays TABLE (StartDate DATE, EndDate DATE, WeeksOff INT);

		INSERT INTO @Holidays (StartDate, EndDate, WeeksOff)
		SELECT ca.StartDate, ca.EndDate, ((DATEDIFF(DAY, ca.StartDate, ca.EndDate) + 1) / 7) 'WeeksOff'
		FROM CV_Prod.dbo.AdCalendar ca
		INNER JOIN CV_Prod.dbo.SyCampusList cl ON ca.SyCampusGrpID = cl.SyCampusGrpID
		WHERE cl.SyCampusID = 9
			AND LEFT(ca.Code, 2) = 'CB'
		ORDER BY ca.StartDate DESC;

		--SELECT * FROM @Holidays; 
		--Update the stage.Courses table to remove holiday weeks before any further proceesing continues
		DECLARE @HolidayDateCheck DATETIME;

		SET @HolidayDateCheck = DATEADD(DAY, 0, DATEDIFF(DAY, 0, GETDATE()));

		UPDATE co
		SET co.WeekNumber = co.WeekNumber - CASE 
				WHEN @HolidayDateCheck < ho.StartDate
					THEN 0
				WHEN @HolidayDateCheck >= ho.StartDate
					AND @HolidayDateCheck <= ho.EndDate
					THEN ls_co.WeekNumber
				WHEN @HolidayDateCheck > ho.EndDate
					THEN ho.WeeksOff
				ELSE 0
				END
		FROM stage.Courses co
		INNER JOIN @Holidays ho ON co.SectionStart <= ho.StartDate
			AND co.SectionEnd >= ho.EndDate
		INNER JOIN LS_ODS.Courses ls_co ON co.CoursePrimaryKey = ls_co.CoursePrimaryKey
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7

		--AND DATEDIFF(DAY, co.SectionStart, GETDATE()) < 50; 
		EXEC LS_ODS.AddODSLoadLog 'Updated Course Start Dates And Week Numbers', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Courses table with week x start date and extension week start date 
		--**************************************************************************************************************************************** 
		--Set the basic start dates 
		UPDATE stage.Courses
		SET Week1StartDate = SectionStart, Week2StartDate = DATEADD(DAY, CASE DATEPART(WEEKDAY, SectionStart)
					WHEN 2
						THEN 7
					WHEN 3
						THEN 6
					ELSE 9999
					END, SectionStart), Week3StartDate = DATEADD(DAY, CASE DATEPART(WEEKDAY, SectionStart)
					WHEN 2
						THEN 14
					WHEN 3
						THEN 13
					ELSE 9999
					END, SectionStart), Week4StartDate = DATEADD(DAY, CASE DATEPART(WEEKDAY, SectionStart)
					WHEN 2
						THEN 21
					WHEN 3
						THEN 20
					ELSE 9999
					END, SectionStart), Week5StartDate = DATEADD(DAY, CASE DATEPART(WEEKDAY, SectionStart)
					WHEN 2
						THEN 28
					WHEN 3
						THEN 27
					ELSE 9999
					END, SectionStart), ExtensionWeekStartDate = DATEADD(DAY, CASE DATEPART(WEEKDAY, SectionStart)
					WHEN 2
						THEN 35
					WHEN 3
						THEN 34
					ELSE 9999
					END, SectionStart);

		--Modify for holidays: processed in reverse (week 5 to week 1) to have correct dates to check 
		DECLARE @HolidayDateCheck DATETIME;

		SET @HolidayDateCheck = DATEADD(DAY, 0, DATEDIFF(DAY, 0, GETDATE()));
		UPDATE co
		SET co.Week5StartDate = DATEADD(WEEK, CASE 
					WHEN (
							co.Week1StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week2StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week3StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week4StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week5StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							)
						THEN ho.WeeksOff
					ELSE 0
					END, co.Week5StartDate), co.ExtensionWeekStartDate = DATEADD(WEEK, CASE 
					WHEN (
							co.Week1StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week2StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week3StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week4StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week5StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							)
						THEN ho.WeeksOff
					ELSE 0
					END, co.ExtensionWeekStartDate)
		FROM stage.Courses co
		INNER JOIN @Holidays ho ON co.SectionStart <= ho.StartDate
			AND co.SectionEnd >= ho.EndDate
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7

		--AND DATEDIFF(DAY, co.SectionStart, GETDATE()) < 50; 
		UPDATE co
		SET co.Week4StartDate = DATEADD(WEEK, CASE 
					WHEN (
							co.Week1StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week2StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week3StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week4StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							)
						THEN ho.WeeksOff
					ELSE 0
					END, co.Week4StartDate)
		FROM stage.Courses co
		INNER JOIN @Holidays ho ON co.SectionStart <= ho.StartDate
			AND co.SectionEnd >= ho.EndDate
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7

		--AND DATEDIFF(DAY, co.SectionStart, GETDATE()) < 50; 
		UPDATE co
		SET co.Week3StartDate = DATEADD(WEEK, CASE 
					WHEN (
							co.Week1StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week2StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week3StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							)
						THEN ho.WeeksOff
					ELSE 0
					END, co.Week3StartDate)
		FROM stage.Courses co
		INNER JOIN @Holidays ho ON co.SectionStart <= ho.StartDate
			AND co.SectionEnd >= ho.EndDate
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7

		--AND DATEDIFF(DAY, co.SectionStart, GETDATE()) < 50; 
		UPDATE co
		SET co.Week2StartDate = DATEADD(WEEK, CASE 
					WHEN (
							co.Week1StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week2StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							)
						THEN ho.WeeksOff
					ELSE 0
					END, co.Week2StartDate)
		FROM stage.Courses co
		INNER JOIN @Holidays ho ON co.SectionStart <= ho.StartDate
			AND co.SectionEnd >= ho.EndDate
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7

		--AND DATEDIFF(DAY, co.SectionStart, GETDATE()) < 50; 
		UPDATE co
		SET co.Week1StartDate = DATEADD(WEEK, CASE 
					WHEN co.Week1StartDate BETWEEN ho.StartDate
							AND ho.EndDate
						THEN ho.WeeksOff
					ELSE 0
					END, co.Week1StartDate)
		FROM stage.Courses co
		INNER JOIN @Holidays ho ON co.SectionStart <= ho.StartDate
			AND co.SectionEnd >= ho.EndDate
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7

		--AND DATEDIFF(DAY, co.SectionStart, GETDATE()) < 50; 
		EXEC LS_ODS.AddODSLoadLog 'Updated Course Week X Start Dates And Extension Week Start Date', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Assignments table with the IsReportable and CountsAsSubmission values 
		--**************************************************************************************************************************************** 
		UPDATE a
		SET a.IsReportable = 0
		FROM stage.Assignments a
		INNER JOIN LS_ODS.AssignmentDetails ad ON a.AssignmentTitle = ad.AssignmentTitle
			AND ad.IsReportable = 0;

		UPDATE a
		SET a.CountsAsSubmission = 0
		FROM stage.Assignments a
		INNER JOIN LS_ODS.AssignmentDetails ad ON a.AssignmentTitle = ad.AssignmentTitle
			AND ad.CountsAsSubmission = 0;

		EXEC LS_ODS.AddODSLoadLog 'Updated Assignments IsReportable And CountsAsSubmission Flags', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Courses table with the weekly assignment counts 
		--**************************************************************************************************************************************** 
		WITH cteCounts (CoursePrimaryKey, WeekNumber, AssignmentCount)
		AS (
			SELECT a.CoursePrimaryKey 'CoursePrimaryKey', a.WeekNumber 'WeekNumber', COUNT(a.AssignmentPrimaryKey) 'AssignmentCount'
			FROM stage.Assignments a
			WHERE a.WeekNumber <> 0 --Filter out assignments that are not part of a week 
				AND a.CountsAsSubmission = 1
			GROUP BY a.CoursePrimaryKey, a.WeekNumber
			)
		UPDATE c
		SET c.Week1AssignmentCount = c1.AssignmentCount, c.Week2AssignmentCount = c2.AssignmentCount, c.Week3AssignmentCount = c3.AssignmentCount, c.Week4AssignmentCount = c4.AssignmentCount, c.Week5AssignmentCount = c5.AssignmentCount
		FROM stage.Courses c
		LEFT JOIN cteCounts c1 ON c.CoursePrimaryKey = c1.CoursePrimaryKey
			AND c1.WeekNumber = 1
		LEFT JOIN cteCounts c2 ON c.CoursePrimaryKey = c2.CoursePrimaryKey
			AND c2.WeekNumber = 2
		LEFT JOIN cteCounts c3 ON c.CoursePrimaryKey = c3.CoursePrimaryKey
			AND c3.WeekNumber = 3
		LEFT JOIN cteCounts c4 ON c.CoursePrimaryKey = c4.CoursePrimaryKey
			AND c4.WeekNumber = 4
		LEFT JOIN cteCounts c5 ON c.CoursePrimaryKey = c5.CoursePrimaryKey
			AND c5.WeekNumber = 5;

		EXEC LS_ODS.AddODSLoadLog 'Updated Course Weekly Assignment Counts', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the weekly completed assignment counts and submission rates 
		--**************************************************************************************************************************************** 
		WITH cteCounts (CourseUsersPrimaryKey, WeekNumber, GradeCount)
		AS (
			SELECT g.CourseUsersPrimaryKey 'CourseUsersPrimaryKey', a.WeekNumber 'WeekNumber', COUNT(g.GradePrimaryKey) 'GradeCount'
			FROM stage.Grades g
			INNER JOIN stage.Assignments a ON g.AssignmentPrimaryKey = a.AssignmentPrimaryKey
				AND a.WeekNumber <> 0 --Filter out assignments that are not part of a week 
				AND a.CountsAsSubmission = 1
			WHERE g.AssignmentStatus IN (
					'NEEDS GRADING'
					,'COMPLETED'
					,'IN MORE PROGRESS'
					,'NEEDS MORE GRADING'
					)
				OR (
					a.AlternateTitle LIKE '%Disc%'
					AND g.AssignmentStatus = 'IN PROGRESS'
					)
			GROUP BY g.CourseUsersPrimaryKey, a.WeekNumber
			)
		UPDATE s
		SET s.Week1CompletedAssignments = w1.GradeCount, s.Week2CompletedAssignments = w2.GradeCount, s.Week3CompletedAssignments = w3.GradeCount, s.Week4CompletedAssignments = w4.GradeCount, s.Week5CompletedAssignments = w5.GradeCount, s.Week1CompletionRate = CAST(w1.GradeCount AS NUMERIC(12, 2)) / CAST(c.Week1AssignmentCount AS NUMERIC(12, 2)), s.Week2CompletionRate = CAST(w2.GradeCount AS NUMERIC(12, 2)) / CAST(c.Week2AssignmentCount AS NUMERIC(12, 2)), s.Week3CompletionRate = CAST(w3.GradeCount AS NUMERIC(12, 2)) / CAST(c.Week3AssignmentCount AS NUMERIC(12, 2)), s.Week4CompletionRate = CAST(w4.GradeCount AS NUMERIC(12, 2)) / CAST(c.Week4AssignmentCount AS NUMERIC(12, 2)), s.Week5CompletionRate = CAST(w5.GradeCount AS NUMERIC(12, 2)) / CAST(c.Week5AssignmentCount AS NUMERIC(12, 2)), s.CoursePercentage = CAST((COALESCE(w1.GradeCount, 0) + COALESCE(w2.GradeCount, 0) + COALESCE(w3.GradeCount, 0) + COALESCE(w4.GradeCount, 0) + COALESCE(w5.GradeCount, 0)) AS NUMERIC(12, 2)) / CAST((c.Week1AssignmentCount + c.Week2AssignmentCount + c.Week3AssignmentCount + c.Week4AssignmentCount + c.Week5AssignmentCount
					) AS NUMERIC(12, 2))
		FROM stage.Students s
		INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
		LEFT JOIN cteCounts w1 ON s.CourseUsersPrimaryKey = w1.CourseUsersPrimaryKey
			AND w1.WeekNumber = 1
		LEFT JOIN cteCounts w2 ON s.CourseUsersPrimaryKey = w2.CourseUsersPrimaryKey
			AND w2.WeekNumber = 2
		LEFT JOIN cteCounts w3 ON s.CourseUsersPrimaryKey = w3.CourseUsersPrimaryKey
			AND w3.WeekNumber = 3
		LEFT JOIN cteCounts w4 ON s.CourseUsersPrimaryKey = w4.CourseUsersPrimaryKey
			AND w4.WeekNumber = 4
		LEFT JOIN cteCounts w5 ON s.CourseUsersPrimaryKey = w5.CourseUsersPrimaryKey
			AND w5.WeekNumber = 5;

		WITH cteTotalWork (SyStudentId, SectionStart, CompletedAssignments, TotalAssignments)
		AS (
			SELECT s.SyStudentId, c.SectionStart, SUM(CAST(COALESCE(s.Week1CompletedAssignments, 0) + COALESCE(s.Week2CompletedAssignments, 0) + COALESCE(s.Week3CompletedAssignments, 0) + COALESCE(s.Week4CompletedAssignments, 0) + COALESCE(s.Week5CompletedAssignments, 0) AS NUMERIC(12, 2))) 'CompletedAssignments', SUM(CAST(c.Week1AssignmentCount + c.Week2AssignmentCount + c.Week3AssignmentCount + c.Week4AssignmentCount + c.Week5AssignmentCount AS NUMERIC(12, 2))) 'TotalAssignments'
			FROM stage.Students s
			INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
				AND c.SectionStart IS NOT NULL
			GROUP BY s.SyStudentId, c.SectionStart
			)
		UPDATE s
		SET s.TotalWorkPercentage = tw.CompletedAssignments / tw.TotalAssignments
		FROM stage.Students s
		INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
		INNER JOIN cteTotalWork tw ON s.SyStudentId = tw.SyStudentId
			AND c.SectionStart = tw.SectionStart;

		UPDATE st
		SET st.Week1CompletionRateFixed = CASE 
				WHEN co.DayNumber BETWEEN 0
						AND 6
					THEN st.Week1CompletionRate
				ELSE st1.Week1CompletionRateFixed
				END, st.Week2CompletionRateFixed = CASE 
				WHEN co.DayNumber BETWEEN 7
						AND 13
					THEN st.Week2CompletionRate
				ELSE st1.Week2CompletionRateFixed
				END, st.Week3CompletionRateFixed = CASE 
				WHEN co.DayNumber BETWEEN 14
						AND 20
					THEN st.Week3CompletionRate
				ELSE st1.Week3CompletionRateFixed
				END, st.Week4CompletionRateFixed = CASE 
				WHEN co.DayNumber BETWEEN 21
						AND 27
					THEN st.Week4CompletionRate
				ELSE st1.Week4CompletionRateFixed
				END, st.Week5CompletionRateFixed = CASE 
				WHEN co.DayNumber BETWEEN 28
						AND 34
					THEN st.Week5CompletionRate
				ELSE st1.Week5CompletionRateFixed
				END
		FROM stage.Students st
		INNER JOIN stage.Courses co ON st.AdClassSchedId = co.AdClassSchedId
		LEFT JOIN LS_ODS.Students st1 ON st.SyStudentId = st1.SyStudentId
			AND st.AdClassSchedId = st1.AdClassSchedId;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Counts Of Completed Assignments And Submission Rates', 0;

		--**************************************************************************************************************************************** 
		--Update completion/submission rates by assignment type 
		--****************************************************************************************************************************************		
		EXEC LS_ODS.ProcessStudentRatesByAssignmentType;

		EXEC LS_ODS.AddODSLoadLog 'Updated Completion/Submission Rates By Assignment Type', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the weekly LDAs 
		--**************************************************************************************************************************************** 
		--Get value from current table 
		UPDATE s
		SET s.Week1LDA = sp.Week1LDA, s.Week2LDA = sp.Week2LDA, s.Week3LDA = sp.Week3LDA, s.Week4LDA = sp.Week4LDA, s.Week5LDA = sp.Week5LDA
		FROM stage.Students s
		INNER JOIN LS_ODS.Students sp ON s.SyStudentId = sp.SyStudentId
			AND s.AdEnrollSchedId = sp.AdEnrollSchedId
			AND sp.ActiveFlag = 1;

		DECLARE @WeeklyLDAs TABLE (SyStudentId INT, AdEnrollSchedId INT, WeekNumber INT, LDA DATE);

		--Get new values 
		INSERT INTO @WeeklyLDAs (SyStudentId, AdEnrollSchedId, WeekNumber, LDA)
		SELECT es.SyStudentId 'SyStudentId', es.AdEnrollSchedID 'AdEnrollSchedId', c.WeekNumber 'WeekNumber', es.LDA 'LDA'
		FROM CV_PROD.dbo.AdEnrollSched es WITH (NOLOCK)
		INNER JOIN stage.Courses c ON es.AdClassSchedID = c.AdClassSchedId;

		--Update Week 1 
		UPDATE s
		SET s.Week1LDA = wk.LDA
		FROM stage.Students s
		INNER JOIN @WeeklyLDAs wk ON s.SyStudentId = wk.SyStudentId
			AND s.AdEnrollSchedId = wk.AdEnrollSchedId
			AND wk.WeekNumber = 1;

		--Update Week 2 
		UPDATE s
		SET s.Week2LDA = wk.LDA
		FROM stage.Students s
		INNER JOIN @WeeklyLDAs wk ON s.SyStudentId = wk.SyStudentId
			AND s.AdEnrollSchedId = wk.AdEnrollSchedId
			AND wk.WeekNumber = 2;

		--Update Week 3 
		UPDATE s
		SET s.Week3LDA = wk.LDA
		FROM stage.Students s
		INNER JOIN @WeeklyLDAs wk ON s.SyStudentId = wk.SyStudentId
			AND s.AdEnrollSchedId = wk.AdEnrollSchedId
			AND wk.WeekNumber = 3;

		--Update Week 4 
		UPDATE s
		SET s.Week4LDA = wk.LDA
		FROM stage.Students s
		INNER JOIN @WeeklyLDAs wk ON s.SyStudentId = wk.SyStudentId
			AND s.AdEnrollSchedId = wk.AdEnrollSchedId
			AND wk.WeekNumber = 4;

		--Update Week 5 
		UPDATE s
		SET s.Week5LDA = wk.LDA
		FROM stage.Students s
		INNER JOIN @WeeklyLDAs wk ON s.SyStudentId = wk.SyStudentId
			AND s.AdEnrollSchedId = wk.AdEnrollSchedId
			AND wk.WeekNumber = 5;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Weekly LDAs', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Grades table with the number of attempts 
		--**************************************************************************************************************************************** 
		WITH cteCounts (GradePrimaryKey, AttemptCount)
		AS (
			SELECT a.GRADEBOOK_GRADE_PK1 'GradePrimaryKey', COUNT(a.PK1) 'AttemptCount'
			FROM ATTEMPT a
			GROUP BY a.GRADEBOOK_GRADE_PK1
			)
		UPDATE g
		SET g.NumberOfAttempts = c.AttemptCount
		FROM stage.Grades g
		INNER JOIN cteCounts c ON g.GradePrimaryKey = c.GradePrimaryKey;

		EXEC LS_ODS.AddODSLoadLog 'Updated Grade Counts Of Attempts', 0;

		--**************************************************************************************************************************************** 
		--Add new records to the TimeInModule table 
		--**************************************************************************************************************************************** 
		INSERT INTO LS_ODS.TimeInModule (ScormRegistrationId, LaunchHistoryId, BlackboardUsername, UserPrimaryKey, SyStudentId, CourseUsersPrimaryKey, CoursePrimaryKey, AssignmentPrimaryKey, StartDateTime, EndDateTime, ElapsedTimeMinutes, CompletionStatus, SatisfactionStatus, ScormRegistrationLaunchHistoryStartDateTimeKey)
		SELECT sr.SCORM_REGISTRATION_ID 'ScormRegistrationId', slh.LAUNCH_HISTORY_ID 'LaunchHistoryId', sr.GLOBAL_OBJECTIVE_SCOPE 'BlackboardUsername', u.PK1 'UserPrimaryKey', REPLACE(u.BATCH_UID, 'SyStudent_', '') 'SyStudentId', cu.PK1 'CourseUsersPrimaryKey', cm.PK1 'CoursePrimaryKey', cc.PK1 'AssignmentPrimaryKey', slh.LAUNCH_TIME 'StartDateTime', slh.EXIT_TIME 'EndDateTime', DATEDIFF(MINUTE, slh.LAUNCH_TIME, slh.EXIT_TIME) 'ElapsedTimeMinutes', slh.COMPLETION 'CompletionStatus', slh.SATISFACTION 'StatisfactionStatus', sr.SCORM_REGISTRATION_ID + '_' + slh.LAUNCH_HISTORY_ID + '_' + CONVERT(VARCHAR(50), slh.LAUNCH_TIME, 126) 'ScormRegistrationLaunchHistoryStartDateTimeKey'
		FROM dbo.SCORMLAUNCHHISTORY slh
		INNER JOIN dbo.SCORMREGISTRATION sr ON slh.SCORM_REGISTRATION_ID = sr.SCORM_REGISTRATION_ID
		INNER JOIN dbo.USERS u ON sr.GLOBAL_OBJECTIVE_SCOPE = u.[USER_ID]
			AND LEFT(u.BATCH_UID, 10) = 'SyStudent_'
		INNER JOIN dbo.COURSE_CONTENTS cc ON REPLACE(REPLACE(sr.CONTENT_ID, '_1', ''), '_', '') = cc.PK1
		INNER JOIN dbo.COURSE_MAIN cm ON cc.CRSMAIN_PK1 = cm.PK1
		INNER JOIN dbo.COURSE_USERS cu ON u.PK1 = cu.USERS_PK1
			AND cm.PK1 = cu.CRSMAIN_PK1
		LEFT JOIN dbo.DATA_SOURCE ds --Adding to deal with erroneous DSKs added
			ON ds.PK1 = cu.DATA_SRC_PK1 --in the SIS Framework cleanup effort
		WHERE sr.SCORM_REGISTRATION_ID + '_' + slh.LAUNCH_HISTORY_ID + '_' + CONVERT(VARCHAR(50), slh.LAUNCH_TIME, 126) NOT IN (
				SELECT tim.ScormRegistrationLaunchHistoryStartDateTimeKey
				FROM LS_ODS.TimeInModule tim
				)
			AND ds.batch_uid NOT IN (
				'ENR_181008_02.txt'
				,'ENR_181008'
				,'ENR_181008_1558036.txt'
				);--Adding to deal with erroneous DSKs added
			--in the SIS Framework cleanup effort 

		EXEC LS_ODS.AddODSLoadLog 'Updated Time In Module Table', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the Current Course Grade 
		--**************************************************************************************************************************************** 
		DECLARE @TodayDayNumber INT;

		SET @TodayDayNumber = DATEPART(WEEKDAY, GETDATE());

		--SELECT @TodayDayNumber; 
		WITH cteCurrentCourseGrade (SyStudentId, AdClassSchedId, CurrentCourseGrade)
AS (
    SELECT 
        s.SyStudentId, 
        s.AdClassSchedId, 
        CASE 
            WHEN s.Week1Grade IS NULL
                AND s.Week2Grade IS NULL
                AND s.Week3Grade IS NULL
                AND s.Week4Grade IS NULL
                AND s.Week5Grade IS NULL
                THEN NULL
            WHEN c.WeekNumber = 1 THEN 1.0
            WHEN c.WeekNumber = 2 THEN 
                CASE 
                    WHEN @TodayDayNumber < 5 THEN 1.0
                    ELSE s.Week1Grade
                END
            WHEN c.WeekNumber = 3 THEN 
                CASE 
                    WHEN @TodayDayNumber < 5 THEN s.Week1Grade
                    ELSE s.Week2Grade
                END
            WHEN c.WeekNumber = 4 THEN 
                CASE 
                    WHEN @TodayDayNumber < 5 THEN s.Week2Grade
                    ELSE s.Week3Grade
                END
            WHEN c.WeekNumber = 5 THEN 
                CASE 
                    WHEN @TodayDayNumber < 5 THEN s.Week3Grade
                    ELSE s.Week4Grade
                END
            WHEN c.WeekNumber = 6 THEN 
                CASE 
                    WHEN @TodayDayNumber < 5 THEN s.Week4Grade
                    ELSE s.Week5Grade
                END
            ELSE s.Week5Grade
        END AS CurrentCourseGrade
    FROM 
        stage.Students s
    INNER JOIN 
        stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
)

 -- Helps to recompile the query plan based on current parameters

		UPDATE s
		SET s.CurrentCourseGrade = ccg.CurrentCourseGrade
		FROM stage.Students s
		INNER JOIN cteCurrentCourseGrade ccg ON s.SyStudentId = ccg.SyStudentId
			AND s.AdClassSchedId = ccg.AdClassSchedId;

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
		DECLARE @ChangedStudents TABLE (StudentPrimaryKey INT, CourseUsersPrimaryKey INT);

		--Find Changed Students And Populated Table Variable 
		INSERT INTO @ChangedStudents (StudentPrimaryKey, CourseUsersPrimaryKey)
		SELECT new.StudentPrimaryKey, new.CourseUsersPrimaryKey
		FROM stage.Students new
		INNER JOIN LS_ODS.Students old ON new.StudentPrimaryKey = old.StudentPrimaryKey
			AND new.CourseUsersPrimaryKey = old.CourseUsersPrimaryKey
			AND old.ActiveFlag = 1
		WHERE new.DateTimeCreated <> old.DateTimeCreated
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
			OR new.CourseUsersPrimaryKey <> old.CourseUsersPrimaryKey
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
			OR (
				new.DateTimeCreated IS NOT NULL
				AND old.DateTimeCreated IS NULL
				)
			OR (
				new.DateTimeModified IS NOT NULL
				AND old.DateTimeModified IS NULL
				)
			OR (
				new.RowStatus IS NOT NULL
				AND old.RowStatus IS NULL
				)
			OR (
				new.BatchUniqueIdentifier IS NOT NULL
				AND old.BatchUniqueIdentifier IS NULL
				)
			OR (
				new.BlackboardUsername IS NOT NULL
				AND old.BlackboardUsername IS NULL
				)
			OR (
				new.SyStudentId IS NOT NULL
				AND old.SyStudentId IS NULL
				)
			OR (
				new.FirstName IS NOT NULL
				AND old.FirstName IS NULL
				)
			OR (
				new.LastName IS NOT NULL
				AND old.LastName IS NULL
				)
			OR (
				new.Campus IS NOT NULL
				AND old.Campus IS NULL
				)
			OR (
				new.AdEnrollSchedId IS NOT NULL
				AND old.AdEnrollSchedId IS NULL
				)
			OR (
				new.AdClassSchedId IS NOT NULL
				AND old.AdClassSchedId IS NULL
				)
			OR (
				new.LastLoginDateTime IS NOT NULL
				AND old.LastLoginDateTime IS NULL
				)
			OR (
				new.CourseUsersPrimaryKey IS NOT NULL
				AND old.CourseUsersPrimaryKey IS NULL
				)
			OR (
				new.TimeInClass IS NOT NULL
				AND old.TimeInClass IS NULL
				)
			OR (
				new.LastI3InteractionNumberMainPhone IS NOT NULL
				AND old.LastI3InteractionNumberMainPhone IS NULL
				)
			OR (
				new.LastI3InteractionDateTimeMainPhone IS NOT NULL
				AND old.LastI3InteractionDateTimeMainPhone IS NULL
				)
			OR (
				new.DaysSinceLastI3InteractionMainPhone IS NOT NULL
				AND old.DaysSinceLastI3InteractionMainPhone IS NULL
				)
			OR (
				new.LastI3InteractionNumberWorkPhone IS NOT NULL
				AND old.LastI3InteractionNumberWorkPhone IS NULL
				)
			OR (
				new.LastI3InteractionDateTimeWorkPhone IS NOT NULL
				AND old.LastI3InteractionDateTimeWorkPhone IS NULL
				)
			OR (
				new.DaysSinceLastI3InteractionWorkPhone IS NOT NULL
				AND old.DaysSinceLastI3InteractionWorkPhone IS NULL
				)
			OR (
				new.LastI3InteractionNumberMobilePhone IS NOT NULL
				AND old.LastI3InteractionNumberMobilePhone IS NULL
				)
			OR (
				new.LastI3InteractionDateTimeMobilePhone IS NOT NULL
				AND old.LastI3InteractionDateTimeMobilePhone IS NULL
				)
			OR (
				new.DaysSinceLastI3InteractionMobilePhone IS NOT NULL
				AND old.DaysSinceLastI3InteractionMobilePhone IS NULL
				)
			OR (
				new.LastI3InteractionNumberOtherPhone IS NOT NULL
				AND old.LastI3InteractionNumberOtherPhone IS NULL
				)
			OR (
				new.LastI3InteractionDateTimeOtherPhone IS NOT NULL
				AND old.LastI3InteractionDateTimeOtherPhone IS NULL
				)
			OR (
				new.DaysSinceLastI3InteractionOtherPhone IS NOT NULL
				AND old.DaysSinceLastI3InteractionOtherPhone IS NULL
				)
			OR (
				new.Week1Grade IS NOT NULL
				AND old.Week1Grade IS NULL
				)
			OR (
				new.Week2Grade IS NOT NULL
				AND old.Week2Grade IS NULL
				)
			OR (
				new.Week3Grade IS NOT NULL
				AND old.Week3Grade IS NULL
				)
			OR (
				new.Week4Grade IS NOT NULL
				AND old.Week4Grade IS NULL
				)
			OR (
				new.Week5Grade IS NOT NULL
				AND old.Week5Grade IS NULL
				)
			OR (
				new.SelfTestsCount IS NOT NULL
				AND old.SelfTestsCount IS NULL
				)
			OR (
				new.AssessmentsCount IS NOT NULL
				AND old.AssessmentsCount IS NULL
				)
			OR (
				new.AssignmentsCount IS NOT NULL
				AND old.AssignmentsCount IS NULL
				)
			OR (
				new.DiscussionsCount IS NOT NULL
				AND old.DiscussionsCount IS NULL
				)
			OR (
				new.Week1CompletionRate IS NOT NULL
				AND old.Week1CompletionRate IS NULL
				)
			OR (
				new.Week2CompletionRate IS NOT NULL
				AND old.Week2CompletionRate IS NULL
				)
			OR (
				new.Week3CompletionRate IS NOT NULL
				AND old.Week3CompletionRate IS NULL
				)
			OR (
				new.Week4CompletionRate IS NOT NULL
				AND old.Week4CompletionRate IS NULL
				)
			OR (
				new.Week5CompletionRate IS NOT NULL
				AND old.Week5CompletionRate IS NULL
				)
			OR (
				new.VAStudent IS NOT NULL
				AND old.VAStudent IS NULL
				)
			OR (
				new.NoticeName IS NOT NULL
				AND old.NoticeName IS NULL
				)
			OR (
				new.NoticeDueDate IS NOT NULL
				AND old.NoticeDueDate IS NULL
				)
			OR (
				new.VABenefitName IS NOT NULL
				AND old.VABenefitName IS NULL
				)
			OR (
				new.ClassStatus IS NOT NULL
				AND old.ClassStatus IS NULL
				)
			OR (
				new.Week1LDA IS NOT NULL
				AND old.Week1LDA IS NULL
				)
			OR (
				new.Week2LDA IS NOT NULL
				AND old.Week2LDA IS NULL
				)
			OR (
				new.Week3LDA IS NOT NULL
				AND old.Week3LDA IS NULL
				)
			OR (
				new.Week4LDA IS NOT NULL
				AND old.Week4LDA IS NULL
				)
			OR (
				new.Week5LDA IS NOT NULL
				AND old.Week5LDA IS NULL
				)
			OR (
				new.Week1CompletedAssignments IS NOT NULL
				AND old.Week1CompletedAssignments IS NULL
				)
			OR (
				new.Week2CompletedAssignments IS NOT NULL
				AND old.Week2CompletedAssignments IS NULL
				)
			OR (
				new.Week3CompletedAssignments IS NOT NULL
				AND old.Week3CompletedAssignments IS NULL
				)
			OR (
				new.Week4CompletedAssignments IS NOT NULL
				AND old.Week4CompletedAssignments IS NULL
				)
			OR (
				new.Week5CompletedAssignments IS NOT NULL
				AND old.Week5CompletedAssignments IS NULL
				)
			OR (
				new.CoursePercentage IS NOT NULL
				AND old.CoursePercentage IS NULL
				)
			OR (
				new.TotalWorkPercentage IS NOT NULL
				AND old.TotalWorkPercentage IS NULL
				)
			OR (
				new.AdEnrollId IS NOT NULL
				AND old.AdEnrollId IS NULL
				)
			OR (
				new.IsRetake IS NOT NULL
				AND old.IsRetake IS NULL
				)
			OR (
				new.StudentCourseUserKeys IS NOT NULL
				AND old.StudentCourseUserKeys IS NULL
				)
			OR (
				new.CurrentCourseGrade IS NOT NULL
				AND old.CurrentCourseGrade IS NULL
				)
			OR (
				new.ProgramCode IS NOT NULL
				AND old.ProgramCode IS NULL
				)
			OR (
				new.ProgramName IS NOT NULL
				AND old.ProgramName IS NULL
				)
			OR (
				new.ProgramVersionCode IS NOT NULL
				AND old.ProgramVersionCode IS NULL
				)
			OR (
				new.ProgramVersionName IS NOT NULL
				AND old.ProgramVersionName IS NULL
				)
			OR (
				new.MondayTimeInClass IS NOT NULL
				AND old.MondayTimeInClass IS NULL
				)
			OR (
				new.TuesdayTimeInClass IS NOT NULL
				AND old.TuesdayTimeInClass IS NULL
				)
			OR (
				new.WednesdayTimeInClass IS NOT NULL
				AND old.WednesdayTimeInClass IS NULL
				)
			OR (
				new.ThursdayTimeInClass IS NOT NULL
				AND old.ThursdayTimeInClass IS NULL
				)
			OR (
				new.FridayTimeInClass IS NOT NULL
				AND old.FridayTimeInClass IS NULL
				)
			OR (
				new.SaturdayTimeInClass IS NOT NULL
				AND old.SaturdayTimeInClass IS NULL
				)
			OR (
				new.SundayTimeInClass IS NOT NULL
				AND old.SundayTimeInClass IS NULL
				)
			OR (
				new.Week1CompletionRateFixed IS NOT NULL
				AND old.Week1CompletionRateFixed IS NULL
				)
			OR (
				new.Week2CompletionRateFixed IS NOT NULL
				AND old.Week2CompletionRateFixed IS NULL
				)
			OR (
				new.Week3CompletionRateFixed IS NOT NULL
				AND old.Week3CompletionRateFixed IS NULL
				)
			OR (
				new.Week4CompletionRateFixed IS NOT NULL
				AND old.Week4CompletionRateFixed IS NULL
				)
			OR (
				new.Week5CompletionRateFixed IS NOT NULL
				AND old.Week5CompletionRateFixed IS NULL
				)
			OR (
				new.StudentNumber IS NOT NULL
				AND old.StudentNumber IS NULL
				)
			OR (
				new.SourceSystem IS NOT NULL
				AND old.SourceSystem IS NULL
				);

		--Update LS_ODS Students Table To Inactivate Changed Student Records 
		UPDATE old
		SET old.ActiveFlag = 0
		FROM LS_ODS.Students old
		INNER JOIN @ChangedStudents new ON old.StudentPrimaryKey = new.StudentPrimaryKey
			AND old.CourseUsersPrimaryKey = new.CourseUsersPrimaryKey;

		--Add Changed Student Records To LS_ODS Students Table 
		INSERT INTO LS_ODS.Students (
			StudentPrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, BlackboardUsername, SyStudentId, FirstName, LastName, Campus, AdEnrollSchedId, AdClassSchedId, CourseUsersPrimaryKey, LastLoginDateTime, TimeInClass, LastI3InteractionNumberMainPhone, LastI3InteractionDateTimeMainPhone, DaysSinceLastI3InteractionMainPhone, LastI3InteractionNumberWorkPhone, LastI3InteractionDateTimeWorkPhone, DaysSinceLastI3InteractionWorkPhone, LastI3InteractionNumberMobilePhone, LastI3InteractionDateTimeMobilePhone, DaysSinceLastI3InteractionMobilePhone, LastI3InteractionNumberOtherPhone, LastI3InteractionDateTimeOtherPhone, DaysSinceLastI3InteractionOtherPhone, Week1Grade, Week2Grade, Week3Grade, Week4Grade, Week5Grade, SelfTestsCount, AssessmentsCount, AssignmentsCount, DiscussionsCount, Week1CompletionRate, Week2CompletionRate, Week3CompletionRate, Week4CompletionRate, Week5CompletionRate, VAStudent, NoticeName, NoticeDueDate, VABenefitName, ClassStatus, Week1LDA, Week2LDA, Week3LDA, Week4LDA, Week5LDA, 
			Week1CompletedAssignments, Week2CompletedAssignments, Week3CompletedAssignments, Week4CompletedAssignments, Week5CompletedAssignments, CoursePercentage, TotalWorkPercentage, AdEnrollId, IsRetake, StudentCourseUserKeys, CurrentCourseGrade, ProgramCode, ProgramName, ProgramVersionCode, ProgramVersionName, MondayTimeInClass, TuesdayTimeInClass, WednesdayTimeInClass, ThursdayTimeInClass, FridayTimeInClass, SaturdayTimeInClass, SundayTimeInClass, Week1CompletionRateFixed, Week2CompletionRateFixed, Week3CompletionRateFixed, Week4CompletionRateFixed, Week5CompletionRateFixed, StudentNumber, SourceSystem
			)
		SELECT new.StudentPrimaryKey, new.DateTimeCreated, new.DateTimeModified, new.RowStatus, new.BatchUniqueIdentifier, new.BlackboardUsername, new.SyStudentId, new.FirstName, new.LastName, new.Campus, new.AdEnrollSchedId, new.AdClassSchedId, new.CourseUsersPrimaryKey, new.LastLoginDateTime, new.TimeInClass, new.LastI3InteractionNumberMainPhone, new.LastI3InteractionDateTimeMainPhone, new.DaysSinceLastI3InteractionMainPhone, new.LastI3InteractionNumberWorkPhone, new.LastI3InteractionDateTimeWorkPhone, new.DaysSinceLastI3InteractionWorkPhone, new.LastI3InteractionNumberMobilePhone, new.LastI3InteractionDateTimeMobilePhone, new.DaysSinceLastI3InteractionMobilePhone, new.LastI3InteractionNumberOtherPhone, new.LastI3InteractionDateTimeOtherPhone, new.DaysSinceLastI3InteractionOtherPhone, new.Week1Grade, new.Week2Grade, new.Week3Grade, new.Week4Grade, new.Week5Grade, new.SelfTestsCount, new.AssessmentsCount, new.AssignmentsCount, new.DiscussionsCount, new.Week1CompletionRate, new.Week2CompletionRate, new.Week3CompletionRate, 
			new.Week4CompletionRate, new.Week5CompletionRate, new.VAStudent, new.NoticeName, new.NoticeDueDate, new.VABenefitName, new.ClassStatus, new.Week1LDA, new.Week2LDA, new.Week3LDA, new.Week4LDA, new.Week5LDA, new.Week1CompletedAssignments, new.Week2CompletedAssignments, new.Week3CompletedAssignments, new.Week4CompletedAssignments, new.Week5CompletedAssignments, new.CoursePercentage, new.TotalWorkPercentage, new.AdEnrollId, new.IsRetake, new.StudentCourseUserKeys, new.CurrentCourseGrade, new.ProgramCode, new.ProgramName, new.ProgramVersionCode, new.ProgramVersionName, new.MondayTimeInClass, new.TuesdayTimeInClass, new.WednesdayTimeInClass, new.ThursdayTimeInClass, new.FridayTimeInClass, new.SaturdayTimeInClass, new.SundayTimeInClass, new.Week1CompletionRateFixed, new.Week2CompletionRateFixed, new.Week3CompletionRateFixed, new.Week4CompletionRateFixed, new.Week5CompletionRateFixed, new.StudentNumber, new.SourceSystem
		FROM stage.Students new
		INNER JOIN @ChangedStudents changed ON new.StudentPrimaryKey = changed.StudentPrimaryKey
			AND new.CourseUsersPrimaryKey = changed.CourseUsersPrimaryKey
		WHERE new.AdEnrollSchedId IS NOT NULL;

		EXEC LS_ODS.AddODSLoadLog 'Updated Students Records That Have Changed', 0;

		--**************************************************************************************************************************************** 
		--Update Course records that have changed 
		--**************************************************************************************************************************************** 
		--Create Table Variable To Hold Changed Course Records 
		DECLARE @ChangedCourses TABLE (CoursePrimaryKey INT);

		--Find Changed Courses And Populated Table Variable 
		INSERT INTO @ChangedCourses (CoursePrimaryKey)
		SELECT new.CoursePrimaryKey
		FROM stage.Courses new
		INNER JOIN LS_ODS.Courses old ON new.CoursePrimaryKey = old.CoursePrimaryKey
			AND old.ActiveFlag = 1
		WHERE new.DateTimeCreated <> old.DateTimeCreated
			OR new.DateTimeModified <> old.DateTimeModified
			OR new.RowStatus <> old.RowStatus
			OR new.BatchUniqueIdentifier <> old.BatchUniqueIdentifier
			OR new.CourseCode <> old.CourseCode
			OR new.CourseName <> old.CourseName
			OR new.SectionNumber <> old.SectionNumber
			OR new.SectionStart <> old.SectionStart
			OR new.SectionEnd <> old.SectionEnd
			OR new.AdClassSchedId <> old.AdClassSchedId
			OR new.WeekNumber <> old.WeekNumber
			OR new.Week1AssignmentCount <> old.Week1AssignmentCount
			OR new.Week2AssignmentCount <> old.Week2AssignmentCount
			OR new.Week3AssignmentCount <> old.Week3AssignmentCount
			OR new.Week4AssignmentCount <> old.Week4AssignmentCount
			OR new.Week5AssignmentCount <> old.Week5AssignmentCount
			OR new.PrimaryInstructor <> old.PrimaryInstructor
			OR new.SecondaryInstructor <> old.SecondaryInstructor
			OR new.Week1StartDate <> old.Week1StartDate
			OR new.Week2StartDate <> old.Week2StartDate
			OR new.Week3StartDate <> old.Week3StartDate
			OR new.Week4StartDate <> old.Week4StartDate
			OR new.Week5StartDate <> old.Week5StartDate
			OR new.IsOrganization <> old.IsOrganization
			OR new.ExtensionWeekStartDate <> old.ExtensionWeekStartDate
			OR new.AcademicFacilitator <> old.AcademicFacilitator
			OR new.PrimaryInstructorId <> old.PrimaryInstructorId
			OR new.SecondaryInstructorId <> old.SecondaryInstructorId
			OR new.AcademicFacilitatorId <> old.AcademicFacilitatorId
			OR new.DayNumber <> old.DayNumber
			OR new.CengageCourseIndicator <> old.CengageCourseIndicator
			OR (
				new.DateTimeCreated IS NOT NULL
				AND old.DateTimeCreated IS NULL
				)
			OR (
				new.DateTimeModified IS NOT NULL
				AND old.DateTimeModified IS NULL
				)
			OR (
				new.RowStatus IS NOT NULL
				AND old.RowStatus IS NULL
				)
			OR (
				new.BatchUniqueIdentifier IS NOT NULL
				AND old.BatchUniqueIdentifier IS NULL
				)
			OR (
				new.CourseCode IS NOT NULL
				AND old.CourseCode IS NULL
				)
			OR (
				new.CourseName IS NOT NULL
				AND old.CourseName IS NULL
				)
			OR (
				new.SectionNumber IS NOT NULL
				AND old.SectionNumber IS NULL
				)
			OR (
				new.SectionStart IS NOT NULL
				AND old.SectionStart IS NULL
				)
			OR (
				new.SectionEnd IS NOT NULL
				AND old.SectionEnd IS NULL
				)
			OR (
				new.AdClassSchedId IS NOT NULL
				AND old.AdClassSchedId IS NULL
				)
			OR (
				new.WeekNumber IS NOT NULL
				AND old.WeekNumber IS NULL
				)
			OR (
				new.Week1AssignmentCount IS NOT NULL
				AND old.Week1AssignmentCount IS NULL
				)
			OR (
				new.Week2AssignmentCount IS NOT NULL
				AND old.Week2AssignmentCount IS NULL
				)
			OR (
				new.Week3AssignmentCount IS NOT NULL
				AND old.Week3AssignmentCount IS NULL
				)
			OR (
				new.Week4AssignmentCount IS NOT NULL
				AND old.Week4AssignmentCount IS NULL
				)
			OR (
				new.Week5AssignmentCount IS NOT NULL
				AND old.Week5AssignmentCount IS NULL
				)
			OR (
				new.PrimaryInstructor IS NOT NULL
				AND old.PrimaryInstructor IS NULL
				)
			OR (
				new.SecondaryInstructor IS NOT NULL
				AND old.SecondaryInstructor IS NULL
				)
			OR (
				new.Week1StartDate IS NOT NULL
				AND old.Week1StartDate IS NULL
				)
			OR (
				new.Week2StartDate IS NOT NULL
				AND old.Week2StartDate IS NULL
				)
			OR (
				new.Week3StartDate IS NOT NULL
				AND old.Week3StartDate IS NULL
				)
			OR (
				new.Week4StartDate IS NOT NULL
				AND old.Week4StartDate IS NULL
				)
			OR (
				new.Week5StartDate IS NOT NULL
				AND old.Week5StartDate IS NULL
				)
			OR (
				new.ExtensionWeekStartDate IS NOT NULL
				AND old.ExtensionWeekStartDate IS NULL
				)
			OR (
				new.IsOrganization IS NOT NULL
				AND old.IsOrganization IS NULL
				)
			OR (
				new.AcademicFacilitator IS NOT NULL
				AND old.AcademicFacilitator IS NULL
				)
			OR (
				new.PrimaryInstructorId IS NOT NULL
				AND old.PrimaryInstructorId IS NULL
				)
			OR (
				new.SecondaryInstructorId IS NOT NULL
				AND old.SecondaryInstructorId IS NULL
				)
			OR (
				new.AcademicFacilitatorId IS NOT NULL
				AND old.AcademicFacilitatorId IS NULL
				)
			OR (
				new.DayNumber IS NOT NULL
				AND old.DayNumber IS NULL
				)
			OR (
				new.CengageCourseIndicator IS NOT NULL
				AND old.CengageCourseIndicator IS NULL
				)
			OR (
				new.SourceSystem IS NOT NULL
				AND old.SourceSystem IS NULL
				);

		--Update LS_ODS Course Table To Inactivate Changed Course Records 
		UPDATE old
		SET old.ActiveFlag = 0
		FROM LS_ODS.Courses old
		INNER JOIN @ChangedCourses new ON old.CoursePrimaryKey = new.CoursePrimaryKey;

		--Add Changed Course Records To LS_ODS Course Table 
		INSERT INTO LS_ODS.Courses (CoursePrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, CourseCode, CourseName, SectionNumber, SectionStart, SectionEnd, AdClassSchedId, WeekNumber, Week1AssignmentCount, Week2AssignmentCount, Week3AssignmentCount, Week4AssignmentCount, Week5AssignmentCount, PrimaryInstructor, SecondaryInstructor, Week1StartDate, Week2StartDate, Week3StartDate, Week4StartDate, Week5StartDate, ExtensionWeekStartDate, IsOrganization, AcademicFacilitator, PrimaryInstructorId, SecondaryInstructorId, AcademicFacilitatorId, DayNumber, CengageCourseIndicator, SourceSystem)
		SELECT new.CoursePrimaryKey, new.DateTimeCreated, new.DateTimeModified, new.RowStatus, new.BatchUniqueIdentifier, new.CourseCode, new.CourseName, new.SectionNumber, new.SectionStart, new.SectionEnd, new.AdClassSchedId, new.WeekNumber, new.Week1AssignmentCount, new.Week2AssignmentCount, new.Week3AssignmentCount, new.Week4AssignmentCount, new.Week5AssignmentCount, new.PrimaryInstructor, new.SecondaryInstructor, new.Week1StartDate, new.Week2StartDate, new.Week3StartDate, new.Week4StartDate, new.Week5StartDate, new.ExtensionWeekStartDate, new.IsOrganization, new.AcademicFacilitator, new.PrimaryInstructorId, new.SecondaryInstructorId, new.AcademicFacilitatorId, new.DayNumber, new.CengageCourseIndicator, new.SourceSystem
		FROM stage.Courses new
		INNER JOIN @ChangedCourses changed ON new.CoursePrimaryKey = changed.CoursePrimaryKey;

		EXEC LS_ODS.AddODSLoadLog 'Updated Course Records That Have Changed', 0;

		--**************************************************************************************************************************************** 
		--Update Assignment records that have changed 
		--**************************************************************************************************************************************** 
		--Create Table Variable To Hold Changed Assignment Records 
		DECLARE @ChangedAssignments TABLE (AssignmentPrimaryKey INT);

		--Find Changed Assignments And Populated Table Variable 
		INSERT INTO @ChangedAssignments (AssignmentPrimaryKey)
		SELECT new.AssignmentPrimaryKey
		FROM stage.Assignments new
		INNER JOIN LS_ODS.Assignments old ON new.AssignmentPrimaryKey = old.AssignmentPrimaryKey
			AND old.ActiveFlag = 1
		WHERE new.CoursePrimaryKey <> old.CoursePrimaryKey
			OR new.WeekNumber <> old.WeekNumber
			OR new.AssignmentTitle <> old.AssignmentTitle
			OR new.DueDate <> old.DueDate
			OR new.PossiblePoints <> old.PossiblePoints
			OR new.DateTimeCreated <> old.DateTimeCreated
			OR new.DateTimeModified <> old.DateTimeModified
			OR new.ScoreProviderHandle <> old.ScoreProviderHandle
			OR new.CourseContentsPrimaryKey1 <> old.CourseContentsPrimaryKey1
			OR new.AlternateTitle <> old.AlternateTitle
			OR new.IsReportable <> old.IsReportable
			OR new.CountsAsSubmission <> old.CountsAsSubmission
			OR new.AssignmentType <> old.AssignmentType
			OR (
				new.CoursePrimaryKey IS NOT NULL
				AND old.CoursePrimaryKey IS NULL
				)
			OR (
				new.WeekNumber IS NOT NULL
				AND old.WeekNumber IS NULL
				)
			OR (
				new.AssignmentTitle IS NOT NULL
				AND old.AssignmentTitle IS NULL
				)
			OR (
				new.DueDate IS NOT NULL
				AND old.DueDate IS NULL
				)
			OR (
				new.PossiblePoints IS NOT NULL
				AND old.PossiblePoints IS NULL
				)
			OR (
				new.DateTimeCreated IS NOT NULL
				AND old.DateTimeCreated IS NULL
				)
			OR (
				new.DateTimeModified IS NOT NULL
				AND old.DateTimeModified IS NULL
				)
			OR (
				new.ScoreProviderHandle IS NOT NULL
				AND old.ScoreProviderHandle IS NULL
				)
			OR (
				new.CourseContentsPrimaryKey1 IS NOT NULL
				AND old.CourseContentsPrimaryKey1 IS NULL
				)
			OR (
				new.AlternateTitle IS NOT NULL
				AND old.AlternateTitle IS NULL
				)
			OR (
				new.IsReportable IS NOT NULL
				AND old.IsReportable IS NULL
				)
			OR (
				new.CountsAsSubmission IS NOT NULL
				AND old.CountsAsSubmission IS NULL
				)
			OR (
				new.AssignmentType IS NOT NULL
				AND old.AssignmentType IS NULL
				)
			OR (
				new.SourceSystem IS NOT NULL
				AND old.SourceSystem IS NULL
				);

		--Update LS_ODS Assignments Table To Inactivate Changed Assignments Records 
		UPDATE old
		SET old.ActiveFlag = 0
		FROM LS_ODS.Assignments old
		INNER JOIN @ChangedAssignments new ON old.AssignmentPrimaryKey = new.AssignmentPrimaryKey;

		DECLARE @CountStageD2LAssignments AS INT

		SELECT @CountStageD2LAssignments = COUNT(*)
		FROM [stage].[Assignments]
		WHERE SourceSystem = 'D2L'

		IF @CountStageD2LAssignments > 0
		BEGIN
			-- Update LS_ODS Assignments Table To Inactivate Duplicated D2L Assignments
			UPDATE Assignments
			SET Assignments.[ActiveFlag] = 0
			FROM [LS_ODS].[Assignments] Assignments
			WHERE Assignments.[AssignmentPrimaryKey] IN (
					SELECT asg.[AssignmentPrimaryKey]
					FROM (
						SELECT [asg].[CoursePrimaryKey], [asg].[WeekNumber], [asg].[AssignmentTitle], COUNT(*) AS Total
						FROM [LS_ODS].[Assignments] asg
						GROUP BY [asg].[CoursePrimaryKey], [asg].[WeekNumber], [asg].[AssignmentTitle]
						HAVING COUNT(*) > 1
						) da
					INNER JOIN [LS_ODS].[Assignments] asg ON [da].[CoursePrimaryKey] = [asg].[CoursePrimaryKey]
						AND [da].[WeekNumber] = [asg].[WeekNumber]
						AND [da].[AssignmentTitle] = [asg].[AssignmentTitle]
					INNER JOIN [dbo].[COURSE_MAIN] cm ON [cm].PK1 = [asg].[CoursePrimaryKey]
						AND [cm].[SourceSystem] = 'D2L'
					LEFT JOIN [stage].[Assignments] sasg ON sasg.[AssignmentPrimaryKey] = [asg].[AssignmentPrimaryKey]
					WHERE [sasg].[AssignmentPrimaryKey] IS NULL
					)

			-- UPDATE LS_ODS Assignments Table To Inactivate deleted Assignments
			UPDATE Assignments
			SET Assignments.[ActiveFlag] = 0
			FROM [LS_ODS].[Assignments] Assignments
			LEFT JOIN [stage].[Assignments] sasg ON sasg.[AssignmentPrimaryKey] = Assignments.[AssignmentPrimaryKey]
			WHERE Assignments.[SourceSystem] = 'D2L'
				AND sasg.[AssignmentPrimaryKey] IS NULL
		END

		--Add Changed Assignment Records To LS_ODS Assignments Table 
		INSERT INTO LS_ODS.Assignments (AssignmentPrimaryKey, CoursePrimaryKey, WeekNumber, AssignmentTitle, DueDate, PossiblePoints, DateTimeCreated, DateTimeModified, ScoreProviderHandle, CourseContentsPrimaryKey1, AlternateTitle, IsReportable, CountsAsSubmission, AssignmentType, SourceSystem)
		SELECT new.AssignmentPrimaryKey, new.CoursePrimaryKey, new.WeekNumber, new.AssignmentTitle, new.DueDate, new.PossiblePoints, new.DateTimeCreated, new.DateTimeModified, new.ScoreProviderHandle, new.CourseContentsPrimaryKey1, new.AlternateTitle, new.IsReportable, new.CountsAsSubmission, new.AssignmentType, new.SourceSystem
		FROM stage.Assignments new
		INNER JOIN @ChangedAssignments changed ON new.AssignmentPrimaryKey = changed.AssignmentPrimaryKey;

		EXEC LS_ODS.AddODSLoadLog 'Updated Assignment Records That Have Changed', 0;

		--**************************************************************************************************************************************** 
		--Update Grade records that have changed 
		--**************************************************************************************************************************************** 
		--Create Table Variable To Hold Changed Grades Records 
		DECLARE @ChangedGrades TABLE (GradePrimaryKey INT);

		--Find Changed Grades And Populated Table Variable 
		INSERT INTO @ChangedGrades (GradePrimaryKey)
		SELECT new.GradePrimaryKey
		FROM stage.Grades new
		INNER JOIN LS_ODS.Grades old ON new.GradePrimaryKey = old.GradePrimaryKey
			AND old.ActiveFlag = 1
		WHERE new.CourseUsersPrimaryKey <> old.CourseUsersPrimaryKey
			OR new.RowStatus <> old.RowStatus
			OR new.HighestScore <> old.HighestScore
			OR new.HighestGrade <> old.HighestGrade
			OR new.HighestAttemptDateTime <> old.HighestAttemptDateTime
			OR new.ManualScore <> old.ManualScore
			OR new.ManualGrade <> old.ManualGrade
			OR new.ManualDateTime <> old.ManualDateTime
			OR new.ExemptIndicator <> old.ExemptIndicator
			OR new.HighestDateTimeCreated <> old.HighestDateTimeCreated
			OR new.HighestDateTimeModified <> old.HighestDateTimeModified
			OR new.HighestIsLatestAttemptIndicator <> old.HighestIsLatestAttemptIndicator
			OR new.NumberOfAttempts <> old.NumberOfAttempts
			OR new.FirstScore <> old.FirstScore
			OR new.FirstGrade <> old.FirstGrade
			OR new.FirstAttemptDateTime <> old.FirstAttemptDateTime
			OR new.FirstIsLatestAttemptIndicator <> old.FirstIsLatestAttemptIndicator
			OR new.FirstDateTimeCreated <> old.FirstDateTimeCreated
			OR new.FirstDateTimeModified <> old.FirstDateTimeModified
			OR new.AssignmentPrimaryKey <> old.AssignmentPrimaryKey
			OR new.AssignmentStatus <> old.AssignmentStatus
			OR (
				new.CourseUsersPrimaryKey IS NOT NULL
				AND old.CourseUsersPrimaryKey IS NULL
				)
			OR (
				new.RowStatus IS NOT NULL
				AND old.RowStatus IS NULL
				)
			OR (
				new.HighestScore IS NOT NULL
				AND old.HighestScore IS NULL
				)
			OR (
				new.HighestGrade IS NOT NULL
				AND old.HighestGrade IS NULL
				)
			OR (
				new.HighestAttemptDateTime IS NOT NULL
				AND old.HighestAttemptDateTime IS NULL
				)
			OR (
				new.ManualScore IS NOT NULL
				AND old.ManualScore IS NULL
				)
			OR (
				new.ManualGrade IS NOT NULL
				AND old.ManualGrade IS NULL
				)
			OR (
				new.ManualDateTime IS NOT NULL
				AND old.ManualDateTime IS NULL
				)
			OR (
				new.ExemptIndicator IS NOT NULL
				AND old.ExemptIndicator IS NULL
				)
			OR (
				new.HighestDateTimeCreated IS NOT NULL
				AND old.HighestDateTimeCreated IS NULL
				)
			OR (
				new.HighestDateTimeModified IS NOT NULL
				AND old.HighestDateTimeModified IS NULL
				)
			OR (
				new.HighestIsLatestAttemptIndicator IS NOT NULL
				AND old.HighestIsLatestAttemptIndicator IS NULL
				)
			OR (
				new.NumberOfAttempts IS NOT NULL
				AND old.NumberOfAttempts IS NULL
				)
			OR (
				new.FirstScore IS NOT NULL
				AND old.FirstScore IS NULL
				)
			OR (
				new.FirstGrade IS NOT NULL
				AND old.FirstGrade IS NULL
				)
			OR (
				new.FirstAttemptDateTime IS NOT NULL
				AND old.FirstAttemptDateTime IS NULL
				)
			OR (
				new.FirstIsLatestAttemptIndicator IS NOT NULL
				AND old.FirstIsLatestAttemptIndicator IS NULL
				)
			OR (
				new.FirstDateTimeCreated IS NOT NULL
				AND old.FirstDateTimeCreated IS NULL
				)
			OR (
				new.FirstDateTimeModified IS NOT NULL
				AND old.FirstDateTimeModified IS NULL
				)
			OR (
				new.AssignmentPrimaryKey IS NOT NULL
				AND old.AssignmentPrimaryKey IS NULL
				)
			OR (
				new.AssignmentStatus IS NOT NULL
				AND old.AssignmentStatus IS NULL
				)
			OR (
				new.SourceSystem IS NOT NULL
				AND old.SourceSystem IS NULL
				);

		--Performance Fix for LS_ODS Grades Table CRUD; Index Balancing
		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashActiveFlagLargeIncludeDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashAssignmentPrimaryKeyDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashCourseUsersPrimaryKeyDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_CourseUsersPKAssignPKActiveFG'
				)
		BEGIN
			DROP INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = '_dta_index_Grades_10_1223675407__K2_K23_K21_4'
				)
		BEGIN
			DROP INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_ODS_010'
				)
		BEGIN
			DROP INDEX idx_ODS_010 ON LS_ODS.Grades;
		END;

		--Update LS_ODS Grades Table To Inactivate Changed Grades Records 
		UPDATE old
		SET old.ActiveFlag = 0
		FROM LS_ODS.Grades old
		INNER JOIN @ChangedGrades new ON old.GradePrimaryKey = new.GradePrimaryKey;

		-- Update LS_ODS Grades Table To Inactivate Grades with Duplicated D2L Assignments
		UPDATE Grades
		SET Grades.[ActiveFlag] = 0
		FROM [LS_ODS].[Grades] Grades
		WHERE Grades.[AssignmentPrimaryKey] IN (
				SELECT [asg].[AssignmentPrimaryKey]
				FROM (
					SELECT [asg].[CoursePrimaryKey], [asg].[WeekNumber], [asg].[AssignmentTitle], COUNT(*) AS Total
					FROM [LS_ODS].[Assignments] asg
					GROUP BY [asg].[CoursePrimaryKey], [asg].[WeekNumber], [asg].[AssignmentTitle]
					HAVING COUNT(*) > 1
					) da
				INNER JOIN [LS_ODS].[Assignments] asg ON [da].[CoursePrimaryKey] = [asg].[CoursePrimaryKey]
					AND [da].[WeekNumber] = [asg].[WeekNumber]
					AND [da].[AssignmentTitle] = [asg].[AssignmentTitle]
				INNER JOIN [dbo].[COURSE_MAIN] cm ON [cm].PK1 = [asg].[CoursePrimaryKey]
					AND [cm].[SourceSystem] = 'D2L'
				LEFT JOIN [stage].[Assignments] sasg ON sasg.[AssignmentPrimaryKey] = [asg].[AssignmentPrimaryKey]
				WHERE [sasg].[AssignmentPrimaryKey] IS NULL
				)

		--Add Changed Grades Records To LS_ODS Grades Table 
		INSERT INTO LS_ODS.Grades (GradePrimaryKey, CourseUsersPrimaryKey, RowStatus, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, ManualDateTime, ExemptIndicator, HighestDateTimeCreated, HighestDateTimeModified, HighestIsLatestAttemptIndicator, NumberOfAttempts, FirstScore, FirstGrade, FirstAttemptDateTime, FirstIsLatestAttemptIndicator, FirstDateTimeCreated, FirstDateTimeModified, AssignmentPrimaryKey, AssignmentStatus, SourceSystem)
		SELECT new.GradePrimaryKey, new.CourseUsersPrimaryKey, new.RowStatus, new.HighestScore, new.HighestGrade, new.HighestAttemptDateTime, new.ManualScore, new.ManualGrade, new.ManualDateTime, new.ExemptIndicator, new.HighestDateTimeCreated, new.HighestDateTimeModified, new.HighestIsLatestAttemptIndicator, new.NumberOfAttempts, new.FirstScore, new.FirstGrade, new.FirstAttemptDateTime, new.FirstIsLatestAttemptIndicator, new.FirstDateTimeCreated, new.FirstDateTimeModified, new.AssignmentPrimaryKey, new.AssignmentStatus, new.SourceSystem
		FROM stage.Grades new
		INNER JOIN @ChangedGrades changed ON new.GradePrimaryKey = changed.GradePrimaryKey;

		--Performance Fix for LS_ODS Grades Table CRUD; Index Balancing
		CREATE NONCLUSTERED INDEX idx_ODS_010 ON LS_ODS.Grades (GradePrimaryKey ASC, ActiveFlag ASC) INCLUDE (CourseUsersPrimaryKey, RowStatus, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, ManualDateTime, ExemptIndicator, HighestDateTimeCreated, HighestDateTimeModified, HighestIsLatestAttemptIndicator, NumberOfAttempts, FirstScore, FirstGrade, FirstAttemptDateTime, FirstIsLatestAttemptIndicator, FirstDateTimeModified, AssignmentPrimaryKey, AssignmentStatus, SourceSystem)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades (CourseUsersPrimaryKey ASC, ActiveFlag ASC, AssignmentPrimaryKey ASC) INCLUDE (HighestScore)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades (CourseUsersPrimaryKey DESC) INCLUDE (GradePrimaryKey, AssignmentPrimaryKey, ActiveFlag, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades (CourseUsersPrimaryKey DESC, AssignmentPrimaryKey DESC, ActiveFlag DESC) INCLUDE (GradePrimaryKey, HighestScore, HighestDateTimeCreated, AssignmentStatus)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades (AssignmentPrimaryKey DESC) INCLUDE (GradePrimaryKey, CourseUsersPrimaryKey, ActiveFlag, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades (ActiveFlag DESC) INCLUDE (GradePrimaryKey, AssignmentPrimaryKey, CourseUsersPrimaryKey, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades (CourseUsersPrimaryKey ASC, AssignmentPrimaryKey ASC, ActiveFlag ASC) INCLUDE (GradePrimaryKey, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts, AssignmentStatus)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		EXEC LS_ODS.AddODSLoadLog 'Updated Grades Records That Have Changed', 0;

		--**************************************************************************************************************************************** 
		--Add new Student records 
		--**************************************************************************************************************************************** 
		--Insert New Student Records To Students Table 
		INSERT INTO LS_ODS.Students (
			StudentPrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, BlackboardUsername, SyStudentId, FirstName, LastName, Campus, AdEnrollSchedId, AdClassSchedId, CourseUsersPrimaryKey, LastLoginDateTime, TimeInClass, LastI3InteractionNumberMainPhone, LastI3InteractionDateTimeMainPhone, DaysSinceLastI3InteractionMainPhone, LastI3InteractionNumberWorkPhone, LastI3InteractionDateTimeWorkPhone, DaysSinceLastI3InteractionWorkPhone, LastI3InteractionNumberMobilePhone, LastI3InteractionDateTimeMobilePhone, DaysSinceLastI3InteractionMobilePhone, LastI3InteractionNumberOtherPhone, LastI3InteractionDateTimeOtherPhone, DaysSinceLastI3InteractionOtherPhone, Week1Grade, Week2Grade, Week3Grade, Week4Grade, Week5Grade, SelfTestsCount, AssessmentsCount, AssignmentsCount, DiscussionsCount, Week1CompletionRate, Week2CompletionRate, Week3CompletionRate, Week4CompletionRate, Week5CompletionRate, VAStudent, NoticeName, NoticeDueDate, VABenefitName, ClassStatus, Week1LDA, Week2LDA, Week3LDA, Week4LDA, Week5LDA, 
			Week1CompletedAssignments, Week2CompletedAssignments, Week3CompletedAssignments, Week4CompletedAssignments, Week5CompletedAssignments, CoursePercentage, TotalWorkPercentage, AdEnrollId, IsRetake, StudentCourseUserKeys, CurrentCourseGrade, ProgramCode, ProgramName, ProgramVersionCode, ProgramVersionName, MondayTimeInClass, TuesdayTimeInClass, WednesdayTimeInClass, ThursdayTimeInClass, FridayTimeInClass, SaturdayTimeInClass, SundayTimeInClass, Week1CompletionRateFixed, Week2CompletionRateFixed, Week3CompletionRateFixed, Week4CompletionRateFixed, Week5CompletionRateFixed, StudentNumber, SourceSystem
			)
		SELECT DISTINCT new.StudentPrimaryKey, new.DateTimeCreated, new.DateTimeModified, new.RowStatus, new.BatchUniqueIdentifier, new.BlackboardUsername, new.SyStudentId, new.FirstName, new.LastName, new.Campus, new.AdEnrollSchedId, new.AdClassSchedId, new.CourseUsersPrimaryKey, new.LastLoginDateTime, new.TimeInClass, new.LastI3InteractionNumberMainPhone, new.LastI3InteractionDateTimeMainPhone, new.DaysSinceLastI3InteractionMainPhone, new.LastI3InteractionNumberWorkPhone, new.LastI3InteractionDateTimeWorkPhone, new.DaysSinceLastI3InteractionWorkPhone, new.LastI3InteractionNumberMobilePhone, new.LastI3InteractionDateTimeMobilePhone, new.DaysSinceLastI3InteractionMobilePhone, new.LastI3InteractionNumberOtherPhone, new.LastI3InteractionDateTimeOtherPhone, new.DaysSinceLastI3InteractionOtherPhone, new.Week1Grade, new.Week2Grade, new.Week3Grade, new.Week4Grade, new.Week5Grade, new.SelfTestsCount, new.AssessmentsCount, new.AssignmentsCount, new.DiscussionsCount, new.Week1CompletionRate, new.Week2CompletionRate, new.
			Week3CompletionRate, new.Week4CompletionRate, new.Week5CompletionRate, new.VAStudent, new.NoticeName, new.NoticeDueDate, new.VABenefitName, new.ClassStatus, new.Week1LDA, new.Week2LDA, new.Week3LDA, new.Week4LDA, new.Week5LDA, new.Week1CompletedAssignments, new.Week2CompletedAssignments, new.Week3CompletedAssignments, new.Week4CompletedAssignments, new.Week5CompletedAssignments, new.CoursePercentage, new.TotalWorkPercentage, new.AdEnrollId, new.IsRetake, new.StudentCourseUserKeys, new.CurrentCourseGrade, new.ProgramCode, new.ProgramName, new.ProgramVersionCode, new.ProgramVersionName, new.MondayTimeInClass, new.TuesdayTimeInClass, new.WednesdayTimeInClass, new.ThursdayTimeInClass, new.FridayTimeInClass, new.SaturdayTimeInClass, new.SundayTimeInClass, new.Week1CompletionRateFixed, new.Week2CompletionRateFixed, new.Week3CompletionRateFixed, new.Week4CompletionRateFixed, new.Week5CompletionRateFixed, new.StudentNumber, new.SourceSystem
		FROM stage.Students new
		WHERE new.StudentCourseUserKeys NOT IN (
				SELECT old.StudentCourseUserKeys
				FROM LS_ODS.Students old
				WHERE old.StudentPrimaryKey IS NOT NULL
					AND old.CourseUsersPrimaryKey IS NOT NULL
				);

		EXEC LS_ODS.AddODSLoadLog 'Added New Students Records', 0;

		--**************************************************************************************************************************************** 
		--Add new Course records 
		--**************************************************************************************************************************************** 
		--Insert New Course Records To Courses Table 
		INSERT INTO LS_ODS.Courses (CoursePrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, CourseCode, CourseName, SectionNumber, SectionStart, SectionEnd, AdClassSchedId, WeekNumber, Week1AssignmentCount, Week2AssignmentCount, Week3AssignmentCount, Week4AssignmentCount, Week5AssignmentCount, PrimaryInstructor, SecondaryInstructor, Week1StartDate, Week2StartDate, Week3StartDate, Week4StartDate, Week5StartDate, ExtensionWeekStartDate, IsOrganization, AcademicFacilitator, PrimaryInstructorId, SecondaryInstructorId, AcademicFacilitatorId, DayNumber, CengageCourseIndicator, SourceSystem)
		SELECT new.CoursePrimaryKey, new.DateTimeCreated, new.DateTimeModified, new.RowStatus, new.BatchUniqueIdentifier, new.CourseCode, new.CourseName, new.SectionNumber, new.SectionStart, new.SectionEnd, new.AdClassSchedId, new.WeekNumber, new.Week1AssignmentCount, new.Week2AssignmentCount, new.Week3AssignmentCount, new.Week4AssignmentCount, new.Week5AssignmentCount, new.PrimaryInstructor, new.SecondaryInstructor, new.Week1StartDate, new.Week2StartDate, new.Week3StartDate, new.Week4StartDate, new.Week5StartDate, new.ExtensionWeekStartDate, new.IsOrganization, new.AcademicFacilitator, new.PrimaryInstructorId, new.SecondaryInstructorId, new.AcademicFacilitatorId, new.DayNumber, new.CengageCourseIndicator, new.SourceSystem
		FROM stage.Courses new
		WHERE NOT EXISTS (
				SELECT 1
				FROM LS_ODS.Courses old
				WHERE old.CoursePrimaryKey = new.CoursePrimaryKey
				);

		EXEC LS_ODS.AddODSLoadLog 'Added New Course Records', 0;

		--**************************************************************************************************************************************** 
		--Add new Assignment records 
		--**************************************************************************************************************************************** 
		--Insert New Assignment Records To Assignments Table 
		INSERT INTO LS_ODS.Assignments (AssignmentPrimaryKey, CoursePrimaryKey, WeekNumber, AssignmentTitle, DueDate, PossiblePoints, DateTimeCreated, DateTimeModified, ScoreProviderHandle, CourseContentsPrimaryKey1, AlternateTitle, IsReportable, CountsAsSubmission, AssignmentType, SourceSystem)
		SELECT new.AssignmentPrimaryKey, new.CoursePrimaryKey, new.WeekNumber, new.AssignmentTitle, new.DueDate, new.PossiblePoints, new.DateTimeCreated, new.DateTimeModified, new.ScoreProviderHandle, new.CourseContentsPrimaryKey1, new.AlternateTitle, new.IsReportable, new.CountsAsSubmission, new.AssignmentType, new.SourceSystem
		FROM stage.Assignments new
		WHERE NOT EXISTS (
				SELECT 1
				FROM LS_ODS.Assignments old
				WHERE old.AssignmentPrimaryKey = new.AssignmentPrimaryKey
				);

		EXEC LS_ODS.AddODSLoadLog 'Added New Assignment Records', 0;

		--**************************************************************************************************************************************** 
		--Add new Grade records 
		--**************************************************************************************************************************************** 
		--Insert New Grade Records Into Grades Table 
		INSERT INTO LS_ODS.Grades (GradePrimaryKey, CourseUsersPrimaryKey, RowStatus, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, ManualDateTime, ExemptIndicator, HighestDateTimeCreated, HighestDateTimeModified, HighestIsLatestAttemptIndicator, NumberOfAttempts, FirstScore, FirstGrade, FirstAttemptDateTime, FirstIsLatestAttemptIndicator, FirstDateTimeCreated, FirstDateTimeModified, AssignmentPrimaryKey, AssignmentStatus, SourceSystem)
		SELECT DISTINCT new.GradePrimaryKey, new.CourseUsersPrimaryKey, new.RowStatus, new.HighestScore, new.HighestGrade, new.HighestAttemptDateTime, new.ManualScore, new.ManualGrade, new.ManualDateTime, new.ExemptIndicator, new.HighestDateTimeCreated, new.HighestDateTimeModified, new.HighestIsLatestAttemptIndicator, new.NumberOfAttempts, new.FirstScore, new.FirstGrade, new.FirstAttemptDateTime, new.FirstIsLatestAttemptIndicator, new.FirstDateTimeCreated, new.FirstDateTimeModified, new.AssignmentPrimaryKey, new.AssignmentStatus, new.SourceSystem
		FROM stage.Grades new
		WHERE NOT EXISTS (
				SELECT 1
				FROM LS_ODS.Grades old
				WHERE old.GradePrimaryKey = new.GradePrimaryKey
				);

		EXEC LS_ODS.AddODSLoadLog 'Added New Grade Records', 0;

		--**************************************************************************************************************************************** 
		--Remove all records in the Students table with no StudentCourseUserKey 
		--**************************************************************************************************************************************** 
		DELETE
		FROM LS_ODS.Students
		WHERE StudentCourseUserKeys IS NULL;

		EXEC LS_ODS.AddODSLoadLog 'Removed Student Records With No Valid StudentCourseUserKey Value', 0;

		--**************************************************************************************************************************************** 
		--Handle Grade records with negative primary keys 
		--These come from Documents, Weekly Roadmaps, and various other "assignments" that are not true assignments. 
		--The negative value appears because the assignment has not been released to the student for use (adaptive release). 
		--We do not need to report on these value so we can just delete them from the database. 
		--**************************************************************************************************************************************** 
		DELETE
		FROM LS_ODS.Grades
		WHERE GradePrimaryKey < 0
			AND GradePrimaryKey NOT BETWEEN - 514999999
				AND - 514000000;

		EXEC LS_ODS.AddODSLoadLog 'Removed Grade Records With Negative Primary Keys', 0;

		--**************************************************************************************************************************************** 
		--Process Course Activity Counts for BI Reporting needs 
		--**************************************************************************************************************************************** 
		EXEC LS_ODS.ProcessCourseActivityCounts;

		EXEC LS_ODS.AddODSLoadLog 'Processed Course Activity Counts', 0;

		--**************************************************************************************************************************************** 
		--Create a distinct list of all courses to ensure any course no longer in the GradeExtract is disabled 
		--**************************************************************************************************************************************** 
		DECLARE @DisabledCourses TABLE (CoursePrimaryKey INT, AdClassSchedId INT);

		WITH cActiveCourses (CoursePrimaryKey, AdClassSchedId)
		AS (
			SELECT DISTINCT CAST(gei.CoursePK1 AS INT) 'CoursePrimaryKey', CAST(REPLACE(gei.CourseEPK, 'AdCourse_', '') AS INT) 'AdClassSchedId'
			FROM stage.GradeExtractImport gei
			WHERE LEFT(gei.UserEPK, 9) = 'SyStudent' --Only Students 
				AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8 --Filter Out Test/Bad Students 
				AND LEFT(gei.CourseEPK, 8) = 'AdCourse' --Only Courses 
				AND (
					gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --Filter Out Test/Bad Courses 
					OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --To bring in CLW courses 
					OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%' --2/28/2024 CML: Captures EMT Courses
					OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%'
					) --2/28/2024 CML: Captures EMT Courses based out of CLW 
				AND gei.UserFirstName NOT LIKE 'BBAFL%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART1%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART2%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART3%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART4%' --More Test Students
				AND gei.USEREPK NOT LIKE '%PART5%' --More Test Students
			), cAllCourses (CoursePrimaryKey, AdClassSchedId)
		AS (
			SELECT DISTINCT c.CoursePrimaryKey 'CoursePrimaryKey', c.AdClassSchedId 'AdClassSched'
			FROM LS_ODS.Courses c
			WHERE c.ActiveFlag = 1
			)
		INSERT INTO @DisabledCourses (CoursePrimaryKey, AdClassSchedId)
		SELECT ac.CoursePrimaryKey 'CoursePrimaryKey', ac.AdClassSchedId 'AdClassSchedId'
		FROM cAllCourses ac
		INNER JOIN cActiveCourses acc ON ac.AdClassSchedId = acc.AdClassSchedId
			AND ac.CoursePrimaryKey <> acc.CoursePrimaryKey;

		UPDATE c
		SET c.ActiveFlag = 0
		FROM LS_ODS.Courses c
		INNER JOIN @DisabledCourses dc ON c.CoursePrimaryKey = dc.CoursePrimaryKey
			AND c.AdClassSchedId = dc.AdClassSchedId;

		EXEC LS_ODS.AddODSLoadLog 'Removed Disable Courses', 0;

		--**************************************************************************************************************************************** 
		--Create a distinct list of all student/section combinations to ensure any student moved from one section to another has the old section disabled 
		--**************************************************************************************************************************************** 
		DECLARE @DisabledStudentCourseCombinations TABLE (SyStudentId INT, AdEnrollSchedId INT, AdClassSchedId INT);

		WITH cActiveStudentCourseCombinations (SystudentId, AdEnrollSchedId, AdClassSchedId)
		AS (
			SELECT DISTINCT REPLACE(gei.UserEPK, 'SyStudent_', '') 'SyStudentId', CAST(CAST(es.AdEnrollSchedID AS VARCHAR(100)) AS INT) 'AdEnrollSchedId', CAST(REPLACE(gei.CourseEPK, 'AdCourse_', '') AS INT) 'AdClassSchedId'
			FROM stage.GradeExtractImport gei
			LEFT JOIN CV_Prod.dbo.AdEnrollSched es ON REPLACE(gei.CourseEPK, 'AdCourse_', '') = CAST(es.AdClassSchedID AS VARCHAR(50))
				AND REPLACE(gei.UserEPK, 'SyStudent_', '') = CAST(es.SyStudentID AS VARCHAR(50))
			LEFT JOIN CV_Prod.dbo.AdClassSched cs ON CAST(REPLACE(gei.CourseEPK, 'AdCourse_', '') AS INT) = cs.AdClassSchedID
			WHERE LEFT(gei.UserEPK, 9) = 'SyStudent' --Only Students 
				AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8 --Filter Out Test/Bad Students 
				AND LEFT(gei.CourseEPK, 8) = 'AdCourse' --Only Courses 
				AND (
					gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --Filter Out Test/Bad Courses 
					OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --To bring in CLW courses 
					OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%' --2/28/2024 CML: Captures EMT Courses
					OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%'
					) --2/28/2024 CML: Captures EMT Courses based out of CLW 
				AND gei.UserFirstName NOT LIKE 'BBAFL%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART1%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART2%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART3%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART4%' --More Test Students
				AND gei.USEREPK NOT LIKE '%PART5%' --More Test Students
			), cAllStudentCourseCombinations (SyStudentId, AdEnrollSchedId, AdClassSchedId)
		AS (
			SELECT DISTINCT s.SyStudentId 'SyStudentId', s.AdEnrollSchedId 'AdEnrollSchedId', s.AdClassSchedId 'AdClassSchedId'
			FROM LS_ODS.Students s
			WHERE s.ActiveFlag = 1
			)
		INSERT INTO @DisabledStudentCourseCombinations (SyStudentId, AdEnrollSchedId, AdClassSchedId)
		SELECT cAllStudentsCourses.SyStudentId 'SyStudentId', cAllStudentsCourses.AdEnrollSchedId 'AdEnrollSched', cAllStudentsCourses.AdClassSchedId 'AdClassSched'
		FROM cAllStudentCourseCombinations cAllStudentsCourses
		INNER JOIN cActiveStudentCourseCombinations cActiveStudentsCourses ON cAllStudentsCourses.SyStudentId = cActiveStudentsCourses.SystudentId
			AND cAllStudentsCourses.AdEnrollSchedId = cActiveStudentsCourses.AdEnrollSchedId
			AND cAllStudentsCourses.AdClassSchedId <> cActiveStudentsCourses.AdClassSchedId;

		UPDATE s
		SET s.ActiveFlag = 0
		FROM LS_ODS.Students s
		INNER JOIN @DisabledStudentCourseCombinations dssc ON s.SyStudentId = dssc.SyStudentId
			AND s.AdEnrollSchedId = dssc.AdEnrollSchedId
			AND s.AdClassSchedId = dssc.AdClassSchedId;

		EXEC LS_ODS.AddODSLoadLog 'Removed Disabled Student/Course Combinations', 0;

		--**************************************************************************************************************************************** 
		--Disable all students with no matching CampusVue Enrollment records 
		--**************************************************************************************************************************************** 
		IF OBJECT_ID('tempdb..#NonMatchedStudents') IS NOT NULL
			DROP TABLE #NonMatchedStudents;

		CREATE TABLE #NonMatchedStudents (SyStudentId INT, AdEnrollSchedId INT, AdClassSchedId INT);

		INSERT INTO #NonMatchedStudents (SyStudentId, AdEnrollSchedId, AdClassSchedId)
		SELECT s.SyStudentId, s.AdEnrollSchedId, s.AdClassSchedId
		FROM LS_ODS.Students s;

		DELETE s
		FROM #NonMatchedStudents s
		INNER JOIN CV_Prod.dbo.AdEnrollSched es ON s.SyStudentId = es.SyStudentId
			AND s.AdEnrollSchedId = es.AdEnrollSchedId
			AND s.AdClassSchedId = es.AdClassSchedId;

		UPDATE s
		SET ActiveFlag = 0
		FROM LS_ODS.Students s
		INNER JOIN #NonMatchedStudents s2 ON s.SyStudentId = s2.SyStudentId
			AND s.AdEnrollSchedId = s2.AdEnrollSchedId
			AND s.AdClassSchedId = s2.AdClassSchedId;

		EXEC LS_ODS.AddODSLoadLog 'Removed Students With No CampusVue Enrollment Records', 0;

		--**************************************************************************************************************************************** 
		--Move old Students records to Audit table 
		--**************************************************************************************************************************************** 
		INSERT INTO Archive.Students (
			StudentPrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, BlackboardUsername, SyStudentId, FirstName, LastName, Campus, AdEnrollSchedId, AdClassSchedId, CourseUsersPrimaryKey, LastLoginDateTime, TimeInClass, LastI3InteractionNumberMainPhone, LastI3InteractionDateTimeMainPhone, DaysSinceLastI3InteractionMainPhone, LastI3InteractionNumberWorkPhone, LastI3InteractionDateTimeWorkPhone, DaysSinceLastI3InteractionWorkPhone, LastI3InteractionNumberMobilePhone, LastI3InteractionDateTimeMobilePhone, DaysSinceLastI3InteractionMobilePhone, LastI3InteractionNumberOtherPhone, LastI3InteractionDateTimeOtherPhone, DaysSinceLastI3InteractionOtherPhone, Week1Grade, Week2Grade, Week3Grade, Week4Grade, Week5Grade, SelfTestsCount, AssessmentsCount, AssignmentsCount, DiscussionsCount, Week1CompletionRate, Week2CompletionRate, Week3CompletionRate, Week4CompletionRate, Week5CompletionRate, ActiveFlag, UMADateTimeAdded, VAStudent, NoticeName, NoticeDueDate, VABenefitName, ClassStatus, ODSPrimaryKey, Week1LDA, 
			Week2LDA, Week3LDA, Week4LDA, Week5LDA, Week1CompletedAssignments, Week2CompletedAssignments, Week3CompletedAssignments, Week4CompletedAssignments, Week5CompletedAssignments, CoursePercentage, TotalWorkPercentage, AdEnrollId, IsRetake, StudentCourseUserKeys, CurrentCourseGrade, ProgramCode, ProgramName, ProgramVersionCode, ProgramVersionName, MondayTimeInClass, TuesdayTimeInClass, WednesdayTimeInClass, ThursdayTimeInClass, FridayTimeInClass, SaturdayTimeInClass, SundayTimeInClass, Week1CompletionRateFixed, Week2CompletionRateFixed, Week3CompletionRateFixed, Week4CompletionRateFixed, Week5CompletionRateFixed, StudentNumber, IsOrphanRecord, SourceSystem
			)
		SELECT s.StudentPrimaryKey, s.DateTimeCreated, s.DateTimeModified, s.RowStatus, s.BatchUniqueIdentifier, s.BlackboardUsername, s.SyStudentId, s.FirstName, s.LastName, s.Campus, s.AdEnrollSchedId, s.AdClassSchedId, s.CourseUsersPrimaryKey, s.LastLoginDateTime, s.TimeInClass, s.LastI3InteractionNumberMainPhone, s.LastI3InteractionDateTimeMainPhone, s.DaysSinceLastI3InteractionMainPhone, s.LastI3InteractionNumberWorkPhone, s.LastI3InteractionDateTimeWorkPhone, s.DaysSinceLastI3InteractionWorkPhone, s.LastI3InteractionNumberMobilePhone, s.LastI3InteractionDateTimeMobilePhone, s.DaysSinceLastI3InteractionMobilePhone, s.LastI3InteractionNumberOtherPhone, s.LastI3InteractionDateTimeOtherPhone, s.DaysSinceLastI3InteractionOtherPhone, s.Week1Grade, s.Week2Grade, s.Week3Grade, s.Week4Grade, s.Week5Grade, s.SelfTestsCount, s.AssessmentsCount, s.AssignmentsCount, s.DiscussionsCount, s.Week1CompletionRate, s.Week2CompletionRate, s.Week3CompletionRate, s.Week4CompletionRate, s.Week5CompletionRate, s.ActiveFlag, s.UMADateTimeAdded, s.
			VAStudent, s.NoticeName, s.NoticeDueDate, s.VABenefitName, s.ClassStatus, s.ODSPrimaryKey, s.Week1LDA, s.Week2LDA, s.Week3LDA, s.Week4LDA, s.Week5LDA, s.Week1CompletedAssignments, s.Week2CompletedAssignments, s.Week3CompletedAssignments, s.Week4CompletedAssignments, s.Week5CompletedAssignments, s.CoursePercentage, s.TotalWorkPercentage, s.AdEnrollId, s.IsRetake, s.StudentCourseUserKeys, s.CurrentCourseGrade, s.ProgramCode, s.ProgramName, s.ProgramVersionCode, s.ProgramVersionName, s.MondayTimeInClass, s.TuesdayTimeInClass, s.WednesdayTimeInClass, s.ThursdayTimeInClass, s.FridayTimeInClass, s.SaturdayTimeInClass, s.SundayTimeInClass, s.Week1CompletionRateFixed, s.Week2CompletionRateFixed, s.Week3CompletionRateFixed, s.Week4CompletionRateFixed, s.Week5CompletionRateFixed, s.StudentNumber, 0, s.SourceSystem
		FROM LS_ODS.Students s
		WHERE s.ActiveFlag = 0;

		DELETE
		FROM LS_ODS.Students
		WHERE ActiveFlag = 0;

		EXEC LS_ODS.AddODSLoadLog 'Moved Old Student Records To Archive Table', 0;

		--**************************************************************************************************************************************** 
		--Move old Courses records to Audit table 
		--**************************************************************************************************************************************** 
		INSERT INTO Archive.Courses (CoursePrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, CourseCode, CourseName, SectionNumber, SectionStart, SectionEnd, AdClassSchedId, WeekNumber, Week1AssignmentCount, Week2AssignmentCount, Week3AssignmentCount, Week4AssignmentCount, Week5AssignmentCount, ActiveFlag, UMADateTimeAdded, ODSPrimaryKey, PrimaryInstructor, SecondaryInstructor, Week1StartDate, Week2StartDate, Week3StartDate, Week4StartDate, Week5StartDate, ExtensionWeekStartDate, IsOrganziation, AcademicFacilitator, PrimaryInstructorId, SecondaryInstructorId, AcademicFacilitatorId, DayNumber, CengageCourseIndicator, SourceSystem)
		SELECT c.CoursePrimaryKey, c.DateTimeCreated, c.DateTimeModified, c.RowStatus, c.BatchUniqueIdentifier, c.CourseCode, c.CourseName, c.SectionNumber, c.SectionStart, c.SectionEnd, c.AdClassSchedId, c.WeekNumber, c.Week1AssignmentCount, c.Week2AssignmentCount, c.Week3AssignmentCount, c.Week4AssignmentCount, c.Week5AssignmentCount, c.ActiveFlag, c.UMADateTimeAdded, c.ODSPrimaryKey, c.PrimaryInstructor, c.SecondaryInstructor, c.Week1StartDate, c.Week2StartDate, c.Week3StartDate, c.Week4StartDate, c.Week5StartDate, c.ExtensionWeekStartDate, c.IsOrganization, c.AcademicFacilitator, c.PrimaryInstructorId, c.SecondaryInstructorId, c.AcademicFacilitatorId, c.DayNumber, c.CengageCourseIndicator, c.SourceSystem
		FROM LS_ODS.Courses c
		WHERE c.ActiveFlag = 0;

		DELETE
		FROM LS_ODS.Courses
		WHERE ActiveFlag = 0;

		EXEC LS_ODS.AddODSLoadLog 'Moved Old Course Records To Archive Table', 0;

		--**************************************************************************************************************************************** 
		--Move old Assignments records to Audit table 
		--**************************************************************************************************************************************** 
		INSERT INTO Archive.Assignments (AssignmentPrimaryKey, CoursePrimaryKey, WeekNumber, AssignmentTitle, DueDate, PossiblePoints, DateTimeCreated, DateTimeModified, ScoreProviderHandle, ActiveFlag, UMADateTimeAdded, CourseContentsPrimaryKey1, ODSPrimaryKey, AlternateTitle, IsReportable, CountsAsSubmission, AssignmentType, SourceSystem)
		SELECT a.AssignmentPrimaryKey, a.CoursePrimaryKey, a.WeekNumber, a.AssignmentTitle, a.DueDate, a.PossiblePoints, a.DateTimeCreated, a.DateTimeModified, a.ScoreProviderHandle, a.ActiveFlag, a.UMADateTimeAdded, a.CourseContentsPrimaryKey1, a.ODSPrimaryKey, a.AlternateTitle, a.IsReportable, a.CountsAsSubmission, a.AssignmentType, a.SourceSystem
		FROM LS_ODS.Assignments a
		WHERE a.ActiveFlag = 0;

		DELETE
		FROM LS_ODS.Assignments
		WHERE ActiveFlag = 0;

		EXEC LS_ODS.AddODSLoadLog 'Moved Old Assignments Records To Archive Table', 0;

		----**************************************************************************************************************************************** 
		----Move old Grades records to Audit table 
		----**************************************************************************************************************************************** 
		--Performance Fix for LS_ODS Grades Table CRUD; Index Balancing
		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashActiveFlagLargeIncludeDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashAssignmentPrimaryKeyDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashCourseUsersPrimaryKeyDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_CourseUsersPKAssignPKActiveFG'
				)
		BEGIN
			DROP INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = '_dta_index_Grades_10_1223675407__K2_K23_K21_4'
				)
		BEGIN
			DROP INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_ODS_010'
				)
		BEGIN
			DROP INDEX idx_ODS_010 ON LS_ODS.Grades;
		END;

		--Merge into audit table     
		DROP TABLE

		IF EXISTS #LSODSGRADE
			SELECT *
			INTO #LSODSGRADE
			FROM LS_ODS.Grades
			WHERE ActiveFlag = 0

		DELETE
		FROM #LSODSGRADE
		WHERE ODSPrimaryKey IN (
				SELECT ODSPrimaryKey
				FROM Audit.Grades
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
		WHEN NOT MATCHED BY TARGET
			AND src.ActiveFlag = 0
			THEN
				INSERT ([GradePrimaryKey], [CourseUsersPrimaryKey], [RowStatus], [HighestScore], [HighestGrade], [HighestAttemptDateTime], [ManualScore], [ManualGrade], [ManualDateTime], [ExemptIndicator], [HighestDateTimeCreated], [HighestDateTimeModified], [HighestIsLatestAttemptIndicator], [NumberOfAttempts], [FirstScore], [FirstGrade], [FirstAttemptDateTime], [FirstIsLatestAttemptIndicator], [FirstDateTimeCreated], [FirstDateTimeModified], [AssignmentPrimaryKey], [AssignmentStatus], [ActiveFlag], [UMADateTimeAdded], [ODSPrimaryKey], SourceSystem)
				VALUES (src.[GradePrimaryKey], src.[CourseUsersPrimaryKey], src.[RowStatus], src.[HighestScore], src.[HighestGrade], src.[HighestAttemptDateTime], src.[ManualScore], src.[ManualGrade], src.[ManualDateTime], src.[ExemptIndicator], src.[HighestDateTimeCreated], src.[HighestDateTimeModified], src.[HighestIsLatestAttemptIndicator], src.[NumberOfAttempts], src.[FirstScore], src.[FirstGrade], src.[FirstAttemptDateTime], src.[FirstIsLatestAttemptIndicator], src.[FirstDateTimeCreated], src.[FirstDateTimeModified], src.[AssignmentPrimaryKey], src.[AssignmentStatus], src.[ActiveFlag], src.[UMADateTimeAdded], src.[ODSPrimaryKey], src.SourceSystem)
		WHEN MATCHED
			THEN
				UPDATE
				SET trg.[GradePrimaryKey] = src.[GradePrimaryKey], trg.[CourseUsersPrimaryKey] = src.[CourseUsersPrimaryKey], trg.[RowStatus] = src.[RowStatus], trg.[HighestScore] = src.[HighestScore], trg.[HighestGrade] = src.[HighestGrade], trg.[HighestAttemptDateTime] = src.[HighestAttemptDateTime], trg.[ManualScore] = src.[ManualScore], trg.[ManualGrade] = src.[ManualGrade], trg.[ManualDateTime] = src.[ManualDateTime], trg.[ExemptIndicator] = src.[ExemptIndicator], trg.[HighestDateTimeCreated] = src.[HighestDateTimeCreated], trg.[HighestDateTimeModified] = src.[HighestDateTimeModified], trg.[HighestIsLatestAttemptIndicator] = src.[HighestIsLatestAttemptIndicator], trg.[NumberOfAttempts] = src.[NumberOfAttempts], trg.[FirstScore] = src.[FirstScore], trg.[FirstGrade] = src.[FirstGrade], trg.[FirstAttemptDateTime] = src.[FirstAttemptDateTime], trg.[FirstIsLatestAttemptIndicator] = src.[FirstIsLatestAttemptIndicator], trg.[FirstDateTimeCreated] = src.[FirstDateTimeCreated], trg.[FirstDateTimeModified] = src.[FirstDateTimeModified], trg.
					[AssignmentPrimaryKey] = src.[AssignmentPrimaryKey], trg.[AssignmentStatus] = src.[AssignmentStatus], trg.[ActiveFlag] = src.[ActiveFlag], trg.[UMADateTimeAdded] = src.[UMADateTimeAdded], trg.[ODSPrimaryKey] = src.[ODSPrimaryKey], trg.SourceSystem = src.SourceSystem;

		DELETE
		FROM LS_ODS.Grades
		WHERE ActiveFlag = 0;

		--Performance Fix for LS_ODS Grades Table CRUD; Index Balancing
		CREATE NONCLUSTERED INDEX idx_ODS_010 ON LS_ODS.Grades (GradePrimaryKey ASC, ActiveFlag ASC) INCLUDE (CourseUsersPrimaryKey, RowStatus, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, ManualDateTime, ExemptIndicator, HighestDateTimeCreated, HighestDateTimeModified, HighestIsLatestAttemptIndicator, NumberOfAttempts, FirstScore, FirstGrade, FirstAttemptDateTime, FirstIsLatestAttemptIndicator, FirstDateTimeModified, AssignmentPrimaryKey, AssignmentStatus, SourceSystem)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades (CourseUsersPrimaryKey ASC, ActiveFlag ASC, AssignmentPrimaryKey ASC) INCLUDE (HighestScore)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades (CourseUsersPrimaryKey DESC) INCLUDE (GradePrimaryKey, AssignmentPrimaryKey, ActiveFlag, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades (CourseUsersPrimaryKey DESC, AssignmentPrimaryKey DESC, ActiveFlag DESC) INCLUDE (GradePrimaryKey, HighestScore, HighestDateTimeCreated, AssignmentStatus)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades (AssignmentPrimaryKey DESC) INCLUDE (GradePrimaryKey, CourseUsersPrimaryKey, ActiveFlag, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades (ActiveFlag DESC) INCLUDE (GradePrimaryKey, AssignmentPrimaryKey, CourseUsersPrimaryKey, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades (CourseUsersPrimaryKey ASC, AssignmentPrimaryKey ASC, ActiveFlag ASC) INCLUDE (GradePrimaryKey, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts, AssignmentStatus)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		EXEC LS_ODS.AddODSLoadLog 'Moved Old Grades Records To Archive Table', 0;

		--If Weekly Course Graded Activity and Weekly Course Grades steps are running after 9am, we should let LS know.
		--**************************************************************************************************************************************** 
		--Remove all duplicates from each of the ODS tables 
		--**************************************************************************************************************************************** 
		WITH cteStudent
		AS (
			SELECT s.StudentPrimaryKey, s.DateTimeCreated, s.DateTimeModified, s.RowStatus, s.BatchUniqueIdentifier, s.BlackboardUsername, s.SyStudentId, s.FirstName, s.LastName, s.Campus, s.AdEnrollSchedId, s.AdClassSchedId, s.CourseUsersPrimaryKey, s.LastLoginDateTime, s.TimeInClass, s.LastI3InteractionNumberMainPhone, s.LastI3InteractionDateTimeMainPhone, s.DaysSinceLastI3InteractionMainPhone, s.LastI3InteractionNumberWorkPhone, s.LastI3InteractionDateTimeWorkPhone, s.DaysSinceLastI3InteractionWorkPhone, s.LastI3InteractionNumberMobilePhone, s.LastI3InteractionDateTimeMobilePhone, s.DaysSinceLastI3InteractionMobilePhone, s.LastI3InteractionNumberOtherPhone, s.LastI3InteractionDateTimeOtherPhone, s.DaysSinceLastI3InteractionOtherPhone, s.Week1Grade, s.Week2Grade, s.Week3Grade, s.Week4Grade, s.Week5Grade, s.SelfTestsCount, s.AssessmentsCount, s.AssignmentsCount, s.DiscussionsCount, s.ActivitiesCount, s.Week1CompletionRate, s.Week2CompletionRate, s.Week3CompletionRate, s.Week4CompletionRate, s.Week5CompletionRate, s.ActiveFlag
				, s.UMADateTimeAdded, s.VAStudent, s.NoticeName, s.NoticeDueDate, s.VABenefitName, s.ClassStatus, s.Week1LDA, s.Week2LDA, s.Week3LDA, s.Week4LDA, s.Week5LDA, s.Week1CompletedAssignments, s.Week2CompletedAssignments, s.Week3CompletedAssignments, s.Week4CompletedAssignments, s.Week5CompletedAssignments, s.CoursePercentage, s.TotalWorkPercentage, s.AdEnrollId, s.IsRetake, s.StudentCourseUserKeys, s.CurrentCourseGrade, s.ProgramCode, s.ProgramName, s.ProgramVersionCode, s.ProgramVersionName, s.MondayTimeInClass, s.TuesdayTimeInClass, s.WednesdayTimeInClass, s.ThursdayTimeInClass, s.FridayTimeInClass, s.SaturdayTimeInClass, s.SundayTimeInClass, s.Week1CompletionRateFixed, s.Week2CompletionRateFixed, s.Week3CompletionRateFixed, s.Week4CompletionRateFixed, s.Week5CompletionRateFixed, s.StudentNumber, s.SourceSystem, ROW_NUMBER() OVER (
					PARTITION BY s.StudentPrimaryKey, s.DateTimeCreated, s.DateTimeModified, s.RowStatus, s.BatchUniqueIdentifier, s.BlackboardUsername, s.SyStudentId, s.FirstName, s.LastName, s.Campus, s.AdEnrollSchedId, s.AdClassSchedId, s.CourseUsersPrimaryKey, s.LastLoginDateTime, s.TimeInClass, s.LastI3InteractionNumberMainPhone, s.LastI3InteractionDateTimeMainPhone, s.DaysSinceLastI3InteractionMainPhone, s.LastI3InteractionNumberWorkPhone, s.LastI3InteractionDateTimeWorkPhone, s.DaysSinceLastI3InteractionWorkPhone, s.LastI3InteractionNumberMobilePhone, s.LastI3InteractionDateTimeMobilePhone, s.DaysSinceLastI3InteractionMobilePhone, s.LastI3InteractionNumberOtherPhone, s.LastI3InteractionDateTimeOtherPhone, s.DaysSinceLastI3InteractionOtherPhone, s.Week1Grade, s.Week2Grade, s.Week3Grade, s.Week4Grade, s.Week5Grade, s.SelfTestsCount, s.AssessmentsCount, s.AssignmentsCount, s.DiscussionsCount, s.ActivitiesCount, s.Week1CompletionRate, s.Week2CompletionRate, s.Week3CompletionRate, s.Week4CompletionRate, s.Week5CompletionRate
					, s.ActiveFlag, s.UMADateTimeAdded, s.VAStudent, s.NoticeName, s.NoticeDueDate, s.VABenefitName, s.ClassStatus, s.Week1LDA, s.Week2LDA, s.Week3LDA, s.Week4LDA, s.Week5LDA, s.Week1CompletedAssignments, s.Week2CompletedAssignments, s.Week3CompletedAssignments, s.Week4CompletedAssignments, s.Week5CompletedAssignments, s.CoursePercentage, s.TotalWorkPercentage, s.AdEnrollId, s.IsRetake, s.StudentCourseUserKeys, s.CurrentCourseGrade, s.ProgramCode, s.ProgramName, s.ProgramVersionCode, s.ProgramVersionName, s.MondayTimeInClass, s.TuesdayTimeInClass, s.WednesdayTimeInClass, s.ThursdayTimeInClass, s.FridayTimeInClass, s.SaturdayTimeInClass, s.SundayTimeInClass, s.Week1CompletionRateFixed, s.Week2CompletionRateFixed, s.Week3CompletionRateFixed, s.Week4CompletionRateFixed, s.Week5CompletionRateFixed, s.StudentNumber, s.SourceSystem ORDER BY s.UMADateTimeAdded
					) 'RowNumber'
			FROM LS_ODS.Students s
			)
		DELETE cteStudent
		WHERE RowNumber > 1;

		EXEC LS_ODS.AddODSLoadLog 'Student Duplicate Check And Deletion Complete', 0;

		WITH cteCourse
		AS (
			SELECT c.CoursePrimaryKey, c.DateTimeCreated, c.DateTimeModified, c.RowStatus, c.BatchUniqueIdentifier, c.CourseCode, c.CourseName, c.SectionNumber, c.SectionStart, c.SectionEnd, c.AdClassSchedId, c.WeekNumber, c.Week1AssignmentCount, c.Week2AssignmentCount, c.Week3AssignmentCount, c.Week4AssignmentCount, c.Week5AssignmentCount, c.TotalAssignmentCount, c.ActiveFlag, c.UMADateTimeAdded, c.PrimaryInstructor, c.SecondaryInstructor, c.Week1StartDate, c.Week2StartDate, c.Week3StartDate, c.Week4StartDate, c.Week5StartDate, c.ExtensionWeekStartDate, c.IsOrganization, c.AcademicFacilitator, c.PrimaryInstructorId, c.SecondaryInstructorId, c.AcademicFacilitatorId, c.DayNumber, c.CengageCourseIndicator, c.SourceSystem, ROW_NUMBER() OVER (
					PARTITION BY c.CoursePrimaryKey, c.DateTimeCreated, c.DateTimeModified, c.RowStatus, c.BatchUniqueIdentifier, c.CourseCode, c.CourseName, c.SectionNumber, c.SectionStart, c.SectionEnd, c.AdClassSchedId, c.WeekNumber, c.Week1AssignmentCount, c.Week2AssignmentCount, c.Week3AssignmentCount, c.Week4AssignmentCount, c.Week5AssignmentCount, c.TotalAssignmentCount, c.ActiveFlag, c.UMADateTimeAdded, c.PrimaryInstructor, c.SecondaryInstructor, c.Week1StartDate, c.Week2StartDate, c.Week3StartDate, c.Week4StartDate, c.Week5StartDate, c.ExtensionWeekStartDate, c.IsOrganization, c.AcademicFacilitator, c.PrimaryInstructorId, c.SecondaryInstructorId, c.AcademicFacilitatorId, c.DayNumber, c.CengageCourseIndicator, c.SourceSystem ORDER BY c.UMADateTimeAdded
					) 'RowNumber'
			FROM LS_ODS.Courses c
			)
		DELETE cteCourse
		WHERE cteCourse.RowNumber > 1;

		EXEC LS_ODS.AddODSLoadLog 'Course Duplicate Check And Deletion Complete', 0;

		WITH cteAssignment
		AS (
			SELECT a.AssignmentPrimaryKey, a.CoursePrimaryKey, a.WeekNumber, a.AssignmentTitle, a.DueDate, a.PossiblePoints, a.DateTimeCreated, a.DateTimeModified, a.ScoreProviderHandle, a.ActiveFlag, a.UMADateTimeAdded, a.CourseContentsPrimaryKey1, a.AlternateTitle, a.IsReportable, a.CountsAsSubmission, a.AssignmentType, a.SourceSystem, ROW_NUMBER() OVER (
					PARTITION BY a.AssignmentPrimaryKey, a.CoursePrimaryKey, a.WeekNumber, a.AssignmentTitle, a.DueDate, a.PossiblePoints, a.DateTimeCreated, a.DateTimeModified, a.ScoreProviderHandle, a.ActiveFlag, a.UMADateTimeAdded, a.CourseContentsPrimaryKey1, a.AlternateTitle, a.IsReportable, a.CountsAsSubmission, a.AssignmentType, a.SourceSystem ORDER BY a.UMADateTimeAdded
					) 'RowNumber'
			FROM LS_ODS.Assignments a
			)
		DELETE
		FROM cteAssignment
		WHERE RowNumber > 1;

		EXEC LS_ODS.AddODSLoadLog 'Assignment Duplicate Check And Deletion Complete', 0;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashActiveFlagLargeIncludeDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashAssignmentPrimaryKeyDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashCourseUsersPrimaryKeyDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_CourseUsersPKAssignPKActiveFG'
				)
		BEGIN
			DROP INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = '_dta_index_Grades_10_1223675407__K2_K23_K21_4'
				)
		BEGIN
			DROP INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_ODS_010'
				)
		BEGIN
			DROP INDEX idx_ODS_010 ON LS_ODS.Grades;
		END;

		WITH cteGrade
		AS (
			SELECT g.GradePrimaryKey, g.CourseUsersPrimaryKey, g.RowStatus, g.HighestScore, g.HighestGrade, g.HighestAttemptDateTime, g.ManualScore, g.ManualGrade, g.ManualDateTime, g.ExemptIndicator, g.HighestDateTimeCreated, g.HighestDateTimeModified, g.HighestIsLatestAttemptIndicator, g.NumberOfAttempts, g.FirstScore, g.FirstGrade, g.FirstAttemptDateTime, g.FirstIsLatestAttemptIndicator, g.FirstDateTimeCreated, g.FirstDateTimeModified, g.AssignmentPrimaryKey, g.AssignmentStatus, g.ActiveFlag, g.UMADateTimeAdded, g.SourceSystem, ROW_NUMBER() OVER (
					PARTITION BY g.GradePrimaryKey, g.CourseUsersPrimaryKey, g.RowStatus, g.HighestScore, g.HighestGrade, g.HighestAttemptDateTime, g.ManualScore, g.ManualGrade, g.ManualDateTime, g.ExemptIndicator, g.HighestDateTimeCreated, g.HighestDateTimeModified, g.HighestIsLatestAttemptIndicator, g.NumberOfAttempts, g.FirstScore, g.FirstGrade, g.FirstAttemptDateTime, g.FirstIsLatestAttemptIndicator, g.FirstDateTimeCreated, g.FirstDateTimeModified, g.AssignmentPrimaryKey, g.AssignmentStatus, g.ActiveFlag, g.UMADateTimeAdded, g.SourceSystem ORDER BY g.UMADateTimeAdded
					) 'RowNumber'
			FROM LS_ODS.Grades g
			)
		DELETE
		FROM cteGrade
		WHERE RowNumber > 1;

		--Set Active Flag For All Grades Records To Active Flag = 0 For Duplicate Check 
		UPDATE LS_ODS.Grades
		SET ActiveFlag = 0;

		--Update The Most Recent Grade Record To Have Active Flag = 1 
		WITH cteMaxDates (CourseUsersPrimaryKey, AssignmentPrimaryKey, MaxDate)
		AS (
			SELECT g.CourseUsersPrimaryKey, g.AssignmentPrimaryKey, MAX(g.UMADateTimeAdded) 'MaxDate'
			FROM LS_ODS.Grades g
			GROUP BY g.CourseUsersPrimaryKey, g.AssignmentPrimaryKey
			)
		UPDATE g
		SET g.ActiveFlag = 1
		FROM LS_ODS.Grades g
		INNER JOIN cteMaxDates md ON g.CourseUsersPrimaryKey = md.CourseUsersPrimaryKey
			AND g.AssignmentPrimaryKey = md.AssignmentPrimaryKey
			AND g.UMADateTimeAdded = md.MaxDate;

		DROP TABLE

		IF EXISTS #LSODSGRADE1
			SELECT *
			INTO #LSODSGRADE1
			FROM LS_ODS.Grades
			WHERE ActiveFlag = 0

		DELETE
		FROM #LSODSGRADE1
		WHERE ODSPrimaryKey IN (
				SELECT ODSPrimaryKey
				FROM Audit.Grades
				)

		MERGE INTO audit.Grades AS trg
		USING #LSODSGRADE1 AS src
			ON src.GradePrimaryKey = trg.GradePrimaryKey
				AND src.CourseUsersPrimaryKey = trg.CourseUsersPrimaryKey
				AND src.HighestScore = trg.HighestScore
				AND src.HighestGrade = trg.HighestGrade
				AND src.AssignmentPrimaryKey = trg.AssignmentPrimaryKey
				AND src.RowStatus = trg.RowStatus
				AND src.AssignmentStatus = trg.AssignmentStatus
		WHEN NOT MATCHED BY TARGET
			THEN
				INSERT ([GradePrimaryKey], [CourseUsersPrimaryKey], [RowStatus], [HighestScore], [HighestGrade], [HighestAttemptDateTime], [ManualScore], [ManualGrade], [ManualDateTime], [ExemptIndicator], [HighestDateTimeCreated], [HighestDateTimeModified], [HighestIsLatestAttemptIndicator], [NumberOfAttempts], [FirstScore], [FirstGrade], [FirstAttemptDateTime], [FirstIsLatestAttemptIndicator], [FirstDateTimeCreated], [FirstDateTimeModified], [AssignmentPrimaryKey], [AssignmentStatus], [ActiveFlag], [UMADateTimeAdded], [ODSPrimaryKey], SourceSystem)
				VALUES (src.[GradePrimaryKey], src.[CourseUsersPrimaryKey], src.[RowStatus], src.[HighestScore], src.[HighestGrade], src.[HighestAttemptDateTime], src.[ManualScore], src.[ManualGrade], src.[ManualDateTime], src.[ExemptIndicator], src.[HighestDateTimeCreated], src.[HighestDateTimeModified], src.[HighestIsLatestAttemptIndicator], src.[NumberOfAttempts], src.[FirstScore], src.[FirstGrade], src.[FirstAttemptDateTime], src.[FirstIsLatestAttemptIndicator], src.[FirstDateTimeCreated], src.[FirstDateTimeModified], src.[AssignmentPrimaryKey], src.[AssignmentStatus], src.[ActiveFlag], src.[UMADateTimeAdded], src.[ODSPrimaryKey], src.SourceSystem)
		WHEN MATCHED
			THEN
				UPDATE
				SET trg.[GradePrimaryKey] = src.[GradePrimaryKey], trg.[CourseUsersPrimaryKey] = src.[CourseUsersPrimaryKey], trg.[RowStatus] = src.[RowStatus], trg.[HighestScore] = src.[HighestScore], trg.[HighestGrade] = src.[HighestGrade], trg.[HighestAttemptDateTime] = src.[HighestAttemptDateTime], trg.[ManualScore] = src.[ManualScore], trg.[ManualGrade] = src.[ManualGrade], trg.[ManualDateTime] = src.[ManualDateTime], trg.[ExemptIndicator] = src.[ExemptIndicator], trg.[HighestDateTimeCreated] = src.[HighestDateTimeCreated], trg.[HighestDateTimeModified] = src.[HighestDateTimeModified], trg.[HighestIsLatestAttemptIndicator] = src.[HighestIsLatestAttemptIndicator], trg.[NumberOfAttempts] = src.[NumberOfAttempts], trg.[FirstScore] = src.[FirstScore], trg.[FirstGrade] = src.[FirstGrade], trg.[FirstAttemptDateTime] = src.[FirstAttemptDateTime], trg.[FirstIsLatestAttemptIndicator] = src.[FirstIsLatestAttemptIndicator], trg.[FirstDateTimeCreated] = src.[FirstDateTimeCreated], trg.[FirstDateTimeModified] = src.[FirstDateTimeModified], trg.
					[AssignmentPrimaryKey] = src.[AssignmentPrimaryKey], trg.[AssignmentStatus] = src.[AssignmentStatus], trg.[ActiveFlag] = src.[ActiveFlag], trg.[UMADateTimeAdded] = src.[UMADateTimeAdded], trg.[ODSPrimaryKey] = src.[ODSPrimaryKey], trg.SourceSystem = src.SourceSystem;

		--Delete Anything Left With Active Flag = 0 
		DELETE
		FROM LS_ODS.Grades
		WHERE ActiveFlag = 0;

		CREATE NONCLUSTERED INDEX idx_ODS_010 ON LS_ODS.Grades (GradePrimaryKey ASC, ActiveFlag ASC) INCLUDE (CourseUsersPrimaryKey, RowStatus, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, ManualDateTime, ExemptIndicator, HighestDateTimeCreated, HighestDateTimeModified, HighestIsLatestAttemptIndicator, NumberOfAttempts, FirstScore, FirstGrade, FirstAttemptDateTime, FirstIsLatestAttemptIndicator, FirstDateTimeModified, AssignmentPrimaryKey, AssignmentStatus, SourceSystem)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades (CourseUsersPrimaryKey ASC, ActiveFlag ASC, AssignmentPrimaryKey ASC) INCLUDE (HighestScore)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades (CourseUsersPrimaryKey DESC) INCLUDE (GradePrimaryKey, AssignmentPrimaryKey, ActiveFlag, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades (CourseUsersPrimaryKey DESC, AssignmentPrimaryKey DESC, ActiveFlag DESC) INCLUDE (GradePrimaryKey, HighestScore, HighestDateTimeCreated, AssignmentStatus)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades (AssignmentPrimaryKey DESC) INCLUDE (GradePrimaryKey, CourseUsersPrimaryKey, ActiveFlag, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades (ActiveFlag DESC) INCLUDE (GradePrimaryKey, AssignmentPrimaryKey, CourseUsersPrimaryKey, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades (CourseUsersPrimaryKey ASC, AssignmentPrimaryKey ASC, ActiveFlag ASC) INCLUDE (GradePrimaryKey, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts, AssignmentStatus)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		EXEC LS_ODS.AddODSLoadLog 'Grade Duplicate Check And Deletion Complete', 0;

		--**************************************************************************************************************************************** 
		--Remove Orphaned Student Records - These are students who were in course X, started course Y then received a failing grade for course X. 
		--	The student is removed from course Y and re-enrolled in another course X.  If the student had no activity in the course Y to turn 
		--	them Active in that course, the course enrollment record in CampusVue is removed.  This leaves the record for course Y orphaned in our 
		--	data set.  This proces will move those records to the Archive table with a IsOrphanRecord flag set to true. 
		--**************************************************************************************************************************************** 
		INSERT INTO Archive.Students (
			StudentPrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, BlackboardUsername, SyStudentId, FirstName, LastName, Campus, AdEnrollSchedId, AdClassSchedId, CourseUsersPrimaryKey, LastLoginDateTime, TimeInClass, LastI3InteractionNumberMainPhone, LastI3InteractionDateTimeMainPhone, DaysSinceLastI3InteractionMainPhone, LastI3InteractionNumberWorkPhone, LastI3InteractionDateTimeWorkPhone, DaysSinceLastI3InteractionWorkPhone, LastI3InteractionNumberMobilePhone, LastI3InteractionDateTimeMobilePhone, DaysSinceLastI3InteractionMobilePhone, LastI3InteractionNumberOtherPhone, LastI3InteractionDateTimeOtherPhone, DaysSinceLastI3InteractionOtherPhone, Week1Grade, Week2Grade, Week3Grade, Week4Grade, Week5Grade, SelfTestsCount, AssessmentsCount, AssignmentsCount, DiscussionsCount, Week1CompletionRate, Week2CompletionRate, Week3CompletionRate, Week4CompletionRate, Week5CompletionRate, ActiveFlag, UMADateTimeAdded, VAStudent, NoticeName, NoticeDueDate, VABenefitName, ClassStatus, ODSPrimaryKey, Week1LDA, 
			Week2LDA, Week3LDA, Week4LDA, Week5LDA, Week1CompletedAssignments, Week2CompletedAssignments, Week3CompletedAssignments, Week4CompletedAssignments, Week5CompletedAssignments, CoursePercentage, TotalWorkPercentage, AdEnrollId, IsRetake, StudentCourseUserKeys, CurrentCourseGrade, ProgramCode, ProgramName, ProgramVersionCode, ProgramVersionName, MondayTimeInClass, TuesdayTimeInClass, WednesdayTimeInClass, ThursdayTimeInClass, FridayTimeInClass, SaturdayTimeInClass, SundayTimeInClass, Week1CompletionRateFixed, Week2CompletionRateFixed, Week3CompletionRateFixed, Week4CompletionRateFixed, Week5CompletionRateFixed, StudentNumber, IsOrphanRecord, SourceSystem
			)
		SELECT s.StudentPrimaryKey, s.DateTimeCreated, s.DateTimeModified, s.RowStatus, s.BatchUniqueIdentifier, s.BlackboardUsername, s.SyStudentId, s.FirstName, s.LastName, s.Campus, s.AdEnrollSchedId, s.AdClassSchedId, s.CourseUsersPrimaryKey, s.LastLoginDateTime, s.TimeInClass, s.LastI3InteractionNumberMainPhone, s.LastI3InteractionDateTimeMainPhone, s.DaysSinceLastI3InteractionMainPhone, s.LastI3InteractionNumberWorkPhone, s.LastI3InteractionDateTimeWorkPhone, s.DaysSinceLastI3InteractionWorkPhone, s.LastI3InteractionNumberMobilePhone, s.LastI3InteractionDateTimeMobilePhone, s.DaysSinceLastI3InteractionMobilePhone, s.LastI3InteractionNumberOtherPhone, s.LastI3InteractionDateTimeOtherPhone, s.DaysSinceLastI3InteractionOtherPhone, s.Week1Grade, s.Week2Grade, s.Week3Grade, s.Week4Grade, s.Week5Grade, s.SelfTestsCount, s.AssessmentsCount, s.AssignmentsCount, s.DiscussionsCount, s.Week1CompletionRate, s.Week2CompletionRate, s.Week3CompletionRate, s.Week4CompletionRate, s.Week5CompletionRate, s.ActiveFlag, s.UMADateTimeAdded, s.
			VAStudent, s.NoticeName, s.NoticeDueDate, s.VABenefitName, s.ClassStatus, s.ODSPrimaryKey, s.Week1LDA, s.Week2LDA, s.Week3LDA, s.Week4LDA, s.Week5LDA, s.Week1CompletedAssignments, s.Week2CompletedAssignments, s.Week3CompletedAssignments, s.Week4CompletedAssignments, s.Week5CompletedAssignments, s.CoursePercentage, s.TotalWorkPercentage, s.AdEnrollId, s.IsRetake, s.StudentCourseUserKeys, s.CurrentCourseGrade, s.ProgramCode, s.ProgramName, s.ProgramVersionCode, s.ProgramVersionName, s.MondayTimeInClass, s.TuesdayTimeInClass, s.WednesdayTimeInClass, s.ThursdayTimeInClass, s.FridayTimeInClass, s.SaturdayTimeInClass, s.SundayTimeInClass, s.Week1CompletionRateFixed, s.Week2CompletionRateFixed, s.Week3CompletionRateFixed, s.Week4CompletionRateFixed, s.Week5CompletionRateFixed, s.StudentNumber, 1, s.SourceSystem
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
		DECLARE @BeginRangeDate DATE;
		DECLARE @EndRangeDate DATE;
		DECLARE @TodaysDate DATE;

		SET @BeginRangeDate = '2012-04-01';
		SET @EndRangeDate = DATEADD(DAY, - 1, GETDATE());
		SET @TodaysDate = GETDATE();

		TRUNCATE TABLE LS_ODS.LDACounts;

		DECLARE @AllDates TABLE (TheDate DATE);

		WITH cteAllDates (TheDate)
		AS (
			SELECT @BeginRangeDate AS TheDate
			
			UNION ALL
			
			SELECT DATEADD(DAY, 1, TheDate)
			FROM cteAllDates
			WHERE TheDate < @EndRangeDate
			)
		INSERT INTO @AllDates (TheDate)
		SELECT ad.TheDate
		FROM cteAllDates ad
		OPTION (MAXRECURSION 5000);

		DECLARE @HolidayCounter TABLE (TheDate DATE, IsSchoolDay INT);

		WITH cteHolidays (HolidayDate)
		AS (
			SELECT da.[Date] 'HolidayDate'
			FROM (
				SELECT ca.StartDate, ca.EndDate
				FROM CV_Prod.dbo.AdCalendar ca
				INNER JOIN CV_Prod.dbo.SyCampusList cl ON ca.SyCampusGrpID = cl.SyCampusGrpID
					AND cl.SyCampusID = 9
					--WHERE ca.AdShiftID = 11		--Online Only 
				) AS dr
			INNER JOIN [master]..spt_values va ON va.number BETWEEN 0
					AND DATEDIFF(DAY, dr.StartDate, dr.EndDate)
				AND va.[type] = 'P'
			CROSS APPLY (
				SELECT DATEADD(DAY, va.number, dr.StartDate)
				) AS da([Date])
			)
		INSERT INTO @HolidayCounter (TheDate, IsSchoolDay)
		SELECT ad.TheDate, CASE 
				WHEN ho.HolidayDate IS NULL
					THEN 1
				ELSE 0
				END 'IsSchoolDay'
		FROM @AllDates ad
		LEFT JOIN cteHolidays ho ON ad.TheDate = ho.HolidayDate;

		WITH cteHolidayCounts (TheDate, HolidayCounter)
		AS (
			SELECT ad.TheDate, SUM(CASE 
						WHEN cd.IsSchoolDay = 1
							THEN 0
						ELSE 1
						END) 'HolidayCounter'
			FROM @HolidayCounter ad
			INNER JOIN @HolidayCounter cd ON ad.TheDate <= cd.TheDate
			GROUP BY ad.TheDate
				--ORDER BY 
				--	ad.TheDate 
			)
		INSERT INTO LS_ODS.LDACounts (TheDate, IsSchoolDay, HolidayCounter, LDACount, LDACountMinusHolidayCounter, LDACountMinusHolidayCounterAddHolidayDate)
		SELECT ad.TheDate, ho.IsSchoolDay, hc.HolidayCounter, DATEDIFF(DAY, ad.TheDate, @TodaysDate) 'LDACount', DATEDIFF(DAY, ad.TheDate, @TodaysDate) - hc.HolidayCounter 'LDACountMinusHolidayCounter', DATEDIFF(DAY, ad.TheDate, @TodaysDate) - hc.HolidayCounter + CASE 
				WHEN ho.IsSchoolDay = 0
					THEN 1
				ELSE 0
				END 'LDACountMinusHolidayCounterAddHolidayDate'
		FROM @AllDates ad
		INNER JOIN @HolidayCounter ho ON ad.TheDate = ho.TheDate
		INNER JOIN cteHolidayCounts hc ON ad.TheDate = hc.TheDate;

		DELETE
		FROM [COL-CVU-P-SQ01].FREEDOM.LMS_Data.LDACounts;--PROD 
			--DELETE FROM [MLK-CVU-D-SQ01].FREEDOM.LMS_Data.LDACounts;																				--DEV 

		INSERT INTO [COL-CVU-P-SQ01].FREEDOM.LMS_Data.LDACounts --PROD 
			--INSERT INTO [MLK-CVU-D-SQ01].FREEDOM.LMS_Data.LDACounts																				--DEV 
			(TheDate, IsSchoolDay, HolidayCounter, LDACount, LDACountMinusHolidayCounter, LDACountMinusHolidayCounterAddHolidayDate)
		SELECT lc.TheDate, lc.IsSchoolDay, lc.HolidayCounter, lc.LDACount, lc.LDACountMinusHolidayCounter, lc.LDACountMinusHolidayCounterAddHolidayDate
		FROM LS_ODS.LDACounts lc;

		EXEC LS_ODS.AddODSLoadLog 'LDA Counts Calculation Complete', 0;

		--**************************************************************************************************************************************** 
		--Update the tables needed for iDash to reduce high number of logical reads 
		--**************************************************************************************************************************************** 
		--CourseWeeklyGradedActivity 
		TRUNCATE TABLE LS_ODS.CourseWeeklyGradedActivity;

		INSERT INTO LS_ODS.CourseWeeklyGradedActivity (StudentId, EnrollSchedId, ClassSchedId, CoursePrimaryKey, AssignmentPrimaryKey, GradePrimaryKey, WeekNumber, Assignment, Grade, [Status], DateTaken, Week1Grade, Week2Grade, Week3Grade, Week4Grade, Week5Grade, Attempts, DateOfLastAttempt, PossiblePoints)
		SELECT st.SyStudentId 'StudentId', st.AdEnrollSchedId 'EnrollSchedId', st.AdClassSchedId 'ClassSchedId', co.CoursePrimaryKey 'CoursePrimaryKey', asg.AssignmentPrimaryKey 'AssignmentPrimaryKey', gr.GradePrimaryKey 'GradePrimaryKey', asg.WeekNumber 'WeekNumber', asg.AssignmentTitle 'Assignment', CASE 
				WHEN asg.PossiblePoints IS NOT NULL
					AND asg.PossiblePoints > 0
					THEN CASE 
							WHEN gr.ManualScore IS NULL
								OR gr.ManualScore = 0.00
								THEN ((gr.HighestScore / asg.PossiblePoints) * 100)
							ELSE ((gr.ManualScore / asg.PossiblePoints) * 100)
							END
				ELSE 0
				END 'Grade', gr.AssignmentStatus 'Status', CONVERT(VARCHAR(10), gr.HighestDateTimeCreated, 101) 'DateTaken', st.Week1Grade 'Week1Grade', st.Week2Grade 'Week2Grade', st.Week3Grade 'Week3Grade', st.Week4Grade 'Week4Grade', st.Week5Grade 'Week5Grade', gr.NumberOfAttempts 'Attempts', gr.HighestAttemptDateTime 'DateOfLastAttempt', COALESCE(gr.ManualGrade, gr.HighestGrade, '0.00') + '/' + CAST(asg.PossiblePoints AS VARCHAR(4)) 'PointsPossible'
		FROM LS_ODS.Assignments asg
		INNER JOIN LS_ODS.Courses co ON asg.CoursePrimaryKey = co.CoursePrimaryKey
		INNER JOIN LS_ODS.Students st ON co.AdClassSchedId = st.AdClassSchedId
		LEFT JOIN LS_ODS.Grades gr ON asg.AssignmentPrimaryKey = gr.AssignmentPrimaryKey
			AND st.CourseUsersPrimaryKey = gr.CourseUsersPrimaryKey
		WHERE st.AdEnrollSchedId IS NOT NULL;

		EXEC LS_ODS.AddODSLoadLog 'Processed Course Weekly Graded Activity', 0;

		--CourseWeeklyGrades 
		TRUNCATE TABLE LS_ODS.CourseWeeklyGrades;

		DECLARE @Instructors TABLE (AdClassSchedId INT, AcademicFacilitator VARCHAR(50), CoInstructor VARCHAR(50));

		INSERT INTO @Instructors (AdClassSchedId)
		SELECT DISTINCT ins.AdClassSchedId
		FROM iDash.Instructors_vw ins;

		WITH cteAcademicFacilitator (AdClassSchedId, AcademicFacilitator, RowNumber)
		AS (
			SELECT ins.AdClassSchedId, ins.InstructorName, ROW_NUMBER() OVER (
					PARTITION BY ins.AdClassSchedId ORDER BY ins.DisplayOrder
					) 'RowNumber'
			FROM iDash.Instructors_vw ins
			WHERE ins.InstructorTypeCode = 'SECONDARY'
			)
		UPDATE ins
		SET ins.AcademicFacilitator = af.AcademicFacilitator
		FROM @Instructors ins
		INNER JOIN cteAcademicFacilitator af ON ins.AdClassSchedId = af.AdClassSchedId
			AND af.RowNumber = 1;

		WITH cteCoInstructor (AdClassSchedId, CoInstructor, RowNumber)
		AS (
			SELECT ins.AdClassSchedId, ins.InstructorName, ROW_NUMBER() OVER (
					PARTITION BY ins.AdClassSchedId ORDER BY ins.DisplayOrder
					) 'RowNumber'
			FROM iDash.Instructors_vw ins
			WHERE ins.InstructorTypeCode = 'COINSTR'
			)
		UPDATE ins
		SET ins.CoInstructor = ci.CoInstructor
		FROM @Instructors ins
		INNER JOIN cteCoInstructor ci ON ins.AdClassSchedId = ci.AdClassSchedId
			AND ci.RowNumber = 1;

		INSERT INTO LS_ODS.CourseWeeklyGrades (StudentId, EnrollSchedId, AdClassSchedID, Week1Dates, Week2Dates, Week3Dates, Week4Dates, Week5Dates, Week1Grade, Week2Grade, Week3Grade, Week4Grade, Week5Grade, Week1SubRate, Week2SubRate, Week3SubRate, Week4SubRate, Week5SubRate, CurrentNumericGrade, ClassTime, SelfTestCount, AssessmentCount, AssignmentCount, DiscussionCount, ActivityCount, CurrentCourseLetterGrade, CourseSubmissionRate, AcademicFacilitator, CoInstructor)
		SELECT st.SyStudentId 'StudentId', st.AdEnrollSchedId 'EnrollSchedId', st.AdClassSchedId 'AdClassSchedID', CONVERT(VARCHAR(5), co.Week1StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week1StartDate), 101) 'Week1Dates', CONVERT(VARCHAR(5), co.Week2StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week2StartDate), 101) 'Week2Dates', CONVERT(VARCHAR(5), co.Week3StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week3StartDate), 101) 'Week3Dates', CONVERT(VARCHAR(5), co.Week4StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week4StartDate), 101) 'Week4Dates', CONVERT(VARCHAR(5), co.Week5StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week5StartDate), 101) 'Week5Dates', st.Week1Grade * 100 'Week1Grade', st.Week2Grade * 100 'Week2Grade', st.Week3Grade * 100 'Week3Grade', st.Week4Grade * 100 'Week4Grade', st.Week5Grade * 100 'Week5Grade', st.Week1CompletionRate * 100 'Week1SubRate', st.Week2CompletionRate * 100 'Week2SubRate', st.Week3CompletionRate * 100 'Week3SubRate', st.Week4CompletionRate * 100 
			'Week4SubRate', st.Week5CompletionRate * 100 'Week5SubRate', st.Week5Grade * 100 'CurrentNumericGrade', st.TimeInClass 'ClassTime', st.SelfTestsCount 'SelfTestCount', st.AssessmentsCount 'AssessmentCount', st.AssignmentsCount 'AssignmentCount', st.DiscussionsCount 'DiscussionCount', st.ActivitiesCount 'ActivityCount', CASE 
				WHEN (st.Week5Grade * 100) >= 90
					THEN 'A'
				WHEN (st.Week5Grade * 100) >= 80
					THEN 'B'
				WHEN (st.Week5Grade * 100) >= 70
					THEN 'C'
				WHEN (st.Week5Grade * 100) >= 60
					THEN 'D'
				WHEN (st.Week5Grade * 100) < 60
					THEN 'F'
				END 'CurrentCourseLetterGrade', st.CoursePercentage * 100 'CourseSubmissionRate', ins.AcademicFacilitator 'AcademicFacilitator', ins.CoInstructor 'CoInstructor'
		FROM LS_ODS.Students st
		LEFT JOIN LS_ODS.Courses co ON st.AdClassSchedId = co.AdClassSchedId
		LEFT JOIN @Instructors ins ON co.AdClassSchedId = ins.AdClassSchedId
		WHERE st.AdEnrollSchedId IS NOT NULL;

		EXEC LS_ODS.AddODSLoadLog 'Processed Course Weekly Grades', 0;

		--Wait a short time to ensure the data is all written before report generation starts 
		WAITFOR DELAY '00:01';

		/*Send ODS email after steo number 54 */
		EXECUTE LS_ODS.ODS_Email_2

		--**************************************************************************************************************************************** 
		--Process the ActiveSubmissionSummary table 
		--**************************************************************************************************************************************** 
		EXEC LS_ODS.ProcessActiveSubmissionSummary;

		EXEC LS_ODS.AddODSLoadLog 'Active Submission Summary Procesing Complete', 0;

		--**************************************************************************************************************************************** 
		--Process the Total Course Points Earned table 
		--**************************************************************************************************************************************** 
		EXEC LS_ODS.UpsertTotalCoursePointsEarned;

		EXEC LS_ODS.AddODSLoadLog 'Total Course Points Earned Procesing Complete', 0;

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

		EXEC LS_ODS.AddODSLoadLog 'Program Certification Tables Update Complete', 0;

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
		EXEC [MLK-REP-P-SQ02].msdb.dbo.sp_start_job N'E5401A80-B99C-4840-83DE-57DDFDCD6C81';--2016 Server

		--Execute VA Report - New Policy ssrs report 
		--EXEC [MLK-SSR-P-SQ01].msdb.dbo.sp_start_job N'B640B3D8-41EA-45C7-A605-6490D8643B0A';  --2008 Server
		EXEC [MLK-REP-P-SQ02].msdb.dbo.sp_start_job N'F98F0617-E4F1-4F1F-A384-B6EE78BA9EF5';--2016 Server

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

		SET @countofrecords = (
				SELECT COUNT(*)
				FROM Stage.ODS_Duplicates
				WHERE PROCCESED_ON = CONVERT(DATE, GETDATE())
				)

		IF @countofrecords > 0
		BEGIN
			DECLARE @tableHTML NVARCHAR(MAX) = N'';

			SELECT @tableHTML += N'<tr><td>' + CAST(PrimaryKey AS NVARCHAR(10)) + N'</td><td>' + STEP_FAILED_ON + N'</td></tr>'
			FROM Stage.ODS_Duplicates
			WHERE PROCCESED_ON = CONVERT(DATE, GETDATE());

			--SET @tableHTML = N'<html><body><p>Dear Team ,</p>';
			--SET @tableHTML+=N'<html><body><p>Please review the duplicates found in todays ODS process ,</p>'
			SET @tableHTML = N'<table border="1"><tr><th>ID</th><th>Name</th></tr>' + @tableHTML + N'</table>';

			EXEC msdb.dbo.sp_send_dbmail @profile_name = 'EDM_DB_ALERT', @recipients = 'ppoonati@ultimatemedical.edu', @subject = 'Duplicate records found in todays ODS Run ', @body = @tableHTML, @body_format = 'HTML';
		END
	END TRY

	--		 --**************************************************************************************************************************************** 
	--	--Catch block, send email incase of ODS failure
	--	--**************************************************************************************************************************************** 
	BEGIN CATCH
		DROP TABLE

		IF EXISTS #tempmail
			CREATE TABLE #tempmail (EventDetails VARCHAR(240), EventDateTime DATETIME);

		DECLARE @emailsubject NVARCHAR(240);
		DECLARE @html_body NVARCHAR(MAX);
		DECLARE @ERRORBODY NVARCHAR(MAX) = 'Error message: ' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR) + CHAR(13) + CHAR(10) + 'Error severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR) + CHAR(13) + CHAR(10) + 'Error state: ' + CAST(ERROR_STATE() AS NVARCHAR) + CHAR(13) + CHAR(10) + 'Error procedure: ' + COALESCE(ERROR_PROCEDURE(), 'N/A') + CHAR(13) + CHAR(10) + 'Error line number: ' + CAST(ERROR_LINE() AS NVARCHAR) + CHAR(13) + CHAR(10);

		SET @html_body = N'<html><body><p>Dear Team ,</p>';
		SET @html_body += N'<html><body><p>ODS failed due to below error ,</p>'
		SET @html_body += @ERRORBODY
		SET @html_body += N'<p>Here are the steps that have been processed today:</p>';
		SET @emailsubject = 'ODS Process failure-' + CONVERT(VARCHAR(50), DATEADD(DAY, 0, DATEDIFF(DAY, 0, GETDATE())), 101);

		-- Execute the SQL statement and insert the results as a table in the HTML body
		DECLARE @table_html NVARCHAR(MAX);

		SET @table_html = N'<table><thead><tr><th>EventDetails</th><th>EventDateTime</th></tr></thead><tbody>';

		INSERT INTO #tempmail (EventDetails, EventDateTime)
		SELECT EventDetails, EventDateTime
		FROM LS_ODS.ODSLoadLog
		WHERE CONVERT(DATE, EventDateTime) = CONVERT(DATE, GETDATE());

		SELECT @table_html += N'<tr><td>' + EventDetails + N'</td><td>' + CONVERT(VARCHAR, EventDateTime) + N'</td></tr>'
		FROM #tempmail;

		SET @table_html += N'</tbody></table>';
		-- Add the table to the HTML body and close the HTML tags
		SET @html_body += @table_html + N'<p>Best regards,</p><p>EDM TEAM </p></body></html>';

		-- Send the email
		EXEC msdb.dbo.sp_send_dbmail @profile_name = 'EDM_DB_ALERT', @recipients = 'edmteam@ultimatemedical.edu', @subject = @emailsubject, @body = @html_body, @body_format = 'HTML';

		DECLARE @errorMessage VARCHAR(4000)
		DECLARE @procName VARCHAR(255)

		SELECT @errorMessage = error_message()

		SELECT @procName = OBJECT_NAME(@@PROCID)

		SELECT @procName

		RAISERROR ('%sODS failed due to %s', 16, 1, @procName, @errorMessage)
	END CATCH
END;
    ha.DATE_ADDED AS HighestDateTimeCreated, 
    ha.DATE_MODIFIED AS HighestDateTimeModified, 
    CASE WHEN gg.HIGHEST_ATTEMPT_PK1 = gg.LAST_ATTEMPT_PK1 THEN 1 ELSE 0 END AS HighestIsLatestAttemptIndicator, 
    fa.SCORE AS FirstScore, 
    fa.GRADE AS FirstGrade, 
    fa.ATTEMPT_DATE AS FirstAttemptDateTime, 
    CASE WHEN gg.FIRST_ATTEMPT_PK1 = gg.LAST_ATTEMPT_PK1 THEN 1 ELSE 0 END AS FirstIsLatestAttemptIndicator, 
    fa.DATE_ADDED AS FirstDateTimeCreated, 
    fa.DATE_MODIFIED AS FirstDateTimeModified, 
    gei.AssignmentPK1 AS AssignmentPrimaryKey, 
    CASE 
        WHEN gei.GradeAttemptStatus IS NULL AND gei.GradeAttemptDate IS NULL THEN 'NOT COMPLETE'
        ELSE gs.[Description]
    END AS AssignmentStatus, 
    gei.SourceSystem
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
    dbo.DATA_SOURCE ds ON ds.PK1 = cu.DATA_SRC_PK1
WHERE 
    LEFT(gei.UserEPK, 9) = 'SyStudent' -- Only Students 
    AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8 -- Filter Out Test/Bad Students 
    AND LEFT(gei.CourseEPK, 8) = 'AdCourse' -- Only Courses 
    AND (
        gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' -- Filter Out Test/Bad Courses 
        OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' -- To bring in CLW courses 
        OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%' -- Captures EMT Courses
        OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%'
    ) -- Captures EMT Courses based out of CLW
    AND gei.UserFirstName NOT LIKE 'BBAFL%' -- More Test Students
    AND gei.UserEPK NOT LIKE '%PART[1-5]%' -- More Test Students
    AND gei.GradePK1 IS NOT NULL -- Filter Out All Grade Placeholders 
    AND ds.batch_uid NOT IN ('ENR_181008_02.txt', 'ENR_181008', 'ENR_181008_1558036.txt'); -- Exclude specified batch_uid values
--Adding to deal with erroneous DSKs added
			--in the SIS Framework cleanup effort
			--IEHR Assignments With No Primary Key (SCORM)

		DECLARE @StartingValue INT;

		SET @StartingValue = COALESCE((
					SELECT MIN(gr.GradePrimaryKey)
					FROM LS_ODS.Grades gr
					WHERE gr.GradePrimaryKey BETWEEN - 514999999
							AND - 514000000
					), - 514000000);

		INSERT INTO stage.Grades (GradePrimaryKey, CourseUsersPrimaryKey, RowStatus, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, ManualDateTime, ExemptIndicator, HighestDateTimeCreated, HighestDateTimeModified, HighestIsLatestAttemptIndicator, FirstScore, FirstGrade, FirstAttemptDateTime, FirstIsLatestAttemptIndicator, FirstDateTimeCreated, FirstDateTimeModified, AssignmentPrimaryKey, AssignmentStatus, SourceSystem)
		SELECT @StartingValue - ROW_NUMBER() OVER (
				ORDER BY gei.UserPK1, gei.CoursePK1, gei.AssignmentPK1
				) 'GradePrimaryKey', cu.PK1 'CourseUsersPrimaryKey', bs.[Description] 'RowStatus', gei.GradeDisplayScore 'HighestScore', gei.GradeDisplayGrade 'HighestGrade', gei.GradeAttemptDate 'HighestAttemptDateTime', gei.GradeManualScore 'ManualScore', gei.GradeManualGrade 'ManualGrade', gei.GradeOverrideDate 'ManualDateTime', gei.GradeExemptIndicator 'ExemptIndicator', ha.DATE_ADDED 'HighestDateTimeCreated', ha.DATE_MODIFIED 'HighestDateTimeModified', CASE 
				WHEN gg.HIGHEST_ATTEMPT_PK1 = gg.LAST_ATTEMPT_PK1
					THEN 1
				ELSE 0
				END 'HighestIsLatestAttemptIndicator', fa.SCORE 'FirstScore', fa.GRADE 'FirstGrade', fa.ATTEMPT_DATE 'FirstAttemptDateTime', CASE 
				WHEN gg.FIRST_ATTEMPT_PK1 = gg.LAST_ATTEMPT_PK1
					THEN 1
				ELSE 0
				END 'FirstIsLatestAttemptIndicator', fa.DATE_ADDED 'FirstDateTimeCreated', fa.DATE_MODIFIED 'FirstDateTimeModified', gei.AssignmentPK1 'AssignmentPrimaryKey', CASE 
				WHEN gei.GradeAttemptStatus IS NULL
					AND gei.GradeAttemptDate IS NULL
					THEN 'NOT COMPLETE'
				ELSE gs.[Description]
				END 'AssignmentStatus', gei.SourceSystem
		FROM stage.GradeExtractImport gei
		LEFT JOIN COURSE_USERS cu ON gei.UserPK1 = cu.USERS_PK1
			AND gei.CoursePK1 = cu.CRSMAIN_PK1
		LEFT JOIN GRADEBOOK_GRADE gg ON gei.GradePK1 = gg.PK1
		LEFT JOIN stage.BlackboardStatuses bs ON gg.[STATUS] = bs.PrimaryKey
			AND bs.[Type] = 'Row'
		LEFT JOIN ATTEMPT ha ON gg.HIGHEST_ATTEMPT_PK1 = ha.PK1
		LEFT JOIN ATTEMPT fa ON gg.FIRST_ATTEMPT_PK1 = fa.PK1
		LEFT JOIN stage.BlackboardStatuses gs ON gei.GradeAttemptStatus = gs.PrimaryKey
			AND gs.[Type] = 'Grade'
		LEFT JOIN dbo.DATA_SOURCE ds --Adding to deal with erroneous DSKs added
			ON ds.PK1 = cu.DATA_SRC_PK1 --in the SIS Framework cleanup effort
		WHERE LEFT(gei.UserEPK, 9) = 'SyStudent' --Only Students 
			AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8 --Filter Out Test/Bad Students 
			AND LEFT(gei.CourseEPK, 8) = 'AdCourse' --Only Courses 
			AND (
				gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --Filter Out Test/Bad Courses 
				OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --To bring in CLW courses 
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%' --2/28/2024 CML: Captures EMT Courses
				OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%'
				) --2/28/2024 CML: Captures EMT Courses based out of CLW
			AND gei.UserFirstName NOT LIKE 'BBAFL%' --More Test Students
			AND gei.UserEPK NOT LIKE '%PART1%' --More Test Students
			AND gei.UserEPK NOT LIKE '%PART2%' --More Test Students
			AND gei.UserEPK NOT LIKE '%PART3%' --More Test Students
			AND gei.UserEPK NOT LIKE '%PART4%' --More Test Students
			AND gei.USEREPK NOT LIKE '%PART5%' --More Test Students
			AND gei.AssignmentDisplayColumnName LIKE '%IEHR%' --IEHR Only
			AND gei.GradePK1 IS NULL --SCORM IEHR Only
			AND gei.GradeManualGrade IS NOT NULL --Student Has Completed The Assignment
			AND ds.batch_uid NOT IN (
				'ENR_181008_02.txt'
				,'ENR_181008'
				,'ENR_181008_1558036.txt'
				);--Adding to deal with erroneous DSKs added
			--in the SIS Framework cleanup effort

		EXEC LS_ODS.AddODSLoadLog 'Loaded Grades Working Table', 0;

		--**************************************************************************************************************************************** 
		--Update the IEHR Assignment statuses in the stage.Grades table 
		--**************************************************************************************************************************************** 
		UPDATE g
		SET g.AssignmentStatus = 'COMPLETED'
		FROM stage.Grades g
		INNER JOIN stage.Assignments a ON g.AssignmentPrimaryKey = a.AssignmentPrimaryKey
			--AND a.AssignmentTitle = 'IEHR Assign' 
			AND g.HighestScore IS NOT NULL
			AND g.HighestScore <> 0;

		EXEC LS_ODS.AddODSLoadLog 'Updated IEHR Assignment Statuses', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with calculated values 
		--**************************************************************************************************************************************** 
		WITH cteLastLogins (SyStudentId, LastLoginDateTime)
		AS (
			SELECT jq.SyStudentId, MAX(jq.LastLoginDateTime) 'LastLoginDateTime'
			FROM (
				--SELECT 
				--	sal.SyStudentID 'SyStudentId', 
				--	MAX(sal.EventTime) 'LastLoginDateTime' 
				--FROM RTSATWeb.dbo.StudentActivityLog sal WITH(NOLOCK) 
				--WHERE EventId = 1 
				--GROUP BY 
				--	sal.SyStudentID 
				--UNION ALL 
				SELECT us.SyStudentId 'SyStudentId', MAX(lo.LoginDateTime) 'LastLoginDateTime'
				FROM RTSAT.[Login] lo
				INNER JOIN RTSAT.[User] us ON lo.UserPK = us.UserPK
				GROUP BY us.SyStudentId
				) AS jq
			GROUP BY jq.SyStudentId
			)
		UPDATE s
		SET s.LastLoginDateTime = ll.LastLoginDateTime
		FROM stage.Students s
		INNER JOIN cteLastLogins ll ON s.SyStudentId = ll.SyStudentId;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Last Logins', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the time in class 
		--**************************************************************************************************************************************** 
		DECLARE @FilterDate DATE;

		SET @FilterDate = DATEADD(DAY, - 90, GETDATE());

		--Check for temp table and delete if it exists 
		IF OBJECT_ID('tempdb..#TimeInClassTemp') IS NOT NULL
			DROP TABLE #TimeInClassTemp;

		CREATE TABLE #TimeInClassTemp (USER_PK1 INT, COURSE_PK1 INT, [DayOfWeek] INT, TimeInClass NUMERIC(12, 2));

		WITH cteTimeInClass (USER_PK1, COURSE_PK1, [DayOfWeek], TIME_IN_CLASS)
		AS (
			SELECT iq.USER_PK1, iq.COURSE_PK1, iq.[DayOfWeek], CAST(SUM(DATEDIFF(ss, iq.SESSION_START, iq.SESSION_END)) AS NUMERIC(36, 12)) / CAST(3600 AS NUMERIC(36, 12)) 'TIME_IN_CLASS'
			FROM (
				SELECT aa.USER_PK1, aa.COURSE_PK1, DATEPART(WEEKDAY, aa.[TIMESTAMP]) 'DayOfWeek', aa.SESSION_ID, MIN(aa.[TIMESTAMP]) SESSION_START, MAX(aa.[TIMESTAMP]) SESSION_END
				FROM ACTIVITY_ACCUMULATOR aa
				WHERE aa.COURSE_PK1 IS NOT NULL
					AND aa.USER_PK1 IS NOT NULL
					AND aa.[TIMESTAMP] >= @FilterDate
				GROUP BY aa.USER_PK1, aa.COURSE_PK1, aa.SESSION_ID, DATEPART(WEEKDAY, aa.[TIMESTAMP])
				) iq
			GROUP BY iq.USER_PK1, iq.COURSE_PK1, iq.[DayOfWeek]
			)
		INSERT INTO #TimeInClassTemp (USER_PK1, COURSE_PK1, [DayOfWeek], TimeInClass)
		SELECT tic.USER_PK1, tic.COURSE_PK1, tic.[DayOfWeek], tic.TIME_IN_CLASS
		FROM cteTimeInClass tic;

		WITH cteTotal
		AS (
			SELECT tic.USER_PK1, tic.COURSE_PK1, SUM(tic.TimeInClass) 'TotalTimeInClass'
			FROM #TimeInClassTemp tic
			GROUP BY tic.USER_PK1, tic.COURSE_PK1
			)
		UPDATE s
		SET s.TimeInClass = tic.TotalTimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN cteTotal tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1;

		UPDATE s
		SET s.MondayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 2;

		UPDATE s
		SET s.TuesdayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 3;

		UPDATE s
		SET s.WednesdayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 4;

		UPDATE s
		SET s.ThursdayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 5;

		UPDATE s
		SET s.FridayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 6;

		UPDATE s
		SET s.SaturdayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 7;

		UPDATE s
		SET s.SundayTimeInClass = tic.TimeInClass
		FROM stage.Students s
		INNER JOIN COURSE_USERS cu ON s.CourseUsersPrimaryKey = cu.PK1
		INNER JOIN #TimeInClassTemp tic ON cu.USERS_PK1 = tic.USER_PK1
			AND cu.CRSMAIN_PK1 = tic.COURSE_PK1
			AND tic.[DayOfWeek] = 1;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Times In Class', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with I3 interaction information 
		--**************************************************************************************************************************************** 
		--Define needed variables 
		DECLARE @I3CurrentDateTime DATETIME;
		DECLARE @LastUpdatedDateTime DATETIME;
		DECLARE @RemoteQuery NVARCHAR(4000);

		--Populate needed variables 
		SET @I3CurrentDateTime = GETDATE();

		--Create table to hold new/updated calls 
		DECLARE @Calls TABLE (PhoneNumber VARCHAR(50) NOT NULL PRIMARY KEY, LastInteractionDateTime DATETIME, SourceSystem VARCHAR(50) NULL);
		DECLARE @CallsBTB TABLE (PhoneNumber VARCHAR(50) NOT NULL PRIMARY KEY, LastInteractionDateTime DATETIME);
		DECLARE @CallsMCS TABLE (PhoneNumber VARCHAR(50) NOT NULL PRIMARY KEY, LastInteractionDateTime DATETIME);
		DECLARE @CallsCombined TABLE (PhoneNumber VARCHAR(50) NOT NULL, LastInteractionDateTime DATETIME, SourceSystem VARCHAR(50) NOT NULL, PRIMARY KEY (PhoneNumber, SourceSystem));

		--Get the most recent time the I3 Interactions was updated 
		SET @LastUpdatedDateTime = (
				SELECT MAX(i3.LastUpdatedDateTime)
				FROM LS_ODS.I3Interactions i3
				);

		--Back to Basics (BTB) Interaction Data
		INSERT INTO @CallsBTB (PhoneNumber, LastInteractionDateTime)
		SELECT REPLACE(btbcalldetail.RemoteNumber, '+', '') 'PhoneNumber', MAX(btbcalldetail.InitiatedDate) 'LastInterationDateTime'
		--FROM [MLK-TEL-D-SQ03].I3_IC_TEST.dbo.CallDetail_viw btbcalldetail															--UAT 
		--FROM [MLK-TEL-D-SQ03].I3_IC_DEV.dbo.CallDetail_viw btbcalldetail															--DEV 
		FROM [COL-TEL-P-SQ01].I3_IC_PROD.dbo.CallDetail_viw btbcalldetail --PROD  
		WHERE btbcalldetail.CallType = 'External'
			AND RTRIM(LTRIM(btbcalldetail.RemoteNumber)) <> ''
			AND btbcalldetail.CallDurationSeconds >= 90
			AND LEN(REPLACE(btbcalldetail.RemoteNumber, '+', '')) = 10
			AND ISNUMERIC(REPLACE(btbcalldetail.RemoteNumber, '+', '')) = 1
			AND btbcalldetail.InitiatedDate >= @LastUpdatedDateTime
		GROUP BY REPLACE(btbcalldetail.RemoteNumber, '+', '');

		--MCS Interaction Data
		SELECT @RemoteQuery = '
                           SELECT MAX(DATEADD(SECOND, I.StartDTOffset, I.InitiatedDateTimeUTC)) AS LastInterationDateTime
                                        ,CASE WHEN LEN(REPLACE(I.RemoteID, ''+'', '''')) = 0 OR REPLACE(I.RemoteID, ''+'', '''') IS NULL THEN ''-'' ELSE I.RemoteID END as RemoteNumber                                      
                           FROM    MCS_I3_IC.dbo.InteractionSummary I
                           where DATEADD(SECOND, I.StartDTOffset, I.InitiatedDateTimeUTC) > ''' + CAST(@LastUpdatedDateTime AS NVARCHAR(25)) + '''
                           AND ConnectedDateTimeUTC > ''' + CAST(@LastUpdatedDateTime AS NVARCHAR(25)) + '''
                           AND TerminatedDateTimeUTC > ''' + CAST(@LastUpdatedDateTime AS NVARCHAR(25)) + 
			'''
                           AND I.ConnectionType = 1
                           AND LEN(I.RemoteID) > 0
                           AND LEN(REPLACE(I.RemoteID, ''+'', '''')) = 10 
                           AND ISNUMERIC(REPLACE(I.RemoteID, ''+'', '''')) = 1 
                           AND CAST(ROUND(DATEDIFF(MILLISECOND, ConnectedDateTimeUTC, TerminatedDateTimeUTC) / 1000.000,0) AS BIGINT) > 90
                           and DATEDIFF(DAY, ConnectedDateTimeUTC, TerminatedDateTimeUTC)  < 23
                           GROUP BY I.RemoteID 
                           '

		INSERT INTO @CallsMCS (LastInteractionDateTime, PhoneNumber)
		EXEC [COL-MCS-P-SQ02].master.dbo.sp_executesql @Remotequery;

		--Add the new/updated calls into the table variable 
		INSERT INTO @CallsCombined (PhoneNumber, LastInteractionDateTime, SourceSystem)
		SELECT PhoneNumber, LastInteractionDateTime, 'BTB'
		FROM @CallsBTB
		
		UNION ALL
		
		SELECT PhoneNumber, LastInteractionDateTime, 'MCS'
		FROM @CallsMCS;

		--Add the new/updated calls into the table variable 
		WITH cteCalls
		AS (
			SELECT cc.PhoneNumber 'PhoneNumber', MAX(cc.LastInteractionDateTime) 'LastInterationDateTime'
			FROM @CallsCombined cc
			GROUP BY cc.PhoneNumber
			)
		INSERT INTO @Calls (PhoneNumber, LastInteractionDateTime, SourceSystem)
		SELECT cc.PhoneNumber, cc.LastInteractionDateTime, cc.SourceSystem
		FROM @CallsCombined cc
		INNER JOIN cteCalls ca ON cc.PhoneNumber = ca.PhoneNumber
			AND cc.LastInteractionDateTime = ca.LastInterationDateTime;

		--Update the phone numbers that have a new interaction date/time 
		UPDATE i3
		SET i3.LastInteractionDateTime = c.LastInteractionDateTime, i3.SourceSystem = c.SourceSystem, i3.LastUpdatedDateTime = @I3CurrentDateTime
		FROM LS_ODS.I3Interactions i3
		INNER JOIN @Calls c ON i3.PhoneNumber = c.PhoneNumber;

		--Add new phone numbers that don't exist in the interactions table 
		INSERT INTO LS_ODS.I3Interactions (
			PhoneNumber, LastInteractionDateTime, SourceSystem,
			--added the SourceSystem column to the table to track source of the interaction data
			LastUpdatedDateTime
			)
		SELECT c.PhoneNumber, c.LastInteractionDateTime, c.SourceSystem,
			--added the SourceSystem column to the table to track source of the interaction data
			@I3CurrentDateTime
		FROM @Calls c
		WHERE c.PhoneNumber NOT IN (
				SELECT i3.PhoneNumber
				FROM LS_ODS.I3Interactions i3
				);

		UPDATE s
		SET s.LastI3InteractionNumberMainPhone = mpi.PhoneNumber, s.LastI3InteractionDateTimeMainPhone = mpi.LastInteractionDateTime, s.DaysSinceLastI3InteractionMainPhone = DATEDIFF(DAY, mpi.LastInteractionDateTime, @CurrentDateTime), s.LastI3InteractionNumberWorkPhone = wpi.PhoneNumber, s.LastI3InteractionDateTimeWorkPhone = wpi.LastInteractionDateTime, s.DaysSinceLastI3InteractionWorkPhone = DATEDIFF(DAY, wpi.LastInteractionDateTime, @CurrentDateTime), s.LastI3InteractionNumberMobilePhone = mopi.PhoneNumber, s.LastI3InteractionDateTimeMobilePhone = mopi.LastInteractionDateTime, s.DaysSinceLastI3InteractionMobilePhone = DATEDIFF(DAY, mopi.LastInteractionDateTime, @CurrentDateTime), s.LastI3InteractionNumberOtherPhone = opi.PhoneNumber, s.LastI3InteractionDateTimeOtherPhone = opi.LastInteractionDateTime, s.DaysSinceLastI3InteractionOtherPhone = DATEDIFF(DAY, opi.LastInteractionDateTime, @CurrentDateTime)
		FROM stage.Students s
		INNER JOIN CV_Prod.dbo.SyStudent cvs ON s.SyStudentID = cvs.SyStudentId
		LEFT JOIN LS_ODS.I3Interactions mpi ON RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cvs.Phone, '-', ''), '*', ''), '(', ''), ')', ''), ' ', ''))) = mpi.PhoneNumber
		LEFT JOIN LS_ODS.I3Interactions wpi ON RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cvs.WorkPhone, '-', ''), '*', ''), '(', ''), ')', ''), ' ', ''))) = wpi.PhoneNumber
		LEFT JOIN LS_ODS.I3Interactions mopi ON RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cvs.MobileNumber, '-', ''), '*', ''), '(', ''), ')', ''), ' ', ''))) = mopi.PhoneNumber
		LEFT JOIN LS_ODS.I3Interactions opi ON RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cvs.OtherPhone, '-', ''), '*', ''), '(', ''), ')', ''), ' ', ''))) = opi.PhoneNumber;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Last I3 Interactions', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the weekly grades 
		--NEED TO PERFORM ANALYSIS TO ACCOUNT FOR 16 WEEK EMT PROGRAM -cml 2/28/2024
		--stage.Courses only has columns for weeks 1 - 5 -cml 2/28/2024
		--EMT has a week 6 grade and a Final Percentage only, only Final Percentage will be placed in week 5 based on current logic -cml 2/28/2024
		--**************************************************************************************************************************************** 
		WITH cteWeeklyGrades (StudentPrimaryKey, CoursePrimaryKey, WeekNumber, WeeklyGrade)
		AS (
			SELECT gei.UserPK1 'StudentPrimaryKey', gei.CoursePK1 'CoursePrimaryKey', CASE 
					WHEN gei.AssignmentDisplayTitle IN (
							'Week 1 Grade %'
							,'Week 1 Grade (%)'
							)
						THEN 1
					WHEN gei.AssignmentDisplayTitle IN (
							'Week 2 Grade %'
							,'Week 2 Grade (%)'
							)
						THEN 2
					WHEN gei.AssignmentDisplayTitle IN (
							'Week 3 Grade %'
							,'Week 3 Grade (%)'
							)
						THEN 3
					WHEN gei.AssignmentDisplayTitle IN (
							'Week 4 Grade %'
							,'Week 4 Grade (%)'
							)
						THEN 4
					ELSE 5
					END 'WeekNumber', (CAST(gei.GradeManualScore AS NUMERIC(12, 2)) / CAST(REPLACE(gei.AssignmentPointsPossible, '"', '') AS NUMERIC(12, 2))) 'WeeklyGrade'
			FROM stage.GradeExtractImport gei
			WHERE gei.AssignmentDisplayTitle IN (
					'Week 1 Grade %'
					,'Week 2 Grade %'
					,'Week 3 Grade %'
					,'Week 4 Grade %'
					,'Week 1 Grade (%)'
					,'Week 2 Grade (%)'
					,'Week 3 Grade (%)'
					,'Week 4 Grade (%)'
					,'Final Percentage'
					)
				AND CAST(REPLACE(gei.AssignmentPointsPossible, '"', '') AS NUMERIC(12, 2)) <> 0
			)
		UPDATE s
		SET s.Week1Grade = w1.WeeklyGrade, s.Week2Grade = w2.WeeklyGrade, s.Week3Grade = w3.WeeklyGrade, s.Week4Grade = w4.WeeklyGrade, s.Week5Grade = w5.WeeklyGrade
		FROM stage.Students s
		INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
		LEFT JOIN cteWeeklyGrades w1 ON s.StudentPrimaryKey = w1.StudentPrimaryKey
			AND c.CoursePrimaryKey = w1.CoursePrimaryKey
			AND w1.WeekNumber = 1
		LEFT JOIN cteWeeklyGrades w2 ON s.StudentPrimaryKey = w2.StudentPrimaryKey
			AND c.CoursePrimaryKey = w2.CoursePrimaryKey
			AND w2.WeekNumber = 2
		LEFT JOIN cteWeeklyGrades w3 ON s.StudentPrimaryKey = w3.StudentPrimaryKey
			AND c.CoursePrimaryKey = w3.CoursePrimaryKey
			AND w3.WeekNumber = 3
		LEFT JOIN cteWeeklyGrades w4 ON s.StudentPrimaryKey = w4.StudentPrimaryKey
			AND c.CoursePrimaryKey = w4.CoursePrimaryKey
			AND w4.WeekNumber = 4
		LEFT JOIN cteWeeklyGrades w5 ON s.StudentPrimaryKey = w5.StudentPrimaryKey
			AND c.CoursePrimaryKey = w5.CoursePrimaryKey
			AND w5.WeekNumber = 5;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Weekly Grades', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the count of practice exercises, tests, and assignments 
		--**************************************************************************************************************************************** 
		WITH cteCounts (CoursePrimaryKey, StudentPrimaryKey, PracticeExercisesCount, TestsCount, AssignmentsCount)
		AS (
			SELECT cm.PK1 'CoursePrimaryKey', cu.USERS_PK1 'StudentPrimaryKey', SUM(CASE 
						WHEN gm.TITLE LIKE '%Practice Exercise%'
							THEN 1
						ELSE 0
						END) 'PracticeExercisesCount', SUM(CASE 
						WHEN gm.TITLE LIKE '%Test%'
							THEN 1
						ELSE 0
						END) 'TestsCount', SUM(CASE 
						WHEN gm.TITLE LIKE '%Assignment%'
							THEN 1
						ELSE 0
						END) 'AssignmentsCount'
			FROM dbo.ATTEMPT a
			INNER JOIN GRADEBOOK_GRADE gg ON a.GRADEBOOK_GRADE_PK1 = gg.PK1
			INNER JOIN GRADEBOOK_MAIN gm ON gg.GRADEBOOK_MAIN_PK1 = gm.PK1
				AND gm.PK1 NOT IN (
					SELECT PK1
					FROM GRADEBOOK_MAIN
					WHERE TITLE LIKE '%IEHR%'
						AND COURSE_CONTENTS_PK1 IS NULL
					)
			INNER JOIN COURSE_USERS cu ON gg.COURSE_USERS_PK1 = cu.PK1
			INNER JOIN COURSE_MAIN cm ON cu.CRSMAIN_PK1 = cm.PK1
			GROUP BY cm.PK1, cu.USERS_PK1
			)
		UPDATE s
		SET s.SelfTestsCount = co.PracticeExercisesCount, s.AssessmentsCount = co.TestsCount, s.AssignmentsCount = co.AssignmentsCount
		FROM stage.Students s
		INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
		INNER JOIN cteCounts co ON s.StudentPrimaryKey = co.StudentPrimaryKey
			AND c.CoursePrimaryKey = co.CoursePrimaryKey;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Counts Of Practice Exercises, Tests And Assignments', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the count of discussion posts 
		--**************************************************************************************************************************************** 
		WITH cteCounts (CoursePrimaryKey, StudentPrimaryKey, DiscussionsCount)
		AS (
			SELECT cm.CRSMAIN_PK1 'CoursePrimaryKey', mm.USERS_PK1 'StudentPrimaryKey', COUNT(mm.PK1) 'DiscussionsCount'
			FROM MSG_MAIN mm
			INNER JOIN FORUM_MAIN fm ON mm.FORUMMAIN_PK1 = fm.PK1
			INNER JOIN CONFERENCE_MAIN cm ON fm.CONFMAIN_PK1 = cm.PK1
			GROUP BY cm.CRSMAIN_PK1, mm.USERS_PK1
			)
		UPDATE s
		SET s.DiscussionsCount = co.DiscussionsCount
		FROM stage.Students s
		INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
		INNER JOIN cteCounts co ON s.StudentPrimaryKey = co.StudentPrimaryKey
			AND c.CoursePrimaryKey = co.CoursePrimaryKey;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Counts Of Discussion Posts', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Courses table with section start date, section end date and the course week number 
		--**************************************************************************************************************************************** 
		UPDATE c
		SET c.SectionStart = cs.StartDate, c.SectionEnd = cs.EndDate, c.WeekNumber = CASE 
				WHEN DATEDIFF(WEEK, cs.StartDate, @CurrentDateTime) + 1 >= 7
					THEN 7
				ELSE DATEDIFF(WEEK, cs.StartDate, @CurrentDateTime) + 1
				END, c.DayNumber = CASE 
				WHEN DATEDIFF(DAY, cs.StartDate, @CurrentDateTime) >= 49
					THEN 49
				ELSE DATEDIFF(DAY, cs.StartDate, @CurrentDateTime)
				END
		FROM stage.Courses c
		INNER JOIN CV_Prod.dbo.AdClassSched cs ON c.AdClassSchedId = cs.AdClassSchedID;

		--**************************************************************************************************************************************** 
		--Update the stage.Grades table with Cengage values
		--**************************************************************************************************************************************** 
		UPDATE co
		SET co.CengageCourseIndicator = 1
		FROM stage.Courses co
		INNER JOIN Cengage.CourseLookup cl ON co.CourseCode = cl.CourseCode
			AND co.SectionStart BETWEEN cl.StartDate
				AND cl.EndDate;

		--Create a table to hold the holiday schedule as defined by CampusVue and populate it with the Christmas Break Online values 
		DECLARE @Holidays TABLE (StartDate DATE, EndDate DATE, WeeksOff INT);

		INSERT INTO @Holidays (StartDate, EndDate, WeeksOff)
		SELECT ca.StartDate, ca.EndDate, ((DATEDIFF(DAY, ca.StartDate, ca.EndDate) + 1) / 7) 'WeeksOff'
		FROM CV_Prod.dbo.AdCalendar ca
		INNER JOIN CV_Prod.dbo.SyCampusList cl ON ca.SyCampusGrpID = cl.SyCampusGrpID
		WHERE cl.SyCampusID = 9
			AND LEFT(ca.Code, 2) = 'CB'
		ORDER BY ca.StartDate DESC;

		--SELECT * FROM @Holidays; 
		--Update the stage.Courses table to remove holiday weeks before any further proceesing continues
		DECLARE @HolidayDateCheck DATETIME;

		SET @HolidayDateCheck = DATEADD(DAY, 0, DATEDIFF(DAY, 0, GETDATE()));

		UPDATE co
		SET co.WeekNumber = co.WeekNumber - CASE 
				WHEN @HolidayDateCheck < ho.StartDate
					THEN 0
				WHEN @HolidayDateCheck >= ho.StartDate
					AND @HolidayDateCheck <= ho.EndDate
					THEN ls_co.WeekNumber
				WHEN @HolidayDateCheck > ho.EndDate
					THEN ho.WeeksOff
				ELSE 0
				END
		FROM stage.Courses co
		INNER JOIN @Holidays ho ON co.SectionStart <= ho.StartDate
			AND co.SectionEnd >= ho.EndDate
		INNER JOIN LS_ODS.Courses ls_co ON co.CoursePrimaryKey = ls_co.CoursePrimaryKey
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7

		--AND DATEDIFF(DAY, co.SectionStart, GETDATE()) < 50; 
		EXEC LS_ODS.AddODSLoadLog 'Updated Course Start Dates And Week Numbers', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Courses table with week x start date and extension week start date 
		--**************************************************************************************************************************************** 
		--Set the basic start dates 
		UPDATE stage.Courses
		SET Week1StartDate = SectionStart, Week2StartDate = DATEADD(DAY, CASE DATEPART(WEEKDAY, SectionStart)
					WHEN 2
						THEN 7
					WHEN 3
						THEN 6
					ELSE 9999
					END, SectionStart), Week3StartDate = DATEADD(DAY, CASE DATEPART(WEEKDAY, SectionStart)
					WHEN 2
						THEN 14
					WHEN 3
						THEN 13
					ELSE 9999
					END, SectionStart), Week4StartDate = DATEADD(DAY, CASE DATEPART(WEEKDAY, SectionStart)
					WHEN 2
						THEN 21
					WHEN 3
						THEN 20
					ELSE 9999
					END, SectionStart), Week5StartDate = DATEADD(DAY, CASE DATEPART(WEEKDAY, SectionStart)
					WHEN 2
						THEN 28
					WHEN 3
						THEN 27
					ELSE 9999
					END, SectionStart), ExtensionWeekStartDate = DATEADD(DAY, CASE DATEPART(WEEKDAY, SectionStart)
					WHEN 2
						THEN 35
					WHEN 3
						THEN 34
					ELSE 9999
					END, SectionStart);

		--Modify for holidays: processed in reverse (week 5 to week 1) to have correct dates to check 
		UPDATE co
		SET co.Week5StartDate = DATEADD(WEEK, CASE 
					WHEN (
							co.Week1StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week2StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week3StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week4StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week5StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							)
						THEN ho.WeeksOff
					ELSE 0
					END, co.Week5StartDate), co.ExtensionWeekStartDate = DATEADD(WEEK, CASE 
					WHEN (
							co.Week1StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week2StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week3StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week4StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week5StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							)
						THEN ho.WeeksOff
					ELSE 0
					END, co.ExtensionWeekStartDate)
		FROM stage.Courses co
		INNER JOIN @Holidays ho ON co.SectionStart <= ho.StartDate
			AND co.SectionEnd >= ho.EndDate
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7

		--AND DATEDIFF(DAY, co.SectionStart, GETDATE()) < 50; 
		UPDATE co
		SET co.Week4StartDate = DATEADD(WEEK, CASE 
					WHEN (
							co.Week1StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week2StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week3StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week4StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							)
						THEN ho.WeeksOff
					ELSE 0
					END, co.Week4StartDate)
		FROM stage.Courses co
		INNER JOIN @Holidays ho ON co.SectionStart <= ho.StartDate
			AND co.SectionEnd >= ho.EndDate
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7

		--AND DATEDIFF(DAY, co.SectionStart, GETDATE()) < 50; 
		UPDATE co
		SET co.Week3StartDate = DATEADD(WEEK, CASE 
					WHEN (
							co.Week1StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week2StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week3StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							)
						THEN ho.WeeksOff
					ELSE 0
					END, co.Week3StartDate)
		FROM stage.Courses co
		INNER JOIN @Holidays ho ON co.SectionStart <= ho.StartDate
			AND co.SectionEnd >= ho.EndDate
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7

		--AND DATEDIFF(DAY, co.SectionStart, GETDATE()) < 50; 
		UPDATE co
		SET co.Week2StartDate = DATEADD(WEEK, CASE 
					WHEN (
							co.Week1StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							OR co.Week2StartDate BETWEEN ho.StartDate
								AND ho.EndDate
							)
						THEN ho.WeeksOff
					ELSE 0
					END, co.Week2StartDate)
		FROM stage.Courses co
		INNER JOIN @Holidays ho ON co.SectionStart <= ho.StartDate
			AND co.SectionEnd >= ho.EndDate
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7

		--AND DATEDIFF(DAY, co.SectionStart, GETDATE()) < 50; 
		UPDATE co
		SET co.Week1StartDate = DATEADD(WEEK, CASE 
					WHEN co.Week1StartDate BETWEEN ho.StartDate
							AND ho.EndDate
						THEN ho.WeeksOff
					ELSE 0
					END, co.Week1StartDate)
		FROM stage.Courses co
		INNER JOIN @Holidays ho ON co.SectionStart <= ho.StartDate
			AND co.SectionEnd >= ho.EndDate
		WHERE DATEDIFF(WEEK, co.SectionStart, co.SectionEnd) = 7

		--AND DATEDIFF(DAY, co.SectionStart, GETDATE()) < 50; 
		EXEC LS_ODS.AddODSLoadLog 'Updated Course Week X Start Dates And Extension Week Start Date', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Assignments table with the IsReportable and CountsAsSubmission values 
		--**************************************************************************************************************************************** 
		UPDATE a
		SET a.IsReportable = 0
		FROM stage.Assignments a
		INNER JOIN LS_ODS.AssignmentDetails ad ON a.AssignmentTitle = ad.AssignmentTitle
			AND ad.IsReportable = 0;

		UPDATE a
		SET a.CountsAsSubmission = 0
		FROM stage.Assignments a
		INNER JOIN LS_ODS.AssignmentDetails ad ON a.AssignmentTitle = ad.AssignmentTitle
			AND ad.CountsAsSubmission = 0;

		EXEC LS_ODS.AddODSLoadLog 'Updated Assignments IsReportable And CountsAsSubmission Flags', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Courses table with the weekly assignment counts 
		--**************************************************************************************************************************************** 
		WITH cteCounts (CoursePrimaryKey, WeekNumber, AssignmentCount)
		AS (
			SELECT a.CoursePrimaryKey 'CoursePrimaryKey', a.WeekNumber 'WeekNumber', COUNT(a.AssignmentPrimaryKey) 'AssignmentCount'
			FROM stage.Assignments a
			WHERE a.WeekNumber <> 0 --Filter out assignments that are not part of a week 
				AND a.CountsAsSubmission = 1
			GROUP BY a.CoursePrimaryKey, a.WeekNumber
			)
		UPDATE c
		SET c.Week1AssignmentCount = c1.AssignmentCount, c.Week2AssignmentCount = c2.AssignmentCount, c.Week3AssignmentCount = c3.AssignmentCount, c.Week4AssignmentCount = c4.AssignmentCount, c.Week5AssignmentCount = c5.AssignmentCount
		FROM stage.Courses c
		LEFT JOIN cteCounts c1 ON c.CoursePrimaryKey = c1.CoursePrimaryKey
			AND c1.WeekNumber = 1
		LEFT JOIN cteCounts c2 ON c.CoursePrimaryKey = c2.CoursePrimaryKey
			AND c2.WeekNumber = 2
		LEFT JOIN cteCounts c3 ON c.CoursePrimaryKey = c3.CoursePrimaryKey
			AND c3.WeekNumber = 3
		LEFT JOIN cteCounts c4 ON c.CoursePrimaryKey = c4.CoursePrimaryKey
			AND c4.WeekNumber = 4
		LEFT JOIN cteCounts c5 ON c.CoursePrimaryKey = c5.CoursePrimaryKey
			AND c5.WeekNumber = 5;

		EXEC LS_ODS.AddODSLoadLog 'Updated Course Weekly Assignment Counts', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the weekly completed assignment counts and submission rates 
		--**************************************************************************************************************************************** 
		WITH cteCounts (CourseUsersPrimaryKey, WeekNumber, GradeCount)
		AS (
			SELECT g.CourseUsersPrimaryKey 'CourseUsersPrimaryKey', a.WeekNumber 'WeekNumber', COUNT(g.GradePrimaryKey) 'GradeCount'
			FROM stage.Grades g
			INNER JOIN stage.Assignments a ON g.AssignmentPrimaryKey = a.AssignmentPrimaryKey
				AND a.WeekNumber <> 0 --Filter out assignments that are not part of a week 
				AND a.CountsAsSubmission = 1
			WHERE g.AssignmentStatus IN (
					'NEEDS GRADING'
					,'COMPLETED'
					,'IN MORE PROGRESS'
					,'NEEDS MORE GRADING'
					)
				OR (
					a.AlternateTitle LIKE '%Disc%'
					AND g.AssignmentStatus = 'IN PROGRESS'
					)
			GROUP BY g.CourseUsersPrimaryKey, a.WeekNumber
			)
		UPDATE s
		SET s.Week1CompletedAssignments = w1.GradeCount, s.Week2CompletedAssignments = w2.GradeCount, s.Week3CompletedAssignments = w3.GradeCount, s.Week4CompletedAssignments = w4.GradeCount, s.Week5CompletedAssignments = w5.GradeCount, s.Week1CompletionRate = CAST(w1.GradeCount AS NUMERIC(12, 2)) / CAST(c.Week1AssignmentCount AS NUMERIC(12, 2)), s.Week2CompletionRate = CAST(w2.GradeCount AS NUMERIC(12, 2)) / CAST(c.Week2AssignmentCount AS NUMERIC(12, 2)), s.Week3CompletionRate = CAST(w3.GradeCount AS NUMERIC(12, 2)) / CAST(c.Week3AssignmentCount AS NUMERIC(12, 2)), s.Week4CompletionRate = CAST(w4.GradeCount AS NUMERIC(12, 2)) / CAST(c.Week4AssignmentCount AS NUMERIC(12, 2)), s.Week5CompletionRate = CAST(w5.GradeCount AS NUMERIC(12, 2)) / CAST(c.Week5AssignmentCount AS NUMERIC(12, 2)), s.CoursePercentage = CAST((COALESCE(w1.GradeCount, 0) + COALESCE(w2.GradeCount, 0) + COALESCE(w3.GradeCount, 0) + COALESCE(w4.GradeCount, 0) + COALESCE(w5.GradeCount, 0)) AS NUMERIC(12, 2)) / CAST((c.Week1AssignmentCount + c.Week2AssignmentCount + c.Week3AssignmentCount + c.Week4AssignmentCount + c.Week5AssignmentCount
					) AS NUMERIC(12, 2))
		FROM stage.Students s
		INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
		LEFT JOIN cteCounts w1 ON s.CourseUsersPrimaryKey = w1.CourseUsersPrimaryKey
			AND w1.WeekNumber = 1
		LEFT JOIN cteCounts w2 ON s.CourseUsersPrimaryKey = w2.CourseUsersPrimaryKey
			AND w2.WeekNumber = 2
		LEFT JOIN cteCounts w3 ON s.CourseUsersPrimaryKey = w3.CourseUsersPrimaryKey
			AND w3.WeekNumber = 3
		LEFT JOIN cteCounts w4 ON s.CourseUsersPrimaryKey = w4.CourseUsersPrimaryKey
			AND w4.WeekNumber = 4
		LEFT JOIN cteCounts w5 ON s.CourseUsersPrimaryKey = w5.CourseUsersPrimaryKey
			AND w5.WeekNumber = 5;

		WITH cteTotalWork (SyStudentId, SectionStart, CompletedAssignments, TotalAssignments)
		AS (
			SELECT s.SyStudentId, c.SectionStart, SUM(CAST(COALESCE(s.Week1CompletedAssignments, 0) + COALESCE(s.Week2CompletedAssignments, 0) + COALESCE(s.Week3CompletedAssignments, 0) + COALESCE(s.Week4CompletedAssignments, 0) + COALESCE(s.Week5CompletedAssignments, 0) AS NUMERIC(12, 2))) 'CompletedAssignments', SUM(CAST(c.Week1AssignmentCount + c.Week2AssignmentCount + c.Week3AssignmentCount + c.Week4AssignmentCount + c.Week5AssignmentCount AS NUMERIC(12, 2))) 'TotalAssignments'
			FROM stage.Students s
			INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
				AND c.SectionStart IS NOT NULL
			GROUP BY s.SyStudentId, c.SectionStart
			)
		UPDATE s
		SET s.TotalWorkPercentage = tw.CompletedAssignments / tw.TotalAssignments
		FROM stage.Students s
		INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
		INNER JOIN cteTotalWork tw ON s.SyStudentId = tw.SyStudentId
			AND c.SectionStart = tw.SectionStart;

		UPDATE st
		SET st.Week1CompletionRateFixed = CASE 
				WHEN co.DayNumber BETWEEN 0
						AND 6
					THEN st.Week1CompletionRate
				ELSE st1.Week1CompletionRateFixed
				END, st.Week2CompletionRateFixed = CASE 
				WHEN co.DayNumber BETWEEN 7
						AND 13
					THEN st.Week2CompletionRate
				ELSE st1.Week2CompletionRateFixed
				END, st.Week3CompletionRateFixed = CASE 
				WHEN co.DayNumber BETWEEN 14
						AND 20
					THEN st.Week3CompletionRate
				ELSE st1.Week3CompletionRateFixed
				END, st.Week4CompletionRateFixed = CASE 
				WHEN co.DayNumber BETWEEN 21
						AND 27
					THEN st.Week4CompletionRate
				ELSE st1.Week4CompletionRateFixed
				END, st.Week5CompletionRateFixed = CASE 
				WHEN co.DayNumber BETWEEN 28
						AND 34
					THEN st.Week5CompletionRate
				ELSE st1.Week5CompletionRateFixed
				END
		FROM stage.Students st
		INNER JOIN stage.Courses co ON st.AdClassSchedId = co.AdClassSchedId
		LEFT JOIN LS_ODS.Students st1 ON st.SyStudentId = st1.SyStudentId
			AND st.AdClassSchedId = st1.AdClassSchedId;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Counts Of Completed Assignments And Submission Rates', 0;

		--**************************************************************************************************************************************** 
		--Update completion/submission rates by assignment type 
		--****************************************************************************************************************************************		
		EXEC LS_ODS.ProcessStudentRatesByAssignmentType;

		EXEC LS_ODS.AddODSLoadLog 'Updated Completion/Submission Rates By Assignment Type', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the weekly LDAs 
		--**************************************************************************************************************************************** 
		--Get value from current table 
		UPDATE s
		SET s.Week1LDA = sp.Week1LDA, s.Week2LDA = sp.Week2LDA, s.Week3LDA = sp.Week3LDA, s.Week4LDA = sp.Week4LDA, s.Week5LDA = sp.Week5LDA
		FROM stage.Students s
		INNER JOIN LS_ODS.Students sp ON s.SyStudentId = sp.SyStudentId
			AND s.AdEnrollSchedId = sp.AdEnrollSchedId
			AND sp.ActiveFlag = 1;

		DECLARE @WeeklyLDAs TABLE (SyStudentId INT, AdEnrollSchedId INT, WeekNumber INT, LDA DATE);

		--Get new values 
		INSERT INTO @WeeklyLDAs (SyStudentId, AdEnrollSchedId, WeekNumber, LDA)
		SELECT es.SyStudentId 'SyStudentId', es.AdEnrollSchedID 'AdEnrollSchedId', c.WeekNumber 'WeekNumber', es.LDA 'LDA'
		FROM CV_PROD.dbo.AdEnrollSched es WITH (NOLOCK)
		INNER JOIN stage.Courses c ON es.AdClassSchedID = c.AdClassSchedId;

		--Update Week 1 
		UPDATE s
		SET s.Week1LDA = wk.LDA
		FROM stage.Students s
		INNER JOIN @WeeklyLDAs wk ON s.SyStudentId = wk.SyStudentId
			AND s.AdEnrollSchedId = wk.AdEnrollSchedId
			AND wk.WeekNumber = 1;

		--Update Week 2 
		UPDATE s
		SET s.Week2LDA = wk.LDA
		FROM stage.Students s
		INNER JOIN @WeeklyLDAs wk ON s.SyStudentId = wk.SyStudentId
			AND s.AdEnrollSchedId = wk.AdEnrollSchedId
			AND wk.WeekNumber = 2;

		--Update Week 3 
		UPDATE s
		SET s.Week3LDA = wk.LDA
		FROM stage.Students s
		INNER JOIN @WeeklyLDAs wk ON s.SyStudentId = wk.SyStudentId
			AND s.AdEnrollSchedId = wk.AdEnrollSchedId
			AND wk.WeekNumber = 3;

		--Update Week 4 
		UPDATE s
		SET s.Week4LDA = wk.LDA
		FROM stage.Students s
		INNER JOIN @WeeklyLDAs wk ON s.SyStudentId = wk.SyStudentId
			AND s.AdEnrollSchedId = wk.AdEnrollSchedId
			AND wk.WeekNumber = 4;

		--Update Week 5 
		UPDATE s
		SET s.Week5LDA = wk.LDA
		FROM stage.Students s
		INNER JOIN @WeeklyLDAs wk ON s.SyStudentId = wk.SyStudentId
			AND s.AdEnrollSchedId = wk.AdEnrollSchedId
			AND wk.WeekNumber = 5;

		EXEC LS_ODS.AddODSLoadLog 'Updated Student Weekly LDAs', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Grades table with the number of attempts 
		--**************************************************************************************************************************************** 
		WITH cteCounts (GradePrimaryKey, AttemptCount)
		AS (
			SELECT a.GRADEBOOK_GRADE_PK1 'GradePrimaryKey', COUNT(a.PK1) 'AttemptCount'
			FROM ATTEMPT a
			GROUP BY a.GRADEBOOK_GRADE_PK1
			)
		UPDATE g
		SET g.NumberOfAttempts = c.AttemptCount
		FROM stage.Grades g
		INNER JOIN cteCounts c ON g.GradePrimaryKey = c.GradePrimaryKey;

		EXEC LS_ODS.AddODSLoadLog 'Updated Grade Counts Of Attempts', 0;

		--**************************************************************************************************************************************** 
		--Add new records to the TimeInModule table 
		--**************************************************************************************************************************************** 
		INSERT INTO LS_ODS.TimeInModule (ScormRegistrationId, LaunchHistoryId, BlackboardUsername, UserPrimaryKey, SyStudentId, CourseUsersPrimaryKey, CoursePrimaryKey, AssignmentPrimaryKey, StartDateTime, EndDateTime, ElapsedTimeMinutes, CompletionStatus, SatisfactionStatus, ScormRegistrationLaunchHistoryStartDateTimeKey)
		SELECT sr.SCORM_REGISTRATION_ID 'ScormRegistrationId', slh.LAUNCH_HISTORY_ID 'LaunchHistoryId', sr.GLOBAL_OBJECTIVE_SCOPE 'BlackboardUsername', u.PK1 'UserPrimaryKey', REPLACE(u.BATCH_UID, 'SyStudent_', '') 'SyStudentId', cu.PK1 'CourseUsersPrimaryKey', cm.PK1 'CoursePrimaryKey', cc.PK1 'AssignmentPrimaryKey', slh.LAUNCH_TIME 'StartDateTime', slh.EXIT_TIME 'EndDateTime', DATEDIFF(MINUTE, slh.LAUNCH_TIME, slh.EXIT_TIME) 'ElapsedTimeMinutes', slh.COMPLETION 'CompletionStatus', slh.SATISFACTION 'StatisfactionStatus', sr.SCORM_REGISTRATION_ID + '_' + slh.LAUNCH_HISTORY_ID + '_' + CONVERT(VARCHAR(50), slh.LAUNCH_TIME, 126) 'ScormRegistrationLaunchHistoryStartDateTimeKey'
		FROM dbo.SCORMLAUNCHHISTORY slh
		INNER JOIN dbo.SCORMREGISTRATION sr ON slh.SCORM_REGISTRATION_ID = sr.SCORM_REGISTRATION_ID
		INNER JOIN dbo.USERS u ON sr.GLOBAL_OBJECTIVE_SCOPE = u.[USER_ID]
			AND LEFT(u.BATCH_UID, 10) = 'SyStudent_'
		INNER JOIN dbo.COURSE_CONTENTS cc ON REPLACE(REPLACE(sr.CONTENT_ID, '_1', ''), '_', '') = cc.PK1
		INNER JOIN dbo.COURSE_MAIN cm ON cc.CRSMAIN_PK1 = cm.PK1
		INNER JOIN dbo.COURSE_USERS cu ON u.PK1 = cu.USERS_PK1
			AND cm.PK1 = cu.CRSMAIN_PK1
		LEFT JOIN dbo.DATA_SOURCE ds --Adding to deal with erroneous DSKs added
			ON ds.PK1 = cu.DATA_SRC_PK1 --in the SIS Framework cleanup effort
		WHERE sr.SCORM_REGISTRATION_ID + '_' + slh.LAUNCH_HISTORY_ID + '_' + CONVERT(VARCHAR(50), slh.LAUNCH_TIME, 126) NOT IN (
				SELECT tim.ScormRegistrationLaunchHistoryStartDateTimeKey
				FROM LS_ODS.TimeInModule tim
				)
			AND ds.batch_uid NOT IN (
				'ENR_181008_02.txt'
				,'ENR_181008'
				,'ENR_181008_1558036.txt'
				);--Adding to deal with erroneous DSKs added
			--in the SIS Framework cleanup effort 

		EXEC LS_ODS.AddODSLoadLog 'Updated Time In Module Table', 0;

		--**************************************************************************************************************************************** 
		--Update the stage.Students table with the Current Course Grade 
		--**************************************************************************************************************************************** 
		DECLARE @TodayDayNumber INT;

		SET @TodayDayNumber = DATEPART(WEEKDAY, GETDATE());

		--SELECT @TodayDayNumber; 
		WITH cteCurrentCourseGrade (SyStudentId, AdClassSchedId, CurrentCourseGrade)
		AS (
			SELECT s.SyStudentId, s.AdClassSchedId, CASE 
					WHEN s.Week1Grade IS NULL
						AND s.Week2Grade IS NULL
						AND s.Week3Grade IS NULL
						AND s.Week4Grade IS NULL
						AND s.Week5Grade IS NULL
						THEN NULL
					WHEN c.WeekNumber = 1
						THEN 1.0
					WHEN c.WeekNumber = 2
						THEN CASE 
								WHEN @TodayDayNumber < 5
									THEN 1.0
								ELSE s.Week1Grade
								END
					WHEN c.WeekNumber = 3
						THEN CASE 
								WHEN @TodayDayNumber < 5
									THEN s.Week1Grade
								ELSE s.Week2Grade
								END
					WHEN c.WeekNumber = 4
						THEN CASE 
								WHEN @TodayDayNumber < 5
									THEN s.Week2Grade
								ELSE s.Week3Grade
								END
					WHEN c.WeekNumber = 5
						THEN CASE 
								WHEN @TodayDayNumber < 5
									THEN s.Week3Grade
								ELSE s.Week4Grade
								END
					WHEN c.WeekNumber = 6
						THEN CASE 
								WHEN @TodayDayNumber < 5
									THEN s.Week4Grade
								ELSE s.Week5Grade
								END
					ELSE s.Week5Grade
					END 'CurrentCourseGrade'
			FROM stage.Students s
			INNER JOIN stage.Courses c ON s.AdClassSchedId = c.AdClassSchedId
			)
		UPDATE s
		SET s.CurrentCourseGrade = ccg.CurrentCourseGrade
		FROM stage.Students s
		INNER JOIN cteCurrentCourseGrade ccg ON s.SyStudentId = ccg.SyStudentId
			AND s.AdClassSchedId = ccg.AdClassSchedId;

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
		DECLARE @ChangedStudents TABLE (StudentPrimaryKey INT, CourseUsersPrimaryKey INT);

		--Find Changed Students And Populated Table Variable 
		INSERT INTO @ChangedStudents (StudentPrimaryKey, CourseUsersPrimaryKey)
		SELECT new.StudentPrimaryKey, new.CourseUsersPrimaryKey
		FROM stage.Students new
		INNER JOIN LS_ODS.Students old ON new.StudentPrimaryKey = old.StudentPrimaryKey
			AND new.CourseUsersPrimaryKey = old.CourseUsersPrimaryKey
			AND old.ActiveFlag = 1
		WHERE new.DateTimeCreated <> old.DateTimeCreated
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
			OR new.CourseUsersPrimaryKey <> old.CourseUsersPrimaryKey
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
			OR (
				new.DateTimeCreated IS NOT NULL
				AND old.DateTimeCreated IS NULL
				)
			OR (
				new.DateTimeModified IS NOT NULL
				AND old.DateTimeModified IS NULL
				)
			OR (
				new.RowStatus IS NOT NULL
				AND old.RowStatus IS NULL
				)
			OR (
				new.BatchUniqueIdentifier IS NOT NULL
				AND old.BatchUniqueIdentifier IS NULL
				)
			OR (
				new.BlackboardUsername IS NOT NULL
				AND old.BlackboardUsername IS NULL
				)
			OR (
				new.SyStudentId IS NOT NULL
				AND old.SyStudentId IS NULL
				)
			OR (
				new.FirstName IS NOT NULL
				AND old.FirstName IS NULL
				)
			OR (
				new.LastName IS NOT NULL
				AND old.LastName IS NULL
				)
			OR (
				new.Campus IS NOT NULL
				AND old.Campus IS NULL
				)
			OR (
				new.AdEnrollSchedId IS NOT NULL
				AND old.AdEnrollSchedId IS NULL
				)
			OR (
				new.AdClassSchedId IS NOT NULL
				AND old.AdClassSchedId IS NULL
				)
			OR (
				new.LastLoginDateTime IS NOT NULL
				AND old.LastLoginDateTime IS NULL
				)
			OR (
				new.CourseUsersPrimaryKey IS NOT NULL
				AND old.CourseUsersPrimaryKey IS NULL
				)
			OR (
				new.TimeInClass IS NOT NULL
				AND old.TimeInClass IS NULL
				)
			OR (
				new.LastI3InteractionNumberMainPhone IS NOT NULL
				AND old.LastI3InteractionNumberMainPhone IS NULL
				)
			OR (
				new.LastI3InteractionDateTimeMainPhone IS NOT NULL
				AND old.LastI3InteractionDateTimeMainPhone IS NULL
				)
			OR (
				new.DaysSinceLastI3InteractionMainPhone IS NOT NULL
				AND old.DaysSinceLastI3InteractionMainPhone IS NULL
				)
			OR (
				new.LastI3InteractionNumberWorkPhone IS NOT NULL
				AND old.LastI3InteractionNumberWorkPhone IS NULL
				)
			OR (
				new.LastI3InteractionDateTimeWorkPhone IS NOT NULL
				AND old.LastI3InteractionDateTimeWorkPhone IS NULL
				)
			OR (
				new.DaysSinceLastI3InteractionWorkPhone IS NOT NULL
				AND old.DaysSinceLastI3InteractionWorkPhone IS NULL
				)
			OR (
				new.LastI3InteractionNumberMobilePhone IS NOT NULL
				AND old.LastI3InteractionNumberMobilePhone IS NULL
				)
			OR (
				new.LastI3InteractionDateTimeMobilePhone IS NOT NULL
				AND old.LastI3InteractionDateTimeMobilePhone IS NULL
				)
			OR (
				new.DaysSinceLastI3InteractionMobilePhone IS NOT NULL
				AND old.DaysSinceLastI3InteractionMobilePhone IS NULL
				)
			OR (
				new.LastI3InteractionNumberOtherPhone IS NOT NULL
				AND old.LastI3InteractionNumberOtherPhone IS NULL
				)
			OR (
				new.LastI3InteractionDateTimeOtherPhone IS NOT NULL
				AND old.LastI3InteractionDateTimeOtherPhone IS NULL
				)
			OR (
				new.DaysSinceLastI3InteractionOtherPhone IS NOT NULL
				AND old.DaysSinceLastI3InteractionOtherPhone IS NULL
				)
			OR (
				new.Week1Grade IS NOT NULL
				AND old.Week1Grade IS NULL
				)
			OR (
				new.Week2Grade IS NOT NULL
				AND old.Week2Grade IS NULL
				)
			OR (
				new.Week3Grade IS NOT NULL
				AND old.Week3Grade IS NULL
				)
			OR (
				new.Week4Grade IS NOT NULL
				AND old.Week4Grade IS NULL
				)
			OR (
				new.Week5Grade IS NOT NULL
				AND old.Week5Grade IS NULL
				)
			OR (
				new.SelfTestsCount IS NOT NULL
				AND old.SelfTestsCount IS NULL
				)
			OR (
				new.AssessmentsCount IS NOT NULL
				AND old.AssessmentsCount IS NULL
				)
			OR (
				new.AssignmentsCount IS NOT NULL
				AND old.AssignmentsCount IS NULL
				)
			OR (
				new.DiscussionsCount IS NOT NULL
				AND old.DiscussionsCount IS NULL
				)
			OR (
				new.Week1CompletionRate IS NOT NULL
				AND old.Week1CompletionRate IS NULL
				)
			OR (
				new.Week2CompletionRate IS NOT NULL
				AND old.Week2CompletionRate IS NULL
				)
			OR (
				new.Week3CompletionRate IS NOT NULL
				AND old.Week3CompletionRate IS NULL
				)
			OR (
				new.Week4CompletionRate IS NOT NULL
				AND old.Week4CompletionRate IS NULL
				)
			OR (
				new.Week5CompletionRate IS NOT NULL
				AND old.Week5CompletionRate IS NULL
				)
			OR (
				new.VAStudent IS NOT NULL
				AND old.VAStudent IS NULL
				)
			OR (
				new.NoticeName IS NOT NULL
				AND old.NoticeName IS NULL
				)
			OR (
				new.NoticeDueDate IS NOT NULL
				AND old.NoticeDueDate IS NULL
				)
			OR (
				new.VABenefitName IS NOT NULL
				AND old.VABenefitName IS NULL
				)
			OR (
				new.ClassStatus IS NOT NULL
				AND old.ClassStatus IS NULL
				)
			OR (
				new.Week1LDA IS NOT NULL
				AND old.Week1LDA IS NULL
				)
			OR (
				new.Week2LDA IS NOT NULL
				AND old.Week2LDA IS NULL
				)
			OR (
				new.Week3LDA IS NOT NULL
				AND old.Week3LDA IS NULL
				)
			OR (
				new.Week4LDA IS NOT NULL
				AND old.Week4LDA IS NULL
				)
			OR (
				new.Week5LDA IS NOT NULL
				AND old.Week5LDA IS NULL
				)
			OR (
				new.Week1CompletedAssignments IS NOT NULL
				AND old.Week1CompletedAssignments IS NULL
				)
			OR (
				new.Week2CompletedAssignments IS NOT NULL
				AND old.Week2CompletedAssignments IS NULL
				)
			OR (
				new.Week3CompletedAssignments IS NOT NULL
				AND old.Week3CompletedAssignments IS NULL
				)
			OR (
				new.Week4CompletedAssignments IS NOT NULL
				AND old.Week4CompletedAssignments IS NULL
				)
			OR (
				new.Week5CompletedAssignments IS NOT NULL
				AND old.Week5CompletedAssignments IS NULL
				)
			OR (
				new.CoursePercentage IS NOT NULL
				AND old.CoursePercentage IS NULL
				)
			OR (
				new.TotalWorkPercentage IS NOT NULL
				AND old.TotalWorkPercentage IS NULL
				)
			OR (
				new.AdEnrollId IS NOT NULL
				AND old.AdEnrollId IS NULL
				)
			OR (
				new.IsRetake IS NOT NULL
				AND old.IsRetake IS NULL
				)
			OR (
				new.StudentCourseUserKeys IS NOT NULL
				AND old.StudentCourseUserKeys IS NULL
				)
			OR (
				new.CurrentCourseGrade IS NOT NULL
				AND old.CurrentCourseGrade IS NULL
				)
			OR (
				new.ProgramCode IS NOT NULL
				AND old.ProgramCode IS NULL
				)
			OR (
				new.ProgramName IS NOT NULL
				AND old.ProgramName IS NULL
				)
			OR (
				new.ProgramVersionCode IS NOT NULL
				AND old.ProgramVersionCode IS NULL
				)
			OR (
				new.ProgramVersionName IS NOT NULL
				AND old.ProgramVersionName IS NULL
				)
			OR (
				new.MondayTimeInClass IS NOT NULL
				AND old.MondayTimeInClass IS NULL
				)
			OR (
				new.TuesdayTimeInClass IS NOT NULL
				AND old.TuesdayTimeInClass IS NULL
				)
			OR (
				new.WednesdayTimeInClass IS NOT NULL
				AND old.WednesdayTimeInClass IS NULL
				)
			OR (
				new.ThursdayTimeInClass IS NOT NULL
				AND old.ThursdayTimeInClass IS NULL
				)
			OR (
				new.FridayTimeInClass IS NOT NULL
				AND old.FridayTimeInClass IS NULL
				)
			OR (
				new.SaturdayTimeInClass IS NOT NULL
				AND old.SaturdayTimeInClass IS NULL
				)
			OR (
				new.SundayTimeInClass IS NOT NULL
				AND old.SundayTimeInClass IS NULL
				)
			OR (
				new.Week1CompletionRateFixed IS NOT NULL
				AND old.Week1CompletionRateFixed IS NULL
				)
			OR (
				new.Week2CompletionRateFixed IS NOT NULL
				AND old.Week2CompletionRateFixed IS NULL
				)
			OR (
				new.Week3CompletionRateFixed IS NOT NULL
				AND old.Week3CompletionRateFixed IS NULL
				)
			OR (
				new.Week4CompletionRateFixed IS NOT NULL
				AND old.Week4CompletionRateFixed IS NULL
				)
			OR (
				new.Week5CompletionRateFixed IS NOT NULL
				AND old.Week5CompletionRateFixed IS NULL
				)
			OR (
				new.StudentNumber IS NOT NULL
				AND old.StudentNumber IS NULL
				)
			OR (
				new.SourceSystem IS NOT NULL
				AND old.SourceSystem IS NULL
				);

		--Update LS_ODS Students Table To Inactivate Changed Student Records 
		UPDATE old
		SET old.ActiveFlag = 0
		FROM LS_ODS.Students old
		INNER JOIN @ChangedStudents new ON old.StudentPrimaryKey = new.StudentPrimaryKey
			AND old.CourseUsersPrimaryKey = new.CourseUsersPrimaryKey;

		--Add Changed Student Records To LS_ODS Students Table 
		INSERT INTO LS_ODS.Students (
			StudentPrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, BlackboardUsername, SyStudentId, FirstName, LastName, Campus, AdEnrollSchedId, AdClassSchedId, CourseUsersPrimaryKey, LastLoginDateTime, TimeInClass, LastI3InteractionNumberMainPhone, LastI3InteractionDateTimeMainPhone, DaysSinceLastI3InteractionMainPhone, LastI3InteractionNumberWorkPhone, LastI3InteractionDateTimeWorkPhone, DaysSinceLastI3InteractionWorkPhone, LastI3InteractionNumberMobilePhone, LastI3InteractionDateTimeMobilePhone, DaysSinceLastI3InteractionMobilePhone, LastI3InteractionNumberOtherPhone, LastI3InteractionDateTimeOtherPhone, DaysSinceLastI3InteractionOtherPhone, Week1Grade, Week2Grade, Week3Grade, Week4Grade, Week5Grade, SelfTestsCount, AssessmentsCount, AssignmentsCount, DiscussionsCount, Week1CompletionRate, Week2CompletionRate, Week3CompletionRate, Week4CompletionRate, Week5CompletionRate, VAStudent, NoticeName, NoticeDueDate, VABenefitName, ClassStatus, Week1LDA, Week2LDA, Week3LDA, Week4LDA, Week5LDA, 
			Week1CompletedAssignments, Week2CompletedAssignments, Week3CompletedAssignments, Week4CompletedAssignments, Week5CompletedAssignments, CoursePercentage, TotalWorkPercentage, AdEnrollId, IsRetake, StudentCourseUserKeys, CurrentCourseGrade, ProgramCode, ProgramName, ProgramVersionCode, ProgramVersionName, MondayTimeInClass, TuesdayTimeInClass, WednesdayTimeInClass, ThursdayTimeInClass, FridayTimeInClass, SaturdayTimeInClass, SundayTimeInClass, Week1CompletionRateFixed, Week2CompletionRateFixed, Week3CompletionRateFixed, Week4CompletionRateFixed, Week5CompletionRateFixed, StudentNumber, SourceSystem
			)
		SELECT new.StudentPrimaryKey, new.DateTimeCreated, new.DateTimeModified, new.RowStatus, new.BatchUniqueIdentifier, new.BlackboardUsername, new.SyStudentId, new.FirstName, new.LastName, new.Campus, new.AdEnrollSchedId, new.AdClassSchedId, new.CourseUsersPrimaryKey, new.LastLoginDateTime, new.TimeInClass, new.LastI3InteractionNumberMainPhone, new.LastI3InteractionDateTimeMainPhone, new.DaysSinceLastI3InteractionMainPhone, new.LastI3InteractionNumberWorkPhone, new.LastI3InteractionDateTimeWorkPhone, new.DaysSinceLastI3InteractionWorkPhone, new.LastI3InteractionNumberMobilePhone, new.LastI3InteractionDateTimeMobilePhone, new.DaysSinceLastI3InteractionMobilePhone, new.LastI3InteractionNumberOtherPhone, new.LastI3InteractionDateTimeOtherPhone, new.DaysSinceLastI3InteractionOtherPhone, new.Week1Grade, new.Week2Grade, new.Week3Grade, new.Week4Grade, new.Week5Grade, new.SelfTestsCount, new.AssessmentsCount, new.AssignmentsCount, new.DiscussionsCount, new.Week1CompletionRate, new.Week2CompletionRate, new.Week3CompletionRate, 
			new.Week4CompletionRate, new.Week5CompletionRate, new.VAStudent, new.NoticeName, new.NoticeDueDate, new.VABenefitName, new.ClassStatus, new.Week1LDA, new.Week2LDA, new.Week3LDA, new.Week4LDA, new.Week5LDA, new.Week1CompletedAssignments, new.Week2CompletedAssignments, new.Week3CompletedAssignments, new.Week4CompletedAssignments, new.Week5CompletedAssignments, new.CoursePercentage, new.TotalWorkPercentage, new.AdEnrollId, new.IsRetake, new.StudentCourseUserKeys, new.CurrentCourseGrade, new.ProgramCode, new.ProgramName, new.ProgramVersionCode, new.ProgramVersionName, new.MondayTimeInClass, new.TuesdayTimeInClass, new.WednesdayTimeInClass, new.ThursdayTimeInClass, new.FridayTimeInClass, new.SaturdayTimeInClass, new.SundayTimeInClass, new.Week1CompletionRateFixed, new.Week2CompletionRateFixed, new.Week3CompletionRateFixed, new.Week4CompletionRateFixed, new.Week5CompletionRateFixed, new.StudentNumber, new.SourceSystem
		FROM stage.Students new
		INNER JOIN @ChangedStudents changed ON new.StudentPrimaryKey = changed.StudentPrimaryKey
			AND new.CourseUsersPrimaryKey = changed.CourseUsersPrimaryKey
		WHERE new.AdEnrollSchedId IS NOT NULL;

		EXEC LS_ODS.AddODSLoadLog 'Updated Students Records That Have Changed', 0;

		--**************************************************************************************************************************************** 
		--Update Course records that have changed 
		--**************************************************************************************************************************************** 
		--Create Table Variable To Hold Changed Course Records 
		DECLARE @ChangedCourses TABLE (CoursePrimaryKey INT);

		--Find Changed Courses And Populated Table Variable 
		INSERT INTO @ChangedCourses (CoursePrimaryKey)
		SELECT new.CoursePrimaryKey
		FROM stage.Courses new
		INNER JOIN LS_ODS.Courses old ON new.CoursePrimaryKey = old.CoursePrimaryKey
			AND old.ActiveFlag = 1
		WHERE new.DateTimeCreated <> old.DateTimeCreated
			OR new.DateTimeModified <> old.DateTimeModified
			OR new.RowStatus <> old.RowStatus
			OR new.BatchUniqueIdentifier <> old.BatchUniqueIdentifier
			OR new.CourseCode <> old.CourseCode
			OR new.CourseName <> old.CourseName
			OR new.SectionNumber <> old.SectionNumber
			OR new.SectionStart <> old.SectionStart
			OR new.SectionEnd <> old.SectionEnd
			OR new.AdClassSchedId <> old.AdClassSchedId
			OR new.WeekNumber <> old.WeekNumber
			OR new.Week1AssignmentCount <> old.Week1AssignmentCount
			OR new.Week2AssignmentCount <> old.Week2AssignmentCount
			OR new.Week3AssignmentCount <> old.Week3AssignmentCount
			OR new.Week4AssignmentCount <> old.Week4AssignmentCount
			OR new.Week5AssignmentCount <> old.Week5AssignmentCount
			OR new.PrimaryInstructor <> old.PrimaryInstructor
			OR new.SecondaryInstructor <> old.SecondaryInstructor
			OR new.Week1StartDate <> old.Week1StartDate
			OR new.Week2StartDate <> old.Week2StartDate
			OR new.Week3StartDate <> old.Week3StartDate
			OR new.Week4StartDate <> old.Week4StartDate
			OR new.Week5StartDate <> old.Week5StartDate
			OR new.IsOrganization <> old.IsOrganization
			OR new.ExtensionWeekStartDate <> old.ExtensionWeekStartDate
			OR new.AcademicFacilitator <> old.AcademicFacilitator
			OR new.PrimaryInstructorId <> old.PrimaryInstructorId
			OR new.SecondaryInstructorId <> old.SecondaryInstructorId
			OR new.AcademicFacilitatorId <> old.AcademicFacilitatorId
			OR new.DayNumber <> old.DayNumber
			OR new.CengageCourseIndicator <> old.CengageCourseIndicator
			OR (
				new.DateTimeCreated IS NOT NULL
				AND old.DateTimeCreated IS NULL
				)
			OR (
				new.DateTimeModified IS NOT NULL
				AND old.DateTimeModified IS NULL
				)
			OR (
				new.RowStatus IS NOT NULL
				AND old.RowStatus IS NULL
				)
			OR (
				new.BatchUniqueIdentifier IS NOT NULL
				AND old.BatchUniqueIdentifier IS NULL
				)
			OR (
				new.CourseCode IS NOT NULL
				AND old.CourseCode IS NULL
				)
			OR (
				new.CourseName IS NOT NULL
				AND old.CourseName IS NULL
				)
			OR (
				new.SectionNumber IS NOT NULL
				AND old.SectionNumber IS NULL
				)
			OR (
				new.SectionStart IS NOT NULL
				AND old.SectionStart IS NULL
				)
			OR (
				new.SectionEnd IS NOT NULL
				AND old.SectionEnd IS NULL
				)
			OR (
				new.AdClassSchedId IS NOT NULL
				AND old.AdClassSchedId IS NULL
				)
			OR (
				new.WeekNumber IS NOT NULL
				AND old.WeekNumber IS NULL
				)
			OR (
				new.Week1AssignmentCount IS NOT NULL
				AND old.Week1AssignmentCount IS NULL
				)
			OR (
				new.Week2AssignmentCount IS NOT NULL
				AND old.Week2AssignmentCount IS NULL
				)
			OR (
				new.Week3AssignmentCount IS NOT NULL
				AND old.Week3AssignmentCount IS NULL
				)
			OR (
				new.Week4AssignmentCount IS NOT NULL
				AND old.Week4AssignmentCount IS NULL
				)
			OR (
				new.Week5AssignmentCount IS NOT NULL
				AND old.Week5AssignmentCount IS NULL
				)
			OR (
				new.PrimaryInstructor IS NOT NULL
				AND old.PrimaryInstructor IS NULL
				)
			OR (
				new.SecondaryInstructor IS NOT NULL
				AND old.SecondaryInstructor IS NULL
				)
			OR (
				new.Week1StartDate IS NOT NULL
				AND old.Week1StartDate IS NULL
				)
			OR (
				new.Week2StartDate IS NOT NULL
				AND old.Week2StartDate IS NULL
				)
			OR (
				new.Week3StartDate IS NOT NULL
				AND old.Week3StartDate IS NULL
				)
			OR (
				new.Week4StartDate IS NOT NULL
				AND old.Week4StartDate IS NULL
				)
			OR (
				new.Week5StartDate IS NOT NULL
				AND old.Week5StartDate IS NULL
				)
			OR (
				new.ExtensionWeekStartDate IS NOT NULL
				AND old.ExtensionWeekStartDate IS NULL
				)
			OR (
				new.IsOrganization IS NOT NULL
				AND old.IsOrganization IS NULL
				)
			OR (
				new.AcademicFacilitator IS NOT NULL
				AND old.AcademicFacilitator IS NULL
				)
			OR (
				new.PrimaryInstructorId IS NOT NULL
				AND old.PrimaryInstructorId IS NULL
				)
			OR (
				new.SecondaryInstructorId IS NOT NULL
				AND old.SecondaryInstructorId IS NULL
				)
			OR (
				new.AcademicFacilitatorId IS NOT NULL
				AND old.AcademicFacilitatorId IS NULL
				)
			OR (
				new.DayNumber IS NOT NULL
				AND old.DayNumber IS NULL
				)
			OR (
				new.CengageCourseIndicator IS NOT NULL
				AND old.CengageCourseIndicator IS NULL
				)
			OR (
				new.SourceSystem IS NOT NULL
				AND old.SourceSystem IS NULL
				);

		--Update LS_ODS Course Table To Inactivate Changed Course Records 
		UPDATE old
		SET old.ActiveFlag = 0
		FROM LS_ODS.Courses old
		INNER JOIN @ChangedCourses new ON old.CoursePrimaryKey = new.CoursePrimaryKey;

		--Add Changed Course Records To LS_ODS Course Table 
		INSERT INTO LS_ODS.Courses (CoursePrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, CourseCode, CourseName, SectionNumber, SectionStart, SectionEnd, AdClassSchedId, WeekNumber, Week1AssignmentCount, Week2AssignmentCount, Week3AssignmentCount, Week4AssignmentCount, Week5AssignmentCount, PrimaryInstructor, SecondaryInstructor, Week1StartDate, Week2StartDate, Week3StartDate, Week4StartDate, Week5StartDate, ExtensionWeekStartDate, IsOrganization, AcademicFacilitator, PrimaryInstructorId, SecondaryInstructorId, AcademicFacilitatorId, DayNumber, CengageCourseIndicator, SourceSystem)
		SELECT new.CoursePrimaryKey, new.DateTimeCreated, new.DateTimeModified, new.RowStatus, new.BatchUniqueIdentifier, new.CourseCode, new.CourseName, new.SectionNumber, new.SectionStart, new.SectionEnd, new.AdClassSchedId, new.WeekNumber, new.Week1AssignmentCount, new.Week2AssignmentCount, new.Week3AssignmentCount, new.Week4AssignmentCount, new.Week5AssignmentCount, new.PrimaryInstructor, new.SecondaryInstructor, new.Week1StartDate, new.Week2StartDate, new.Week3StartDate, new.Week4StartDate, new.Week5StartDate, new.ExtensionWeekStartDate, new.IsOrganization, new.AcademicFacilitator, new.PrimaryInstructorId, new.SecondaryInstructorId, new.AcademicFacilitatorId, new.DayNumber, new.CengageCourseIndicator, new.SourceSystem
		FROM stage.Courses new
		INNER JOIN @ChangedCourses changed ON new.CoursePrimaryKey = changed.CoursePrimaryKey;

		EXEC LS_ODS.AddODSLoadLog 'Updated Course Records That Have Changed', 0;

		--**************************************************************************************************************************************** 
		--Update Assignment records that have changed 
		--**************************************************************************************************************************************** 
		--Create Table Variable To Hold Changed Assignment Records 
		DECLARE @ChangedAssignments TABLE (AssignmentPrimaryKey INT);

		--Find Changed Assignments And Populated Table Variable 
		INSERT INTO @ChangedAssignments (AssignmentPrimaryKey)
		SELECT new.AssignmentPrimaryKey
		FROM stage.Assignments new
		INNER JOIN LS_ODS.Assignments old ON new.AssignmentPrimaryKey = old.AssignmentPrimaryKey
			AND old.ActiveFlag = 1
		WHERE new.CoursePrimaryKey <> old.CoursePrimaryKey
			OR new.WeekNumber <> old.WeekNumber
			OR new.AssignmentTitle <> old.AssignmentTitle
			OR new.DueDate <> old.DueDate
			OR new.PossiblePoints <> old.PossiblePoints
			OR new.DateTimeCreated <> old.DateTimeCreated
			OR new.DateTimeModified <> old.DateTimeModified
			OR new.ScoreProviderHandle <> old.ScoreProviderHandle
			OR new.CourseContentsPrimaryKey1 <> old.CourseContentsPrimaryKey1
			OR new.AlternateTitle <> old.AlternateTitle
			OR new.IsReportable <> old.IsReportable
			OR new.CountsAsSubmission <> old.CountsAsSubmission
			OR new.AssignmentType <> old.AssignmentType
			OR (
				new.CoursePrimaryKey IS NOT NULL
				AND old.CoursePrimaryKey IS NULL
				)
			OR (
				new.WeekNumber IS NOT NULL
				AND old.WeekNumber IS NULL
				)
			OR (
				new.AssignmentTitle IS NOT NULL
				AND old.AssignmentTitle IS NULL
				)
			OR (
				new.DueDate IS NOT NULL
				AND old.DueDate IS NULL
				)
			OR (
				new.PossiblePoints IS NOT NULL
				AND old.PossiblePoints IS NULL
				)
			OR (
				new.DateTimeCreated IS NOT NULL
				AND old.DateTimeCreated IS NULL
				)
			OR (
				new.DateTimeModified IS NOT NULL
				AND old.DateTimeModified IS NULL
				)
			OR (
				new.ScoreProviderHandle IS NOT NULL
				AND old.ScoreProviderHandle IS NULL
				)
			OR (
				new.CourseContentsPrimaryKey1 IS NOT NULL
				AND old.CourseContentsPrimaryKey1 IS NULL
				)
			OR (
				new.AlternateTitle IS NOT NULL
				AND old.AlternateTitle IS NULL
				)
			OR (
				new.IsReportable IS NOT NULL
				AND old.IsReportable IS NULL
				)
			OR (
				new.CountsAsSubmission IS NOT NULL
				AND old.CountsAsSubmission IS NULL
				)
			OR (
				new.AssignmentType IS NOT NULL
				AND old.AssignmentType IS NULL
				)
			OR (
				new.SourceSystem IS NOT NULL
				AND old.SourceSystem IS NULL
				);

		--Update LS_ODS Assignments Table To Inactivate Changed Assignments Records 
		UPDATE old
		SET old.ActiveFlag = 0
		FROM LS_ODS.Assignments old
		INNER JOIN @ChangedAssignments new ON old.AssignmentPrimaryKey = new.AssignmentPrimaryKey;

		DECLARE @CountStageD2LAssignments AS INT

		SELECT @CountStageD2LAssignments = COUNT(*)
		FROM [stage].[Assignments]
		WHERE SourceSystem = 'D2L'

		IF @CountStageD2LAssignments > 0
		BEGIN
			-- Update LS_ODS Assignments Table To Inactivate Duplicated D2L Assignments
			UPDATE Assignments
			SET Assignments.[ActiveFlag] = 0
			FROM [LS_ODS].[Assignments] Assignments
			WHERE Assignments.[AssignmentPrimaryKey] IN (
					SELECT asg.[AssignmentPrimaryKey]
					FROM (
						SELECT [asg].[CoursePrimaryKey], [asg].[WeekNumber], [asg].[AssignmentTitle], COUNT(*) AS Total
						FROM [LS_ODS].[Assignments] asg
						GROUP BY [asg].[CoursePrimaryKey], [asg].[WeekNumber], [asg].[AssignmentTitle]
						HAVING COUNT(*) > 1
						) da
					INNER JOIN [LS_ODS].[Assignments] asg ON [da].[CoursePrimaryKey] = [asg].[CoursePrimaryKey]
						AND [da].[WeekNumber] = [asg].[WeekNumber]
						AND [da].[AssignmentTitle] = [asg].[AssignmentTitle]
					INNER JOIN [dbo].[COURSE_MAIN] cm ON [cm].PK1 = [asg].[CoursePrimaryKey]
						AND [cm].[SourceSystem] = 'D2L'
					LEFT JOIN [stage].[Assignments] sasg ON sasg.[AssignmentPrimaryKey] = [asg].[AssignmentPrimaryKey]
					WHERE [sasg].[AssignmentPrimaryKey] IS NULL
					)

			-- UPDATE LS_ODS Assignments Table To Inactivate deleted Assignments
			UPDATE Assignments
			SET Assignments.[ActiveFlag] = 0
			FROM [LS_ODS].[Assignments] Assignments
			LEFT JOIN [stage].[Assignments] sasg ON sasg.[AssignmentPrimaryKey] = Assignments.[AssignmentPrimaryKey]
			WHERE Assignments.[SourceSystem] = 'D2L'
				AND sasg.[AssignmentPrimaryKey] IS NULL
		END

		--Add Changed Assignment Records To LS_ODS Assignments Table 
		INSERT INTO LS_ODS.Assignments (AssignmentPrimaryKey, CoursePrimaryKey, WeekNumber, AssignmentTitle, DueDate, PossiblePoints, DateTimeCreated, DateTimeModified, ScoreProviderHandle, CourseContentsPrimaryKey1, AlternateTitle, IsReportable, CountsAsSubmission, AssignmentType, SourceSystem)
		SELECT new.AssignmentPrimaryKey, new.CoursePrimaryKey, new.WeekNumber, new.AssignmentTitle, new.DueDate, new.PossiblePoints, new.DateTimeCreated, new.DateTimeModified, new.ScoreProviderHandle, new.CourseContentsPrimaryKey1, new.AlternateTitle, new.IsReportable, new.CountsAsSubmission, new.AssignmentType, new.SourceSystem
		FROM stage.Assignments new
		INNER JOIN @ChangedAssignments changed ON new.AssignmentPrimaryKey = changed.AssignmentPrimaryKey;

		EXEC LS_ODS.AddODSLoadLog 'Updated Assignment Records That Have Changed', 0;

		--**************************************************************************************************************************************** 
		--Update Grade records that have changed 
		--**************************************************************************************************************************************** 
		--Create Table Variable To Hold Changed Grades Records 
		DECLARE @ChangedGrades TABLE (GradePrimaryKey INT);

		--Find Changed Grades And Populated Table Variable 
		INSERT INTO @ChangedGrades (GradePrimaryKey)
		SELECT new.GradePrimaryKey
		FROM stage.Grades new
		INNER JOIN LS_ODS.Grades old ON new.GradePrimaryKey = old.GradePrimaryKey
			AND old.ActiveFlag = 1
		WHERE new.CourseUsersPrimaryKey <> old.CourseUsersPrimaryKey
			OR new.RowStatus <> old.RowStatus
			OR new.HighestScore <> old.HighestScore
			OR new.HighestGrade <> old.HighestGrade
			OR new.HighestAttemptDateTime <> old.HighestAttemptDateTime
			OR new.ManualScore <> old.ManualScore
			OR new.ManualGrade <> old.ManualGrade
			OR new.ManualDateTime <> old.ManualDateTime
			OR new.ExemptIndicator <> old.ExemptIndicator
			OR new.HighestDateTimeCreated <> old.HighestDateTimeCreated
			OR new.HighestDateTimeModified <> old.HighestDateTimeModified
			OR new.HighestIsLatestAttemptIndicator <> old.HighestIsLatestAttemptIndicator
			OR new.NumberOfAttempts <> old.NumberOfAttempts
			OR new.FirstScore <> old.FirstScore
			OR new.FirstGrade <> old.FirstGrade
			OR new.FirstAttemptDateTime <> old.FirstAttemptDateTime
			OR new.FirstIsLatestAttemptIndicator <> old.FirstIsLatestAttemptIndicator
			OR new.FirstDateTimeCreated <> old.FirstDateTimeCreated
			OR new.FirstDateTimeModified <> old.FirstDateTimeModified
			OR new.AssignmentPrimaryKey <> old.AssignmentPrimaryKey
			OR new.AssignmentStatus <> old.AssignmentStatus
			OR (
				new.CourseUsersPrimaryKey IS NOT NULL
				AND old.CourseUsersPrimaryKey IS NULL
				)
			OR (
				new.RowStatus IS NOT NULL
				AND old.RowStatus IS NULL
				)
			OR (
				new.HighestScore IS NOT NULL
				AND old.HighestScore IS NULL
				)
			OR (
				new.HighestGrade IS NOT NULL
				AND old.HighestGrade IS NULL
				)
			OR (
				new.HighestAttemptDateTime IS NOT NULL
				AND old.HighestAttemptDateTime IS NULL
				)
			OR (
				new.ManualScore IS NOT NULL
				AND old.ManualScore IS NULL
				)
			OR (
				new.ManualGrade IS NOT NULL
				AND old.ManualGrade IS NULL
				)
			OR (
				new.ManualDateTime IS NOT NULL
				AND old.ManualDateTime IS NULL
				)
			OR (
				new.ExemptIndicator IS NOT NULL
				AND old.ExemptIndicator IS NULL
				)
			OR (
				new.HighestDateTimeCreated IS NOT NULL
				AND old.HighestDateTimeCreated IS NULL
				)
			OR (
				new.HighestDateTimeModified IS NOT NULL
				AND old.HighestDateTimeModified IS NULL
				)
			OR (
				new.HighestIsLatestAttemptIndicator IS NOT NULL
				AND old.HighestIsLatestAttemptIndicator IS NULL
				)
			OR (
				new.NumberOfAttempts IS NOT NULL
				AND old.NumberOfAttempts IS NULL
				)
			OR (
				new.FirstScore IS NOT NULL
				AND old.FirstScore IS NULL
				)
			OR (
				new.FirstGrade IS NOT NULL
				AND old.FirstGrade IS NULL
				)
			OR (
				new.FirstAttemptDateTime IS NOT NULL
				AND old.FirstAttemptDateTime IS NULL
				)
			OR (
				new.FirstIsLatestAttemptIndicator IS NOT NULL
				AND old.FirstIsLatestAttemptIndicator IS NULL
				)
			OR (
				new.FirstDateTimeCreated IS NOT NULL
				AND old.FirstDateTimeCreated IS NULL
				)
			OR (
				new.FirstDateTimeModified IS NOT NULL
				AND old.FirstDateTimeModified IS NULL
				)
			OR (
				new.AssignmentPrimaryKey IS NOT NULL
				AND old.AssignmentPrimaryKey IS NULL
				)
			OR (
				new.AssignmentStatus IS NOT NULL
				AND old.AssignmentStatus IS NULL
				)
			OR (
				new.SourceSystem IS NOT NULL
				AND old.SourceSystem IS NULL
				);

		--Performance Fix for LS_ODS Grades Table CRUD; Index Balancing
		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashActiveFlagLargeIncludeDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashAssignmentPrimaryKeyDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashCourseUsersPrimaryKeyDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_CourseUsersPKAssignPKActiveFG'
				)
		BEGIN
			DROP INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = '_dta_index_Grades_10_1223675407__K2_K23_K21_4'
				)
		BEGIN
			DROP INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_ODS_010'
				)
		BEGIN
			DROP INDEX idx_ODS_010 ON LS_ODS.Grades;
		END;

		--Update LS_ODS Grades Table To Inactivate Changed Grades Records 
		UPDATE old
		SET old.ActiveFlag = 0
		FROM LS_ODS.Grades old
		INNER JOIN @ChangedGrades new ON old.GradePrimaryKey = new.GradePrimaryKey;

		-- Update LS_ODS Grades Table To Inactivate Grades with Duplicated D2L Assignments
		UPDATE Grades
		SET Grades.[ActiveFlag] = 0
		FROM [LS_ODS].[Grades] Grades
		WHERE Grades.[AssignmentPrimaryKey] IN (
				SELECT [asg].[AssignmentPrimaryKey]
				FROM (
					SELECT [asg].[CoursePrimaryKey], [asg].[WeekNumber], [asg].[AssignmentTitle], COUNT(*) AS Total
					FROM [LS_ODS].[Assignments] asg
					GROUP BY [asg].[CoursePrimaryKey], [asg].[WeekNumber], [asg].[AssignmentTitle]
					HAVING COUNT(*) > 1
					) da
				INNER JOIN [LS_ODS].[Assignments] asg ON [da].[CoursePrimaryKey] = [asg].[CoursePrimaryKey]
					AND [da].[WeekNumber] = [asg].[WeekNumber]
					AND [da].[AssignmentTitle] = [asg].[AssignmentTitle]
				INNER JOIN [dbo].[COURSE_MAIN] cm ON [cm].PK1 = [asg].[CoursePrimaryKey]
					AND [cm].[SourceSystem] = 'D2L'
				LEFT JOIN [stage].[Assignments] sasg ON sasg.[AssignmentPrimaryKey] = [asg].[AssignmentPrimaryKey]
				WHERE [sasg].[AssignmentPrimaryKey] IS NULL
				)

		--Add Changed Grades Records To LS_ODS Grades Table 
		INSERT INTO LS_ODS.Grades (GradePrimaryKey, CourseUsersPrimaryKey, RowStatus, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, ManualDateTime, ExemptIndicator, HighestDateTimeCreated, HighestDateTimeModified, HighestIsLatestAttemptIndicator, NumberOfAttempts, FirstScore, FirstGrade, FirstAttemptDateTime, FirstIsLatestAttemptIndicator, FirstDateTimeCreated, FirstDateTimeModified, AssignmentPrimaryKey, AssignmentStatus, SourceSystem)
		SELECT new.GradePrimaryKey, new.CourseUsersPrimaryKey, new.RowStatus, new.HighestScore, new.HighestGrade, new.HighestAttemptDateTime, new.ManualScore, new.ManualGrade, new.ManualDateTime, new.ExemptIndicator, new.HighestDateTimeCreated, new.HighestDateTimeModified, new.HighestIsLatestAttemptIndicator, new.NumberOfAttempts, new.FirstScore, new.FirstGrade, new.FirstAttemptDateTime, new.FirstIsLatestAttemptIndicator, new.FirstDateTimeCreated, new.FirstDateTimeModified, new.AssignmentPrimaryKey, new.AssignmentStatus, new.SourceSystem
		FROM stage.Grades new
		INNER JOIN @ChangedGrades changed ON new.GradePrimaryKey = changed.GradePrimaryKey;

		--Performance Fix for LS_ODS Grades Table CRUD; Index Balancing
		CREATE NONCLUSTERED INDEX idx_ODS_010 ON LS_ODS.Grades (GradePrimaryKey ASC, ActiveFlag ASC) INCLUDE (CourseUsersPrimaryKey, RowStatus, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, ManualDateTime, ExemptIndicator, HighestDateTimeCreated, HighestDateTimeModified, HighestIsLatestAttemptIndicator, NumberOfAttempts, FirstScore, FirstGrade, FirstAttemptDateTime, FirstIsLatestAttemptIndicator, FirstDateTimeModified, AssignmentPrimaryKey, AssignmentStatus, SourceSystem)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades (CourseUsersPrimaryKey ASC, ActiveFlag ASC, AssignmentPrimaryKey ASC) INCLUDE (HighestScore)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades (CourseUsersPrimaryKey DESC) INCLUDE (GradePrimaryKey, AssignmentPrimaryKey, ActiveFlag, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades (CourseUsersPrimaryKey DESC, AssignmentPrimaryKey DESC, ActiveFlag DESC) INCLUDE (GradePrimaryKey, HighestScore, HighestDateTimeCreated, AssignmentStatus)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades (AssignmentPrimaryKey DESC) INCLUDE (GradePrimaryKey, CourseUsersPrimaryKey, ActiveFlag, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades (ActiveFlag DESC) INCLUDE (GradePrimaryKey, AssignmentPrimaryKey, CourseUsersPrimaryKey, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades (CourseUsersPrimaryKey ASC, AssignmentPrimaryKey ASC, ActiveFlag ASC) INCLUDE (GradePrimaryKey, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts, AssignmentStatus)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		EXEC LS_ODS.AddODSLoadLog 'Updated Grades Records That Have Changed', 0;

		--**************************************************************************************************************************************** 
		--Add new Student records 
		--**************************************************************************************************************************************** 
		--Insert New Student Records To Students Table 
		INSERT INTO LS_ODS.Students (
			StudentPrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, BlackboardUsername, SyStudentId, FirstName, LastName, Campus, AdEnrollSchedId, AdClassSchedId, CourseUsersPrimaryKey, LastLoginDateTime, TimeInClass, LastI3InteractionNumberMainPhone, LastI3InteractionDateTimeMainPhone, DaysSinceLastI3InteractionMainPhone, LastI3InteractionNumberWorkPhone, LastI3InteractionDateTimeWorkPhone, DaysSinceLastI3InteractionWorkPhone, LastI3InteractionNumberMobilePhone, LastI3InteractionDateTimeMobilePhone, DaysSinceLastI3InteractionMobilePhone, LastI3InteractionNumberOtherPhone, LastI3InteractionDateTimeOtherPhone, DaysSinceLastI3InteractionOtherPhone, Week1Grade, Week2Grade, Week3Grade, Week4Grade, Week5Grade, SelfTestsCount, AssessmentsCount, AssignmentsCount, DiscussionsCount, Week1CompletionRate, Week2CompletionRate, Week3CompletionRate, Week4CompletionRate, Week5CompletionRate, VAStudent, NoticeName, NoticeDueDate, VABenefitName, ClassStatus, Week1LDA, Week2LDA, Week3LDA, Week4LDA, Week5LDA, 
			Week1CompletedAssignments, Week2CompletedAssignments, Week3CompletedAssignments, Week4CompletedAssignments, Week5CompletedAssignments, CoursePercentage, TotalWorkPercentage, AdEnrollId, IsRetake, StudentCourseUserKeys, CurrentCourseGrade, ProgramCode, ProgramName, ProgramVersionCode, ProgramVersionName, MondayTimeInClass, TuesdayTimeInClass, WednesdayTimeInClass, ThursdayTimeInClass, FridayTimeInClass, SaturdayTimeInClass, SundayTimeInClass, Week1CompletionRateFixed, Week2CompletionRateFixed, Week3CompletionRateFixed, Week4CompletionRateFixed, Week5CompletionRateFixed, StudentNumber, SourceSystem
			)
		SELECT DISTINCT new.StudentPrimaryKey, new.DateTimeCreated, new.DateTimeModified, new.RowStatus, new.BatchUniqueIdentifier, new.BlackboardUsername, new.SyStudentId, new.FirstName, new.LastName, new.Campus, new.AdEnrollSchedId, new.AdClassSchedId, new.CourseUsersPrimaryKey, new.LastLoginDateTime, new.TimeInClass, new.LastI3InteractionNumberMainPhone, new.LastI3InteractionDateTimeMainPhone, new.DaysSinceLastI3InteractionMainPhone, new.LastI3InteractionNumberWorkPhone, new.LastI3InteractionDateTimeWorkPhone, new.DaysSinceLastI3InteractionWorkPhone, new.LastI3InteractionNumberMobilePhone, new.LastI3InteractionDateTimeMobilePhone, new.DaysSinceLastI3InteractionMobilePhone, new.LastI3InteractionNumberOtherPhone, new.LastI3InteractionDateTimeOtherPhone, new.DaysSinceLastI3InteractionOtherPhone, new.Week1Grade, new.Week2Grade, new.Week3Grade, new.Week4Grade, new.Week5Grade, new.SelfTestsCount, new.AssessmentsCount, new.AssignmentsCount, new.DiscussionsCount, new.Week1CompletionRate, new.Week2CompletionRate, new.
			Week3CompletionRate, new.Week4CompletionRate, new.Week5CompletionRate, new.VAStudent, new.NoticeName, new.NoticeDueDate, new.VABenefitName, new.ClassStatus, new.Week1LDA, new.Week2LDA, new.Week3LDA, new.Week4LDA, new.Week5LDA, new.Week1CompletedAssignments, new.Week2CompletedAssignments, new.Week3CompletedAssignments, new.Week4CompletedAssignments, new.Week5CompletedAssignments, new.CoursePercentage, new.TotalWorkPercentage, new.AdEnrollId, new.IsRetake, new.StudentCourseUserKeys, new.CurrentCourseGrade, new.ProgramCode, new.ProgramName, new.ProgramVersionCode, new.ProgramVersionName, new.MondayTimeInClass, new.TuesdayTimeInClass, new.WednesdayTimeInClass, new.ThursdayTimeInClass, new.FridayTimeInClass, new.SaturdayTimeInClass, new.SundayTimeInClass, new.Week1CompletionRateFixed, new.Week2CompletionRateFixed, new.Week3CompletionRateFixed, new.Week4CompletionRateFixed, new.Week5CompletionRateFixed, new.StudentNumber, new.SourceSystem
		FROM stage.Students new
		WHERE new.StudentCourseUserKeys NOT IN (
				SELECT old.StudentCourseUserKeys
				FROM LS_ODS.Students old
				WHERE old.StudentPrimaryKey IS NOT NULL
					AND old.CourseUsersPrimaryKey IS NOT NULL
				);

		EXEC LS_ODS.AddODSLoadLog 'Added New Students Records', 0;

		--**************************************************************************************************************************************** 
		--Add new Course records 
		--**************************************************************************************************************************************** 
		--Insert New Course Records To Courses Table 
		INSERT INTO LS_ODS.Courses (CoursePrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, CourseCode, CourseName, SectionNumber, SectionStart, SectionEnd, AdClassSchedId, WeekNumber, Week1AssignmentCount, Week2AssignmentCount, Week3AssignmentCount, Week4AssignmentCount, Week5AssignmentCount, PrimaryInstructor, SecondaryInstructor, Week1StartDate, Week2StartDate, Week3StartDate, Week4StartDate, Week5StartDate, ExtensionWeekStartDate, IsOrganization, AcademicFacilitator, PrimaryInstructorId, SecondaryInstructorId, AcademicFacilitatorId, DayNumber, CengageCourseIndicator, SourceSystem)
		SELECT new.CoursePrimaryKey, new.DateTimeCreated, new.DateTimeModified, new.RowStatus, new.BatchUniqueIdentifier, new.CourseCode, new.CourseName, new.SectionNumber, new.SectionStart, new.SectionEnd, new.AdClassSchedId, new.WeekNumber, new.Week1AssignmentCount, new.Week2AssignmentCount, new.Week3AssignmentCount, new.Week4AssignmentCount, new.Week5AssignmentCount, new.PrimaryInstructor, new.SecondaryInstructor, new.Week1StartDate, new.Week2StartDate, new.Week3StartDate, new.Week4StartDate, new.Week5StartDate, new.ExtensionWeekStartDate, new.IsOrganization, new.AcademicFacilitator, new.PrimaryInstructorId, new.SecondaryInstructorId, new.AcademicFacilitatorId, new.DayNumber, new.CengageCourseIndicator, new.SourceSystem
		FROM stage.Courses new
		WHERE NOT EXISTS (
				SELECT 1
				FROM LS_ODS.Courses old
				WHERE old.CoursePrimaryKey = new.CoursePrimaryKey
				);

		EXEC LS_ODS.AddODSLoadLog 'Added New Course Records', 0;

		--**************************************************************************************************************************************** 
		--Add new Assignment records 
		--**************************************************************************************************************************************** 
		--Insert New Assignment Records To Assignments Table 
		INSERT INTO LS_ODS.Assignments (AssignmentPrimaryKey, CoursePrimaryKey, WeekNumber, AssignmentTitle, DueDate, PossiblePoints, DateTimeCreated, DateTimeModified, ScoreProviderHandle, CourseContentsPrimaryKey1, AlternateTitle, IsReportable, CountsAsSubmission, AssignmentType, SourceSystem)
		SELECT new.AssignmentPrimaryKey, new.CoursePrimaryKey, new.WeekNumber, new.AssignmentTitle, new.DueDate, new.PossiblePoints, new.DateTimeCreated, new.DateTimeModified, new.ScoreProviderHandle, new.CourseContentsPrimaryKey1, new.AlternateTitle, new.IsReportable, new.CountsAsSubmission, new.AssignmentType, new.SourceSystem
		FROM stage.Assignments new
		WHERE NOT EXISTS (
				SELECT 1
				FROM LS_ODS.Assignments old
				WHERE old.AssignmentPrimaryKey = new.AssignmentPrimaryKey
				);

		EXEC LS_ODS.AddODSLoadLog 'Added New Assignment Records', 0;

		--**************************************************************************************************************************************** 
		--Add new Grade records 
		--**************************************************************************************************************************************** 
		--Insert New Grade Records Into Grades Table 
		INSERT INTO LS_ODS.Grades (GradePrimaryKey, CourseUsersPrimaryKey, RowStatus, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, ManualDateTime, ExemptIndicator, HighestDateTimeCreated, HighestDateTimeModified, HighestIsLatestAttemptIndicator, NumberOfAttempts, FirstScore, FirstGrade, FirstAttemptDateTime, FirstIsLatestAttemptIndicator, FirstDateTimeCreated, FirstDateTimeModified, AssignmentPrimaryKey, AssignmentStatus, SourceSystem)
		SELECT DISTINCT new.GradePrimaryKey, new.CourseUsersPrimaryKey, new.RowStatus, new.HighestScore, new.HighestGrade, new.HighestAttemptDateTime, new.ManualScore, new.ManualGrade, new.ManualDateTime, new.ExemptIndicator, new.HighestDateTimeCreated, new.HighestDateTimeModified, new.HighestIsLatestAttemptIndicator, new.NumberOfAttempts, new.FirstScore, new.FirstGrade, new.FirstAttemptDateTime, new.FirstIsLatestAttemptIndicator, new.FirstDateTimeCreated, new.FirstDateTimeModified, new.AssignmentPrimaryKey, new.AssignmentStatus, new.SourceSystem
		FROM stage.Grades new
		WHERE NOT EXISTS (
				SELECT 1
				FROM LS_ODS.Grades old
				WHERE old.GradePrimaryKey = new.GradePrimaryKey
				);

		EXEC LS_ODS.AddODSLoadLog 'Added New Grade Records', 0;

		--**************************************************************************************************************************************** 
		--Remove all records in the Students table with no StudentCourseUserKey 
		--**************************************************************************************************************************************** 
		DELETE
		FROM LS_ODS.Students
		WHERE StudentCourseUserKeys IS NULL;

		EXEC LS_ODS.AddODSLoadLog 'Removed Student Records With No Valid StudentCourseUserKey Value', 0;

		--**************************************************************************************************************************************** 
		--Handle Grade records with negative primary keys 
		--These come from Documents, Weekly Roadmaps, and various other "assignments" that are not true assignments. 
		--The negative value appears because the assignment has not been released to the student for use (adaptive release). 
		--We do not need to report on these value so we can just delete them from the database. 
		--**************************************************************************************************************************************** 
		DELETE
		FROM LS_ODS.Grades
		WHERE GradePrimaryKey < 0
			AND GradePrimaryKey NOT BETWEEN - 514999999
				AND - 514000000;

		EXEC LS_ODS.AddODSLoadLog 'Removed Grade Records With Negative Primary Keys', 0;

		--**************************************************************************************************************************************** 
		--Process Course Activity Counts for BI Reporting needs 
		--**************************************************************************************************************************************** 
		EXEC LS_ODS.ProcessCourseActivityCounts;

		EXEC LS_ODS.AddODSLoadLog 'Processed Course Activity Counts', 0;

		--**************************************************************************************************************************************** 
		--Create a distinct list of all courses to ensure any course no longer in the GradeExtract is disabled 
		--**************************************************************************************************************************************** 
		DECLARE @DisabledCourses TABLE (CoursePrimaryKey INT, AdClassSchedId INT);

		WITH cActiveCourses (CoursePrimaryKey, AdClassSchedId)
		AS (
			SELECT DISTINCT CAST(gei.CoursePK1 AS INT) 'CoursePrimaryKey', CAST(REPLACE(gei.CourseEPK, 'AdCourse_', '') AS INT) 'AdClassSchedId'
			FROM stage.GradeExtractImport gei
			WHERE LEFT(gei.UserEPK, 9) = 'SyStudent' --Only Students 
				AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8 --Filter Out Test/Bad Students 
				AND LEFT(gei.CourseEPK, 8) = 'AdCourse' --Only Courses 
				AND (
					gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --Filter Out Test/Bad Courses 
					OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --To bring in CLW courses 
					OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%' --2/28/2024 CML: Captures EMT Courses
					OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%'
					) --2/28/2024 CML: Captures EMT Courses based out of CLW 
				AND gei.UserFirstName NOT LIKE 'BBAFL%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART1%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART2%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART3%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART4%' --More Test Students
				AND gei.USEREPK NOT LIKE '%PART5%' --More Test Students
			), cAllCourses (CoursePrimaryKey, AdClassSchedId)
		AS (
			SELECT DISTINCT c.CoursePrimaryKey 'CoursePrimaryKey', c.AdClassSchedId 'AdClassSched'
			FROM LS_ODS.Courses c
			WHERE c.ActiveFlag = 1
			)
		INSERT INTO @DisabledCourses (CoursePrimaryKey, AdClassSchedId)
		SELECT ac.CoursePrimaryKey 'CoursePrimaryKey', ac.AdClassSchedId 'AdClassSchedId'
		FROM cAllCourses ac
		INNER JOIN cActiveCourses acc ON ac.AdClassSchedId = acc.AdClassSchedId
			AND ac.CoursePrimaryKey <> acc.CoursePrimaryKey;

		UPDATE c
		SET c.ActiveFlag = 0
		FROM LS_ODS.Courses c
		INNER JOIN @DisabledCourses dc ON c.CoursePrimaryKey = dc.CoursePrimaryKey
			AND c.AdClassSchedId = dc.AdClassSchedId;

		EXEC LS_ODS.AddODSLoadLog 'Removed Disable Courses', 0;

		--**************************************************************************************************************************************** 
		--Create a distinct list of all student/section combinations to ensure any student moved from one section to another has the old section disabled 
		--**************************************************************************************************************************************** 
		DECLARE @DisabledStudentCourseCombinations TABLE (SyStudentId INT, AdEnrollSchedId INT, AdClassSchedId INT);

		WITH cActiveStudentCourseCombinations (SystudentId, AdEnrollSchedId, AdClassSchedId)
		AS (
			SELECT DISTINCT REPLACE(gei.UserEPK, 'SyStudent_', '') 'SyStudentId', CAST(CAST(es.AdEnrollSchedID AS VARCHAR(100)) AS INT) 'AdEnrollSchedId', CAST(REPLACE(gei.CourseEPK, 'AdCourse_', '') AS INT) 'AdClassSchedId'
			FROM stage.GradeExtractImport gei
			LEFT JOIN CV_Prod.dbo.AdEnrollSched es ON REPLACE(gei.CourseEPK, 'AdCourse_', '') = CAST(es.AdClassSchedID AS VARCHAR(50))
				AND REPLACE(gei.UserEPK, 'SyStudent_', '') = CAST(es.SyStudentID AS VARCHAR(50))
			LEFT JOIN CV_Prod.dbo.AdClassSched cs ON CAST(REPLACE(gei.CourseEPK, 'AdCourse_', '') AS INT) = cs.AdClassSchedID
			WHERE LEFT(gei.UserEPK, 9) = 'SyStudent' --Only Students 
				AND LEN(REPLACE(gei.UserEPK, 'SyStudent_', '')) <= 8 --Filter Out Test/Bad Students 
				AND LEFT(gei.CourseEPK, 8) = 'AdCourse' --Only Courses 
				AND (
					gei.CourseTitle LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --Filter Out Test/Bad Courses 
					OR gei.CourseTitle LIKE '[A-Z][A-Z][-][0-9][0-9][0-9][0-9][ABCDEFGHIJKLMNOPQRSTUVWXYZ:]%' --To bring in CLW courses 
					OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9]%' --2/28/2024 CML: Captures EMT Courses
					OR gei.CourseTitle LIKE '[A-Z][A-Z][A-Z][-][0-9][0-9][0-9][0-9]%'
					) --2/28/2024 CML: Captures EMT Courses based out of CLW 
				AND gei.UserFirstName NOT LIKE 'BBAFL%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART1%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART2%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART3%' --More Test Students
				AND gei.UserEPK NOT LIKE '%PART4%' --More Test Students
				AND gei.USEREPK NOT LIKE '%PART5%' --More Test Students
			), cAllStudentCourseCombinations (SyStudentId, AdEnrollSchedId, AdClassSchedId)
		AS (
			SELECT DISTINCT s.SyStudentId 'SyStudentId', s.AdEnrollSchedId 'AdEnrollSchedId', s.AdClassSchedId 'AdClassSchedId'
			FROM LS_ODS.Students s
			WHERE s.ActiveFlag = 1
			)
		INSERT INTO @DisabledStudentCourseCombinations (SyStudentId, AdEnrollSchedId, AdClassSchedId)
		SELECT cAllStudentsCourses.SyStudentId 'SyStudentId', cAllStudentsCourses.AdEnrollSchedId 'AdEnrollSched', cAllStudentsCourses.AdClassSchedId 'AdClassSched'
		FROM cAllStudentCourseCombinations cAllStudentsCourses
		INNER JOIN cActiveStudentCourseCombinations cActiveStudentsCourses ON cAllStudentsCourses.SyStudentId = cActiveStudentsCourses.SystudentId
			AND cAllStudentsCourses.AdEnrollSchedId = cActiveStudentsCourses.AdEnrollSchedId
			AND cAllStudentsCourses.AdClassSchedId <> cActiveStudentsCourses.AdClassSchedId;

		UPDATE s
		SET s.ActiveFlag = 0
		FROM LS_ODS.Students s
		INNER JOIN @DisabledStudentCourseCombinations dssc ON s.SyStudentId = dssc.SyStudentId
			AND s.AdEnrollSchedId = dssc.AdEnrollSchedId
			AND s.AdClassSchedId = dssc.AdClassSchedId;

		EXEC LS_ODS.AddODSLoadLog 'Removed Disabled Student/Course Combinations', 0;

		--**************************************************************************************************************************************** 
		--Disable all students with no matching CampusVue Enrollment records 
		--**************************************************************************************************************************************** 
		IF OBJECT_ID('tempdb..#NonMatchedStudents') IS NOT NULL
			DROP TABLE #NonMatchedStudents;

		CREATE TABLE #NonMatchedStudents (SyStudentId INT, AdEnrollSchedId INT, AdClassSchedId INT);

		INSERT INTO #NonMatchedStudents (SyStudentId, AdEnrollSchedId, AdClassSchedId)
		SELECT s.SyStudentId, s.AdEnrollSchedId, s.AdClassSchedId
		FROM LS_ODS.Students s;

		DELETE s
		FROM #NonMatchedStudents s
		INNER JOIN CV_Prod.dbo.AdEnrollSched es ON s.SyStudentId = es.SyStudentId
			AND s.AdEnrollSchedId = es.AdEnrollSchedId
			AND s.AdClassSchedId = es.AdClassSchedId;

		UPDATE s
		SET ActiveFlag = 0
		FROM LS_ODS.Students s
		INNER JOIN #NonMatchedStudents s2 ON s.SyStudentId = s2.SyStudentId
			AND s.AdEnrollSchedId = s2.AdEnrollSchedId
			AND s.AdClassSchedId = s2.AdClassSchedId;

		EXEC LS_ODS.AddODSLoadLog 'Removed Students With No CampusVue Enrollment Records', 0;

		--**************************************************************************************************************************************** 
		--Move old Students records to Audit table 
		--**************************************************************************************************************************************** 
		INSERT INTO Archive.Students (
			StudentPrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, BlackboardUsername, SyStudentId, FirstName, LastName, Campus, AdEnrollSchedId, AdClassSchedId, CourseUsersPrimaryKey, LastLoginDateTime, TimeInClass, LastI3InteractionNumberMainPhone, LastI3InteractionDateTimeMainPhone, DaysSinceLastI3InteractionMainPhone, LastI3InteractionNumberWorkPhone, LastI3InteractionDateTimeWorkPhone, DaysSinceLastI3InteractionWorkPhone, LastI3InteractionNumberMobilePhone, LastI3InteractionDateTimeMobilePhone, DaysSinceLastI3InteractionMobilePhone, LastI3InteractionNumberOtherPhone, LastI3InteractionDateTimeOtherPhone, DaysSinceLastI3InteractionOtherPhone, Week1Grade, Week2Grade, Week3Grade, Week4Grade, Week5Grade, SelfTestsCount, AssessmentsCount, AssignmentsCount, DiscussionsCount, Week1CompletionRate, Week2CompletionRate, Week3CompletionRate, Week4CompletionRate, Week5CompletionRate, ActiveFlag, UMADateTimeAdded, VAStudent, NoticeName, NoticeDueDate, VABenefitName, ClassStatus, ODSPrimaryKey, Week1LDA, 
			Week2LDA, Week3LDA, Week4LDA, Week5LDA, Week1CompletedAssignments, Week2CompletedAssignments, Week3CompletedAssignments, Week4CompletedAssignments, Week5CompletedAssignments, CoursePercentage, TotalWorkPercentage, AdEnrollId, IsRetake, StudentCourseUserKeys, CurrentCourseGrade, ProgramCode, ProgramName, ProgramVersionCode, ProgramVersionName, MondayTimeInClass, TuesdayTimeInClass, WednesdayTimeInClass, ThursdayTimeInClass, FridayTimeInClass, SaturdayTimeInClass, SundayTimeInClass, Week1CompletionRateFixed, Week2CompletionRateFixed, Week3CompletionRateFixed, Week4CompletionRateFixed, Week5CompletionRateFixed, StudentNumber, IsOrphanRecord, SourceSystem
			)
		SELECT s.StudentPrimaryKey, s.DateTimeCreated, s.DateTimeModified, s.RowStatus, s.BatchUniqueIdentifier, s.BlackboardUsername, s.SyStudentId, s.FirstName, s.LastName, s.Campus, s.AdEnrollSchedId, s.AdClassSchedId, s.CourseUsersPrimaryKey, s.LastLoginDateTime, s.TimeInClass, s.LastI3InteractionNumberMainPhone, s.LastI3InteractionDateTimeMainPhone, s.DaysSinceLastI3InteractionMainPhone, s.LastI3InteractionNumberWorkPhone, s.LastI3InteractionDateTimeWorkPhone, s.DaysSinceLastI3InteractionWorkPhone, s.LastI3InteractionNumberMobilePhone, s.LastI3InteractionDateTimeMobilePhone, s.DaysSinceLastI3InteractionMobilePhone, s.LastI3InteractionNumberOtherPhone, s.LastI3InteractionDateTimeOtherPhone, s.DaysSinceLastI3InteractionOtherPhone, s.Week1Grade, s.Week2Grade, s.Week3Grade, s.Week4Grade, s.Week5Grade, s.SelfTestsCount, s.AssessmentsCount, s.AssignmentsCount, s.DiscussionsCount, s.Week1CompletionRate, s.Week2CompletionRate, s.Week3CompletionRate, s.Week4CompletionRate, s.Week5CompletionRate, s.ActiveFlag, s.UMADateTimeAdded, s.
			VAStudent, s.NoticeName, s.NoticeDueDate, s.VABenefitName, s.ClassStatus, s.ODSPrimaryKey, s.Week1LDA, s.Week2LDA, s.Week3LDA, s.Week4LDA, s.Week5LDA, s.Week1CompletedAssignments, s.Week2CompletedAssignments, s.Week3CompletedAssignments, s.Week4CompletedAssignments, s.Week5CompletedAssignments, s.CoursePercentage, s.TotalWorkPercentage, s.AdEnrollId, s.IsRetake, s.StudentCourseUserKeys, s.CurrentCourseGrade, s.ProgramCode, s.ProgramName, s.ProgramVersionCode, s.ProgramVersionName, s.MondayTimeInClass, s.TuesdayTimeInClass, s.WednesdayTimeInClass, s.ThursdayTimeInClass, s.FridayTimeInClass, s.SaturdayTimeInClass, s.SundayTimeInClass, s.Week1CompletionRateFixed, s.Week2CompletionRateFixed, s.Week3CompletionRateFixed, s.Week4CompletionRateFixed, s.Week5CompletionRateFixed, s.StudentNumber, 0, s.SourceSystem
		FROM LS_ODS.Students s
		WHERE s.ActiveFlag = 0;

		DELETE
		FROM LS_ODS.Students
		WHERE ActiveFlag = 0;

		EXEC LS_ODS.AddODSLoadLog 'Moved Old Student Records To Archive Table', 0;

		--**************************************************************************************************************************************** 
		--Move old Courses records to Audit table 
		--**************************************************************************************************************************************** 
		INSERT INTO Archive.Courses (CoursePrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, CourseCode, CourseName, SectionNumber, SectionStart, SectionEnd, AdClassSchedId, WeekNumber, Week1AssignmentCount, Week2AssignmentCount, Week3AssignmentCount, Week4AssignmentCount, Week5AssignmentCount, ActiveFlag, UMADateTimeAdded, ODSPrimaryKey, PrimaryInstructor, SecondaryInstructor, Week1StartDate, Week2StartDate, Week3StartDate, Week4StartDate, Week5StartDate, ExtensionWeekStartDate, IsOrganziation, AcademicFacilitator, PrimaryInstructorId, SecondaryInstructorId, AcademicFacilitatorId, DayNumber, CengageCourseIndicator, SourceSystem)
		SELECT c.CoursePrimaryKey, c.DateTimeCreated, c.DateTimeModified, c.RowStatus, c.BatchUniqueIdentifier, c.CourseCode, c.CourseName, c.SectionNumber, c.SectionStart, c.SectionEnd, c.AdClassSchedId, c.WeekNumber, c.Week1AssignmentCount, c.Week2AssignmentCount, c.Week3AssignmentCount, c.Week4AssignmentCount, c.Week5AssignmentCount, c.ActiveFlag, c.UMADateTimeAdded, c.ODSPrimaryKey, c.PrimaryInstructor, c.SecondaryInstructor, c.Week1StartDate, c.Week2StartDate, c.Week3StartDate, c.Week4StartDate, c.Week5StartDate, c.ExtensionWeekStartDate, c.IsOrganization, c.AcademicFacilitator, c.PrimaryInstructorId, c.SecondaryInstructorId, c.AcademicFacilitatorId, c.DayNumber, c.CengageCourseIndicator, c.SourceSystem
		FROM LS_ODS.Courses c
		WHERE c.ActiveFlag = 0;

		DELETE
		FROM LS_ODS.Courses
		WHERE ActiveFlag = 0;

		EXEC LS_ODS.AddODSLoadLog 'Moved Old Course Records To Archive Table', 0;

		--**************************************************************************************************************************************** 
		--Move old Assignments records to Audit table 
		--**************************************************************************************************************************************** 
		INSERT INTO Archive.Assignments (AssignmentPrimaryKey, CoursePrimaryKey, WeekNumber, AssignmentTitle, DueDate, PossiblePoints, DateTimeCreated, DateTimeModified, ScoreProviderHandle, ActiveFlag, UMADateTimeAdded, CourseContentsPrimaryKey1, ODSPrimaryKey, AlternateTitle, IsReportable, CountsAsSubmission, AssignmentType, SourceSystem)
		SELECT a.AssignmentPrimaryKey, a.CoursePrimaryKey, a.WeekNumber, a.AssignmentTitle, a.DueDate, a.PossiblePoints, a.DateTimeCreated, a.DateTimeModified, a.ScoreProviderHandle, a.ActiveFlag, a.UMADateTimeAdded, a.CourseContentsPrimaryKey1, a.ODSPrimaryKey, a.AlternateTitle, a.IsReportable, a.CountsAsSubmission, a.AssignmentType, a.SourceSystem
		FROM LS_ODS.Assignments a
		WHERE a.ActiveFlag = 0;

		DELETE
		FROM LS_ODS.Assignments
		WHERE ActiveFlag = 0;

		EXEC LS_ODS.AddODSLoadLog 'Moved Old Assignments Records To Archive Table', 0;

		----**************************************************************************************************************************************** 
		----Move old Grades records to Audit table 
		----**************************************************************************************************************************************** 
		--Performance Fix for LS_ODS Grades Table CRUD; Index Balancing
		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashActiveFlagLargeIncludeDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashAssignmentPrimaryKeyDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashCourseUsersPrimaryKeyDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_CourseUsersPKAssignPKActiveFG'
				)
		BEGIN
			DROP INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = '_dta_index_Grades_10_1223675407__K2_K23_K21_4'
				)
		BEGIN
			DROP INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_ODS_010'
				)
		BEGIN
			DROP INDEX idx_ODS_010 ON LS_ODS.Grades;
		END;

		--Merge into audit table     
		DROP TABLE

		IF EXISTS #LSODSGRADE
			SELECT *
			INTO #LSODSGRADE
			FROM LS_ODS.Grades
			WHERE ActiveFlag = 0

		DELETE
		FROM #LSODSGRADE
		WHERE ODSPrimaryKey IN (
				SELECT ODSPrimaryKey
				FROM Audit.Grades
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
		WHEN NOT MATCHED BY TARGET
			AND src.ActiveFlag = 0
			THEN
				INSERT ([GradePrimaryKey], [CourseUsersPrimaryKey], [RowStatus], [HighestScore], [HighestGrade], [HighestAttemptDateTime], [ManualScore], [ManualGrade], [ManualDateTime], [ExemptIndicator], [HighestDateTimeCreated], [HighestDateTimeModified], [HighestIsLatestAttemptIndicator], [NumberOfAttempts], [FirstScore], [FirstGrade], [FirstAttemptDateTime], [FirstIsLatestAttemptIndicator], [FirstDateTimeCreated], [FirstDateTimeModified], [AssignmentPrimaryKey], [AssignmentStatus], [ActiveFlag], [UMADateTimeAdded], [ODSPrimaryKey], SourceSystem)
				VALUES (src.[GradePrimaryKey], src.[CourseUsersPrimaryKey], src.[RowStatus], src.[HighestScore], src.[HighestGrade], src.[HighestAttemptDateTime], src.[ManualScore], src.[ManualGrade], src.[ManualDateTime], src.[ExemptIndicator], src.[HighestDateTimeCreated], src.[HighestDateTimeModified], src.[HighestIsLatestAttemptIndicator], src.[NumberOfAttempts], src.[FirstScore], src.[FirstGrade], src.[FirstAttemptDateTime], src.[FirstIsLatestAttemptIndicator], src.[FirstDateTimeCreated], src.[FirstDateTimeModified], src.[AssignmentPrimaryKey], src.[AssignmentStatus], src.[ActiveFlag], src.[UMADateTimeAdded], src.[ODSPrimaryKey], src.SourceSystem)
		WHEN MATCHED
			THEN
				UPDATE
				SET trg.[GradePrimaryKey] = src.[GradePrimaryKey], trg.[CourseUsersPrimaryKey] = src.[CourseUsersPrimaryKey], trg.[RowStatus] = src.[RowStatus], trg.[HighestScore] = src.[HighestScore], trg.[HighestGrade] = src.[HighestGrade], trg.[HighestAttemptDateTime] = src.[HighestAttemptDateTime], trg.[ManualScore] = src.[ManualScore], trg.[ManualGrade] = src.[ManualGrade], trg.[ManualDateTime] = src.[ManualDateTime], trg.[ExemptIndicator] = src.[ExemptIndicator], trg.[HighestDateTimeCreated] = src.[HighestDateTimeCreated], trg.[HighestDateTimeModified] = src.[HighestDateTimeModified], trg.[HighestIsLatestAttemptIndicator] = src.[HighestIsLatestAttemptIndicator], trg.[NumberOfAttempts] = src.[NumberOfAttempts], trg.[FirstScore] = src.[FirstScore], trg.[FirstGrade] = src.[FirstGrade], trg.[FirstAttemptDateTime] = src.[FirstAttemptDateTime], trg.[FirstIsLatestAttemptIndicator] = src.[FirstIsLatestAttemptIndicator], trg.[FirstDateTimeCreated] = src.[FirstDateTimeCreated], trg.[FirstDateTimeModified] = src.[FirstDateTimeModified], trg.
					[AssignmentPrimaryKey] = src.[AssignmentPrimaryKey], trg.[AssignmentStatus] = src.[AssignmentStatus], trg.[ActiveFlag] = src.[ActiveFlag], trg.[UMADateTimeAdded] = src.[UMADateTimeAdded], trg.[ODSPrimaryKey] = src.[ODSPrimaryKey], trg.SourceSystem = src.SourceSystem;

		DELETE
		FROM LS_ODS.Grades
		WHERE ActiveFlag = 0;

		--Performance Fix for LS_ODS Grades Table CRUD; Index Balancing
		CREATE NONCLUSTERED INDEX idx_ODS_010 ON LS_ODS.Grades (GradePrimaryKey ASC, ActiveFlag ASC) INCLUDE (CourseUsersPrimaryKey, RowStatus, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, ManualDateTime, ExemptIndicator, HighestDateTimeCreated, HighestDateTimeModified, HighestIsLatestAttemptIndicator, NumberOfAttempts, FirstScore, FirstGrade, FirstAttemptDateTime, FirstIsLatestAttemptIndicator, FirstDateTimeModified, AssignmentPrimaryKey, AssignmentStatus, SourceSystem)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades (CourseUsersPrimaryKey ASC, ActiveFlag ASC, AssignmentPrimaryKey ASC) INCLUDE (HighestScore)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades (CourseUsersPrimaryKey DESC) INCLUDE (GradePrimaryKey, AssignmentPrimaryKey, ActiveFlag, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades (CourseUsersPrimaryKey DESC, AssignmentPrimaryKey DESC, ActiveFlag DESC) INCLUDE (GradePrimaryKey, HighestScore, HighestDateTimeCreated, AssignmentStatus)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades (AssignmentPrimaryKey DESC) INCLUDE (GradePrimaryKey, CourseUsersPrimaryKey, ActiveFlag, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades (ActiveFlag DESC) INCLUDE (GradePrimaryKey, AssignmentPrimaryKey, CourseUsersPrimaryKey, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades (CourseUsersPrimaryKey ASC, AssignmentPrimaryKey ASC, ActiveFlag ASC) INCLUDE (GradePrimaryKey, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts, AssignmentStatus)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		EXEC LS_ODS.AddODSLoadLog 'Moved Old Grades Records To Archive Table', 0;

		--If Weekly Course Graded Activity and Weekly Course Grades steps are running after 9am, we should let LS know.
		--**************************************************************************************************************************************** 
		--Remove all duplicates from each of the ODS tables 
		--**************************************************************************************************************************************** 
		WITH cteStudent
		AS (
			SELECT s.StudentPrimaryKey, s.DateTimeCreated, s.DateTimeModified, s.RowStatus, s.BatchUniqueIdentifier, s.BlackboardUsername, s.SyStudentId, s.FirstName, s.LastName, s.Campus, s.AdEnrollSchedId, s.AdClassSchedId, s.CourseUsersPrimaryKey, s.LastLoginDateTime, s.TimeInClass, s.LastI3InteractionNumberMainPhone, s.LastI3InteractionDateTimeMainPhone, s.DaysSinceLastI3InteractionMainPhone, s.LastI3InteractionNumberWorkPhone, s.LastI3InteractionDateTimeWorkPhone, s.DaysSinceLastI3InteractionWorkPhone, s.LastI3InteractionNumberMobilePhone, s.LastI3InteractionDateTimeMobilePhone, s.DaysSinceLastI3InteractionMobilePhone, s.LastI3InteractionNumberOtherPhone, s.LastI3InteractionDateTimeOtherPhone, s.DaysSinceLastI3InteractionOtherPhone, s.Week1Grade, s.Week2Grade, s.Week3Grade, s.Week4Grade, s.Week5Grade, s.SelfTestsCount, s.AssessmentsCount, s.AssignmentsCount, s.DiscussionsCount, s.ActivitiesCount, s.Week1CompletionRate, s.Week2CompletionRate, s.Week3CompletionRate, s.Week4CompletionRate, s.Week5CompletionRate, s.ActiveFlag
				, s.UMADateTimeAdded, s.VAStudent, s.NoticeName, s.NoticeDueDate, s.VABenefitName, s.ClassStatus, s.Week1LDA, s.Week2LDA, s.Week3LDA, s.Week4LDA, s.Week5LDA, s.Week1CompletedAssignments, s.Week2CompletedAssignments, s.Week3CompletedAssignments, s.Week4CompletedAssignments, s.Week5CompletedAssignments, s.CoursePercentage, s.TotalWorkPercentage, s.AdEnrollId, s.IsRetake, s.StudentCourseUserKeys, s.CurrentCourseGrade, s.ProgramCode, s.ProgramName, s.ProgramVersionCode, s.ProgramVersionName, s.MondayTimeInClass, s.TuesdayTimeInClass, s.WednesdayTimeInClass, s.ThursdayTimeInClass, s.FridayTimeInClass, s.SaturdayTimeInClass, s.SundayTimeInClass, s.Week1CompletionRateFixed, s.Week2CompletionRateFixed, s.Week3CompletionRateFixed, s.Week4CompletionRateFixed, s.Week5CompletionRateFixed, s.StudentNumber, s.SourceSystem, ROW_NUMBER() OVER (
					PARTITION BY s.StudentPrimaryKey, s.DateTimeCreated, s.DateTimeModified, s.RowStatus, s.BatchUniqueIdentifier, s.BlackboardUsername, s.SyStudentId, s.FirstName, s.LastName, s.Campus, s.AdEnrollSchedId, s.AdClassSchedId, s.CourseUsersPrimaryKey, s.LastLoginDateTime, s.TimeInClass, s.LastI3InteractionNumberMainPhone, s.LastI3InteractionDateTimeMainPhone, s.DaysSinceLastI3InteractionMainPhone, s.LastI3InteractionNumberWorkPhone, s.LastI3InteractionDateTimeWorkPhone, s.DaysSinceLastI3InteractionWorkPhone, s.LastI3InteractionNumberMobilePhone, s.LastI3InteractionDateTimeMobilePhone, s.DaysSinceLastI3InteractionMobilePhone, s.LastI3InteractionNumberOtherPhone, s.LastI3InteractionDateTimeOtherPhone, s.DaysSinceLastI3InteractionOtherPhone, s.Week1Grade, s.Week2Grade, s.Week3Grade, s.Week4Grade, s.Week5Grade, s.SelfTestsCount, s.AssessmentsCount, s.AssignmentsCount, s.DiscussionsCount, s.ActivitiesCount, s.Week1CompletionRate, s.Week2CompletionRate, s.Week3CompletionRate, s.Week4CompletionRate, s.Week5CompletionRate
					, s.ActiveFlag, s.UMADateTimeAdded, s.VAStudent, s.NoticeName, s.NoticeDueDate, s.VABenefitName, s.ClassStatus, s.Week1LDA, s.Week2LDA, s.Week3LDA, s.Week4LDA, s.Week5LDA, s.Week1CompletedAssignments, s.Week2CompletedAssignments, s.Week3CompletedAssignments, s.Week4CompletedAssignments, s.Week5CompletedAssignments, s.CoursePercentage, s.TotalWorkPercentage, s.AdEnrollId, s.IsRetake, s.StudentCourseUserKeys, s.CurrentCourseGrade, s.ProgramCode, s.ProgramName, s.ProgramVersionCode, s.ProgramVersionName, s.MondayTimeInClass, s.TuesdayTimeInClass, s.WednesdayTimeInClass, s.ThursdayTimeInClass, s.FridayTimeInClass, s.SaturdayTimeInClass, s.SundayTimeInClass, s.Week1CompletionRateFixed, s.Week2CompletionRateFixed, s.Week3CompletionRateFixed, s.Week4CompletionRateFixed, s.Week5CompletionRateFixed, s.StudentNumber, s.SourceSystem ORDER BY s.UMADateTimeAdded
					) 'RowNumber'
			FROM LS_ODS.Students s
			)
		DELETE cteStudent
		WHERE RowNumber > 1;

		EXEC LS_ODS.AddODSLoadLog 'Student Duplicate Check And Deletion Complete', 0;

		WITH cteCourse
		AS (
			SELECT c.CoursePrimaryKey, c.DateTimeCreated, c.DateTimeModified, c.RowStatus, c.BatchUniqueIdentifier, c.CourseCode, c.CourseName, c.SectionNumber, c.SectionStart, c.SectionEnd, c.AdClassSchedId, c.WeekNumber, c.Week1AssignmentCount, c.Week2AssignmentCount, c.Week3AssignmentCount, c.Week4AssignmentCount, c.Week5AssignmentCount, c.TotalAssignmentCount, c.ActiveFlag, c.UMADateTimeAdded, c.PrimaryInstructor, c.SecondaryInstructor, c.Week1StartDate, c.Week2StartDate, c.Week3StartDate, c.Week4StartDate, c.Week5StartDate, c.ExtensionWeekStartDate, c.IsOrganization, c.AcademicFacilitator, c.PrimaryInstructorId, c.SecondaryInstructorId, c.AcademicFacilitatorId, c.DayNumber, c.CengageCourseIndicator, c.SourceSystem, ROW_NUMBER() OVER (
					PARTITION BY c.CoursePrimaryKey, c.DateTimeCreated, c.DateTimeModified, c.RowStatus, c.BatchUniqueIdentifier, c.CourseCode, c.CourseName, c.SectionNumber, c.SectionStart, c.SectionEnd, c.AdClassSchedId, c.WeekNumber, c.Week1AssignmentCount, c.Week2AssignmentCount, c.Week3AssignmentCount, c.Week4AssignmentCount, c.Week5AssignmentCount, c.TotalAssignmentCount, c.ActiveFlag, c.UMADateTimeAdded, c.PrimaryInstructor, c.SecondaryInstructor, c.Week1StartDate, c.Week2StartDate, c.Week3StartDate, c.Week4StartDate, c.Week5StartDate, c.ExtensionWeekStartDate, c.IsOrganization, c.AcademicFacilitator, c.PrimaryInstructorId, c.SecondaryInstructorId, c.AcademicFacilitatorId, c.DayNumber, c.CengageCourseIndicator, c.SourceSystem ORDER BY c.UMADateTimeAdded
					) 'RowNumber'
			FROM LS_ODS.Courses c
			)
		DELETE cteCourse
		WHERE cteCourse.RowNumber > 1;

		EXEC LS_ODS.AddODSLoadLog 'Course Duplicate Check And Deletion Complete', 0;

		WITH cteAssignment
		AS (
			SELECT a.AssignmentPrimaryKey, a.CoursePrimaryKey, a.WeekNumber, a.AssignmentTitle, a.DueDate, a.PossiblePoints, a.DateTimeCreated, a.DateTimeModified, a.ScoreProviderHandle, a.ActiveFlag, a.UMADateTimeAdded, a.CourseContentsPrimaryKey1, a.AlternateTitle, a.IsReportable, a.CountsAsSubmission, a.AssignmentType, a.SourceSystem, ROW_NUMBER() OVER (
					PARTITION BY a.AssignmentPrimaryKey, a.CoursePrimaryKey, a.WeekNumber, a.AssignmentTitle, a.DueDate, a.PossiblePoints, a.DateTimeCreated, a.DateTimeModified, a.ScoreProviderHandle, a.ActiveFlag, a.UMADateTimeAdded, a.CourseContentsPrimaryKey1, a.AlternateTitle, a.IsReportable, a.CountsAsSubmission, a.AssignmentType, a.SourceSystem ORDER BY a.UMADateTimeAdded
					) 'RowNumber'
			FROM LS_ODS.Assignments a
			)
		DELETE
		FROM cteAssignment
		WHERE RowNumber > 1;

		EXEC LS_ODS.AddODSLoadLog 'Assignment Duplicate Check And Deletion Complete', 0;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashActiveFlagLargeIncludeDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashAssignmentPrimaryKeyDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_LSiDashCourseUsersPrimaryKeyDESC'
				)
		BEGIN
			DROP INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_CourseUsersPKAssignPKActiveFG'
				)
		BEGIN
			DROP INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = '_dta_index_Grades_10_1223675407__K2_K23_K21_4'
				)
		BEGIN
			DROP INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades;
		END;

		IF EXISTS (
				SELECT 1
				FROM sys.indexes
				WHERE name = 'idx_ODS_010'
				)
		BEGIN
			DROP INDEX idx_ODS_010 ON LS_ODS.Grades;
		END;

		WITH cteGrade
		AS (
			SELECT g.GradePrimaryKey, g.CourseUsersPrimaryKey, g.RowStatus, g.HighestScore, g.HighestGrade, g.HighestAttemptDateTime, g.ManualScore, g.ManualGrade, g.ManualDateTime, g.ExemptIndicator, g.HighestDateTimeCreated, g.HighestDateTimeModified, g.HighestIsLatestAttemptIndicator, g.NumberOfAttempts, g.FirstScore, g.FirstGrade, g.FirstAttemptDateTime, g.FirstIsLatestAttemptIndicator, g.FirstDateTimeCreated, g.FirstDateTimeModified, g.AssignmentPrimaryKey, g.AssignmentStatus, g.ActiveFlag, g.UMADateTimeAdded, g.SourceSystem, ROW_NUMBER() OVER (
					PARTITION BY g.GradePrimaryKey, g.CourseUsersPrimaryKey, g.RowStatus, g.HighestScore, g.HighestGrade, g.HighestAttemptDateTime, g.ManualScore, g.ManualGrade, g.ManualDateTime, g.ExemptIndicator, g.HighestDateTimeCreated, g.HighestDateTimeModified, g.HighestIsLatestAttemptIndicator, g.NumberOfAttempts, g.FirstScore, g.FirstGrade, g.FirstAttemptDateTime, g.FirstIsLatestAttemptIndicator, g.FirstDateTimeCreated, g.FirstDateTimeModified, g.AssignmentPrimaryKey, g.AssignmentStatus, g.ActiveFlag, g.UMADateTimeAdded, g.SourceSystem ORDER BY g.UMADateTimeAdded
					) 'RowNumber'
			FROM LS_ODS.Grades g
			)
		DELETE
		FROM cteGrade
		WHERE RowNumber > 1;

		--Set Active Flag For All Grades Records To Active Flag = 0 For Duplicate Check 
		UPDATE LS_ODS.Grades
		SET ActiveFlag = 0;

		--Update The Most Recent Grade Record To Have Active Flag = 1 
		WITH cteMaxDates (CourseUsersPrimaryKey, AssignmentPrimaryKey, MaxDate)
		AS (
			SELECT g.CourseUsersPrimaryKey, g.AssignmentPrimaryKey, MAX(g.UMADateTimeAdded) 'MaxDate'
			FROM LS_ODS.Grades g
			GROUP BY g.CourseUsersPrimaryKey, g.AssignmentPrimaryKey
			)
		UPDATE g
		SET g.ActiveFlag = 1
		FROM LS_ODS.Grades g
		INNER JOIN cteMaxDates md ON g.CourseUsersPrimaryKey = md.CourseUsersPrimaryKey
			AND g.AssignmentPrimaryKey = md.AssignmentPrimaryKey
			AND g.UMADateTimeAdded = md.MaxDate;

		DROP TABLE

		IF EXISTS #LSODSGRADE1
			SELECT *
			INTO #LSODSGRADE1
			FROM LS_ODS.Grades
			WHERE ActiveFlag = 0

		DELETE
		FROM #LSODSGRADE1
		WHERE ODSPrimaryKey IN (
				SELECT ODSPrimaryKey
				FROM Audit.Grades
				)

		MERGE INTO audit.Grades AS trg
		USING #LSODSGRADE1 AS src
			ON src.GradePrimaryKey = trg.GradePrimaryKey
				AND src.CourseUsersPrimaryKey = trg.CourseUsersPrimaryKey
				AND src.HighestScore = trg.HighestScore
				AND src.HighestGrade = trg.HighestGrade
				AND src.AssignmentPrimaryKey = trg.AssignmentPrimaryKey
				AND src.RowStatus = trg.RowStatus
				AND src.AssignmentStatus = trg.AssignmentStatus
		WHEN NOT MATCHED BY TARGET
			THEN
				INSERT ([GradePrimaryKey], [CourseUsersPrimaryKey], [RowStatus], [HighestScore], [HighestGrade], [HighestAttemptDateTime], [ManualScore], [ManualGrade], [ManualDateTime], [ExemptIndicator], [HighestDateTimeCreated], [HighestDateTimeModified], [HighestIsLatestAttemptIndicator], [NumberOfAttempts], [FirstScore], [FirstGrade], [FirstAttemptDateTime], [FirstIsLatestAttemptIndicator], [FirstDateTimeCreated], [FirstDateTimeModified], [AssignmentPrimaryKey], [AssignmentStatus], [ActiveFlag], [UMADateTimeAdded], [ODSPrimaryKey], SourceSystem)
				VALUES (src.[GradePrimaryKey], src.[CourseUsersPrimaryKey], src.[RowStatus], src.[HighestScore], src.[HighestGrade], src.[HighestAttemptDateTime], src.[ManualScore], src.[ManualGrade], src.[ManualDateTime], src.[ExemptIndicator], src.[HighestDateTimeCreated], src.[HighestDateTimeModified], src.[HighestIsLatestAttemptIndicator], src.[NumberOfAttempts], src.[FirstScore], src.[FirstGrade], src.[FirstAttemptDateTime], src.[FirstIsLatestAttemptIndicator], src.[FirstDateTimeCreated], src.[FirstDateTimeModified], src.[AssignmentPrimaryKey], src.[AssignmentStatus], src.[ActiveFlag], src.[UMADateTimeAdded], src.[ODSPrimaryKey], src.SourceSystem)
		WHEN MATCHED
			THEN
				UPDATE
				SET trg.[GradePrimaryKey] = src.[GradePrimaryKey], trg.[CourseUsersPrimaryKey] = src.[CourseUsersPrimaryKey], trg.[RowStatus] = src.[RowStatus], trg.[HighestScore] = src.[HighestScore], trg.[HighestGrade] = src.[HighestGrade], trg.[HighestAttemptDateTime] = src.[HighestAttemptDateTime], trg.[ManualScore] = src.[ManualScore], trg.[ManualGrade] = src.[ManualGrade], trg.[ManualDateTime] = src.[ManualDateTime], trg.[ExemptIndicator] = src.[ExemptIndicator], trg.[HighestDateTimeCreated] = src.[HighestDateTimeCreated], trg.[HighestDateTimeModified] = src.[HighestDateTimeModified], trg.[HighestIsLatestAttemptIndicator] = src.[HighestIsLatestAttemptIndicator], trg.[NumberOfAttempts] = src.[NumberOfAttempts], trg.[FirstScore] = src.[FirstScore], trg.[FirstGrade] = src.[FirstGrade], trg.[FirstAttemptDateTime] = src.[FirstAttemptDateTime], trg.[FirstIsLatestAttemptIndicator] = src.[FirstIsLatestAttemptIndicator], trg.[FirstDateTimeCreated] = src.[FirstDateTimeCreated], trg.[FirstDateTimeModified] = src.[FirstDateTimeModified], trg.
					[AssignmentPrimaryKey] = src.[AssignmentPrimaryKey], trg.[AssignmentStatus] = src.[AssignmentStatus], trg.[ActiveFlag] = src.[ActiveFlag], trg.[UMADateTimeAdded] = src.[UMADateTimeAdded], trg.[ODSPrimaryKey] = src.[ODSPrimaryKey], trg.SourceSystem = src.SourceSystem;

		--Delete Anything Left With Active Flag = 0 
		DELETE
		FROM LS_ODS.Grades
		WHERE ActiveFlag = 0;

		CREATE NONCLUSTERED INDEX idx_ODS_010 ON LS_ODS.Grades (GradePrimaryKey ASC, ActiveFlag ASC) INCLUDE (CourseUsersPrimaryKey, RowStatus, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, ManualDateTime, ExemptIndicator, HighestDateTimeCreated, HighestDateTimeModified, HighestIsLatestAttemptIndicator, NumberOfAttempts, FirstScore, FirstGrade, FirstAttemptDateTime, FirstIsLatestAttemptIndicator, FirstDateTimeModified, AssignmentPrimaryKey, AssignmentStatus, SourceSystem)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX _dta_index_Grades_10_1223675407__K2_K23_K21_4 ON LS_ODS.Grades (CourseUsersPrimaryKey ASC, ActiveFlag ASC, AssignmentPrimaryKey ASC) INCLUDE (HighestScore)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashCourseUsersPrimaryKeyDESC ON LS_ODS.Grades (CourseUsersPrimaryKey DESC) INCLUDE (GradePrimaryKey, AssignmentPrimaryKey, ActiveFlag, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashCourseuserspkAssignmentpkActiveflagDESC ON LS_ODS.Grades (CourseUsersPrimaryKey DESC, AssignmentPrimaryKey DESC, ActiveFlag DESC) INCLUDE (GradePrimaryKey, HighestScore, HighestDateTimeCreated, AssignmentStatus)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashAssignmentPrimaryKeyDESC ON LS_ODS.Grades (AssignmentPrimaryKey DESC) INCLUDE (GradePrimaryKey, CourseUsersPrimaryKey, ActiveFlag, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_LSiDashActiveFlagLargeIncludeDESC ON LS_ODS.Grades (ActiveFlag DESC) INCLUDE (GradePrimaryKey, AssignmentPrimaryKey, CourseUsersPrimaryKey, AssignmentStatus, HighestGrade, HighestScore, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95) ON [PRIMARY];

		CREATE NONCLUSTERED INDEX idx_CourseUsersPKAssignPKActiveFG ON LS_ODS.Grades (CourseUsersPrimaryKey ASC, AssignmentPrimaryKey ASC, ActiveFlag ASC) INCLUDE (GradePrimaryKey, HighestScore, HighestGrade, HighestAttemptDateTime, ManualScore, ManualGrade, HighestDateTimeCreated, NumberOfAttempts, AssignmentStatus)
			WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

		EXEC LS_ODS.AddODSLoadLog 'Grade Duplicate Check And Deletion Complete', 0;

		--**************************************************************************************************************************************** 
		--Remove Orphaned Student Records - These are students who were in course X, started course Y then received a failing grade for course X. 
		--	The student is removed from course Y and re-enrolled in another course X.  If the student had no activity in the course Y to turn 
		--	them Active in that course, the course enrollment record in CampusVue is removed.  This leaves the record for course Y orphaned in our 
		--	data set.  This proces will move those records to the Archive table with a IsOrphanRecord flag set to true. 
		--**************************************************************************************************************************************** 
		INSERT INTO Archive.Students (
			StudentPrimaryKey, DateTimeCreated, DateTimeModified, RowStatus, BatchUniqueIdentifier, BlackboardUsername, SyStudentId, FirstName, LastName, Campus, AdEnrollSchedId, AdClassSchedId, CourseUsersPrimaryKey, LastLoginDateTime, TimeInClass, LastI3InteractionNumberMainPhone, LastI3InteractionDateTimeMainPhone, DaysSinceLastI3InteractionMainPhone, LastI3InteractionNumberWorkPhone, LastI3InteractionDateTimeWorkPhone, DaysSinceLastI3InteractionWorkPhone, LastI3InteractionNumberMobilePhone, LastI3InteractionDateTimeMobilePhone, DaysSinceLastI3InteractionMobilePhone, LastI3InteractionNumberOtherPhone, LastI3InteractionDateTimeOtherPhone, DaysSinceLastI3InteractionOtherPhone, Week1Grade, Week2Grade, Week3Grade, Week4Grade, Week5Grade, SelfTestsCount, AssessmentsCount, AssignmentsCount, DiscussionsCount, Week1CompletionRate, Week2CompletionRate, Week3CompletionRate, Week4CompletionRate, Week5CompletionRate, ActiveFlag, UMADateTimeAdded, VAStudent, NoticeName, NoticeDueDate, VABenefitName, ClassStatus, ODSPrimaryKey, Week1LDA, 
			Week2LDA, Week3LDA, Week4LDA, Week5LDA, Week1CompletedAssignments, Week2CompletedAssignments, Week3CompletedAssignments, Week4CompletedAssignments, Week5CompletedAssignments, CoursePercentage, TotalWorkPercentage, AdEnrollId, IsRetake, StudentCourseUserKeys, CurrentCourseGrade, ProgramCode, ProgramName, ProgramVersionCode, ProgramVersionName, MondayTimeInClass, TuesdayTimeInClass, WednesdayTimeInClass, ThursdayTimeInClass, FridayTimeInClass, SaturdayTimeInClass, SundayTimeInClass, Week1CompletionRateFixed, Week2CompletionRateFixed, Week3CompletionRateFixed, Week4CompletionRateFixed, Week5CompletionRateFixed, StudentNumber, IsOrphanRecord, SourceSystem
			)
		SELECT s.StudentPrimaryKey, s.DateTimeCreated, s.DateTimeModified, s.RowStatus, s.BatchUniqueIdentifier, s.BlackboardUsername, s.SyStudentId, s.FirstName, s.LastName, s.Campus, s.AdEnrollSchedId, s.AdClassSchedId, s.CourseUsersPrimaryKey, s.LastLoginDateTime, s.TimeInClass, s.LastI3InteractionNumberMainPhone, s.LastI3InteractionDateTimeMainPhone, s.DaysSinceLastI3InteractionMainPhone, s.LastI3InteractionNumberWorkPhone, s.LastI3InteractionDateTimeWorkPhone, s.DaysSinceLastI3InteractionWorkPhone, s.LastI3InteractionNumberMobilePhone, s.LastI3InteractionDateTimeMobilePhone, s.DaysSinceLastI3InteractionMobilePhone, s.LastI3InteractionNumberOtherPhone, s.LastI3InteractionDateTimeOtherPhone, s.DaysSinceLastI3InteractionOtherPhone, s.Week1Grade, s.Week2Grade, s.Week3Grade, s.Week4Grade, s.Week5Grade, s.SelfTestsCount, s.AssessmentsCount, s.AssignmentsCount, s.DiscussionsCount, s.Week1CompletionRate, s.Week2CompletionRate, s.Week3CompletionRate, s.Week4CompletionRate, s.Week5CompletionRate, s.ActiveFlag, s.UMADateTimeAdded, s.
			VAStudent, s.NoticeName, s.NoticeDueDate, s.VABenefitName, s.ClassStatus, s.ODSPrimaryKey, s.Week1LDA, s.Week2LDA, s.Week3LDA, s.Week4LDA, s.Week5LDA, s.Week1CompletedAssignments, s.Week2CompletedAssignments, s.Week3CompletedAssignments, s.Week4CompletedAssignments, s.Week5CompletedAssignments, s.CoursePercentage, s.TotalWorkPercentage, s.AdEnrollId, s.IsRetake, s.StudentCourseUserKeys, s.CurrentCourseGrade, s.ProgramCode, s.ProgramName, s.ProgramVersionCode, s.ProgramVersionName, s.MondayTimeInClass, s.TuesdayTimeInClass, s.WednesdayTimeInClass, s.ThursdayTimeInClass, s.FridayTimeInClass, s.SaturdayTimeInClass, s.SundayTimeInClass, s.Week1CompletionRateFixed, s.Week2CompletionRateFixed, s.Week3CompletionRateFixed, s.Week4CompletionRateFixed, s.Week5CompletionRateFixed, s.StudentNumber, 1, s.SourceSystem
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
		DECLARE @BeginRangeDate DATE;
		DECLARE @EndRangeDate DATE;
		DECLARE @TodaysDate DATE;

		SET @BeginRangeDate = '2012-04-01';
		SET @EndRangeDate = DATEADD(DAY, - 1, GETDATE());
		SET @TodaysDate = GETDATE();

		TRUNCATE TABLE LS_ODS.LDACounts;

		DECLARE @AllDates TABLE (TheDate DATE);

		WITH cteAllDates (TheDate)
		AS (
			SELECT @BeginRangeDate AS TheDate
			
			UNION ALL
			
			SELECT DATEADD(DAY, 1, TheDate)
			FROM cteAllDates
			WHERE TheDate < @EndRangeDate
			)
		INSERT INTO @AllDates (TheDate)
		SELECT ad.TheDate
		FROM cteAllDates ad
		OPTION (MAXRECURSION 5000);

		DECLARE @HolidayCounter TABLE (TheDate DATE, IsSchoolDay INT);

		WITH cteHolidays (HolidayDate)
		AS (
			SELECT da.[Date] 'HolidayDate'
			FROM (
				SELECT ca.StartDate, ca.EndDate
				FROM CV_Prod.dbo.AdCalendar ca
				INNER JOIN CV_Prod.dbo.SyCampusList cl ON ca.SyCampusGrpID = cl.SyCampusGrpID
					AND cl.SyCampusID = 9
					--WHERE ca.AdShiftID = 11		--Online Only 
				) AS dr
			INNER JOIN [master]..spt_values va ON va.number BETWEEN 0
					AND DATEDIFF(DAY, dr.StartDate, dr.EndDate)
				AND va.[type] = 'P'
			CROSS APPLY (
				SELECT DATEADD(DAY, va.number, dr.StartDate)
				) AS da([Date])
			)
		INSERT INTO @HolidayCounter (TheDate, IsSchoolDay)
		SELECT ad.TheDate, CASE 
				WHEN ho.HolidayDate IS NULL
					THEN 1
				ELSE 0
				END 'IsSchoolDay'
		FROM @AllDates ad
		LEFT JOIN cteHolidays ho ON ad.TheDate = ho.HolidayDate;

		WITH cteHolidayCounts (TheDate, HolidayCounter)
		AS (
			SELECT ad.TheDate, SUM(CASE 
						WHEN cd.IsSchoolDay = 1
							THEN 0
						ELSE 1
						END) 'HolidayCounter'
			FROM @HolidayCounter ad
			INNER JOIN @HolidayCounter cd ON ad.TheDate <= cd.TheDate
			GROUP BY ad.TheDate
				--ORDER BY 
				--	ad.TheDate 
			)
		INSERT INTO LS_ODS.LDACounts (TheDate, IsSchoolDay, HolidayCounter, LDACount, LDACountMinusHolidayCounter, LDACountMinusHolidayCounterAddHolidayDate)
		SELECT ad.TheDate, ho.IsSchoolDay, hc.HolidayCounter, DATEDIFF(DAY, ad.TheDate, @TodaysDate) 'LDACount', DATEDIFF(DAY, ad.TheDate, @TodaysDate) - hc.HolidayCounter 'LDACountMinusHolidayCounter', DATEDIFF(DAY, ad.TheDate, @TodaysDate) - hc.HolidayCounter + CASE 
				WHEN ho.IsSchoolDay = 0
					THEN 1
				ELSE 0
				END 'LDACountMinusHolidayCounterAddHolidayDate'
		FROM @AllDates ad
		INNER JOIN @HolidayCounter ho ON ad.TheDate = ho.TheDate
		INNER JOIN cteHolidayCounts hc ON ad.TheDate = hc.TheDate;

		DELETE
		FROM [COL-CVU-P-SQ01].FREEDOM.LMS_Data.LDACounts;--PROD 
			--DELETE FROM [MLK-CVU-D-SQ01].FREEDOM.LMS_Data.LDACounts;																				--DEV 

		INSERT INTO [COL-CVU-P-SQ01].FREEDOM.LMS_Data.LDACounts --PROD 
			--INSERT INTO [MLK-CVU-D-SQ01].FREEDOM.LMS_Data.LDACounts																				--DEV 
			(TheDate, IsSchoolDay, HolidayCounter, LDACount, LDACountMinusHolidayCounter, LDACountMinusHolidayCounterAddHolidayDate)
		SELECT lc.TheDate, lc.IsSchoolDay, lc.HolidayCounter, lc.LDACount, lc.LDACountMinusHolidayCounter, lc.LDACountMinusHolidayCounterAddHolidayDate
		FROM LS_ODS.LDACounts lc;

		EXEC LS_ODS.AddODSLoadLog 'LDA Counts Calculation Complete', 0;

		--**************************************************************************************************************************************** 
		--Update the tables needed for iDash to reduce high number of logical reads 
		--**************************************************************************************************************************************** 
		--CourseWeeklyGradedActivity 
		TRUNCATE TABLE LS_ODS.CourseWeeklyGradedActivity;

		INSERT INTO LS_ODS.CourseWeeklyGradedActivity (StudentId, EnrollSchedId, ClassSchedId, CoursePrimaryKey, AssignmentPrimaryKey, GradePrimaryKey, WeekNumber, Assignment, Grade, [Status], DateTaken, Week1Grade, Week2Grade, Week3Grade, Week4Grade, Week5Grade, Attempts, DateOfLastAttempt, PossiblePoints)
		SELECT st.SyStudentId 'StudentId', st.AdEnrollSchedId 'EnrollSchedId', st.AdClassSchedId 'ClassSchedId', co.CoursePrimaryKey 'CoursePrimaryKey', asg.AssignmentPrimaryKey 'AssignmentPrimaryKey', gr.GradePrimaryKey 'GradePrimaryKey', asg.WeekNumber 'WeekNumber', asg.AssignmentTitle 'Assignment', CASE 
				WHEN asg.PossiblePoints IS NOT NULL
					AND asg.PossiblePoints > 0
					THEN CASE 
							WHEN gr.ManualScore IS NULL
								OR gr.ManualScore = 0.00
								THEN ((gr.HighestScore / asg.PossiblePoints) * 100)
							ELSE ((gr.ManualScore / asg.PossiblePoints) * 100)
							END
				ELSE 0
				END 'Grade', gr.AssignmentStatus 'Status', CONVERT(VARCHAR(10), gr.HighestDateTimeCreated, 101) 'DateTaken', st.Week1Grade 'Week1Grade', st.Week2Grade 'Week2Grade', st.Week3Grade 'Week3Grade', st.Week4Grade 'Week4Grade', st.Week5Grade 'Week5Grade', gr.NumberOfAttempts 'Attempts', gr.HighestAttemptDateTime 'DateOfLastAttempt', COALESCE(gr.ManualGrade, gr.HighestGrade, '0.00') + '/' + CAST(asg.PossiblePoints AS VARCHAR(4)) 'PointsPossible'
		FROM LS_ODS.Assignments asg
		INNER JOIN LS_ODS.Courses co ON asg.CoursePrimaryKey = co.CoursePrimaryKey
		INNER JOIN LS_ODS.Students st ON co.AdClassSchedId = st.AdClassSchedId
		LEFT JOIN LS_ODS.Grades gr ON asg.AssignmentPrimaryKey = gr.AssignmentPrimaryKey
			AND st.CourseUsersPrimaryKey = gr.CourseUsersPrimaryKey
		WHERE st.AdEnrollSchedId IS NOT NULL;

		EXEC LS_ODS.AddODSLoadLog 'Processed Course Weekly Graded Activity', 0;

		--CourseWeeklyGrades 
		TRUNCATE TABLE LS_ODS.CourseWeeklyGrades;

		DECLARE @Instructors TABLE (AdClassSchedId INT, AcademicFacilitator VARCHAR(50), CoInstructor VARCHAR(50));

		INSERT INTO @Instructors (AdClassSchedId)
		SELECT DISTINCT ins.AdClassSchedId
		FROM iDash.Instructors_vw ins;

		WITH cteAcademicFacilitator (AdClassSchedId, AcademicFacilitator, RowNumber)
		AS (
			SELECT ins.AdClassSchedId, ins.InstructorName, ROW_NUMBER() OVER (
					PARTITION BY ins.AdClassSchedId ORDER BY ins.DisplayOrder
					) 'RowNumber'
			FROM iDash.Instructors_vw ins
			WHERE ins.InstructorTypeCode = 'SECONDARY'
			)
		UPDATE ins
		SET ins.AcademicFacilitator = af.AcademicFacilitator
		FROM @Instructors ins
		INNER JOIN cteAcademicFacilitator af ON ins.AdClassSchedId = af.AdClassSchedId
			AND af.RowNumber = 1;

		WITH cteCoInstructor (AdClassSchedId, CoInstructor, RowNumber)
		AS (
			SELECT ins.AdClassSchedId, ins.InstructorName, ROW_NUMBER() OVER (
					PARTITION BY ins.AdClassSchedId ORDER BY ins.DisplayOrder
					) 'RowNumber'
			FROM iDash.Instructors_vw ins
			WHERE ins.InstructorTypeCode = 'COINSTR'
			)
		UPDATE ins
		SET ins.CoInstructor = ci.CoInstructor
		FROM @Instructors ins
		INNER JOIN cteCoInstructor ci ON ins.AdClassSchedId = ci.AdClassSchedId
			AND ci.RowNumber = 1;

		INSERT INTO LS_ODS.CourseWeeklyGrades (StudentId, EnrollSchedId, AdClassSchedID, Week1Dates, Week2Dates, Week3Dates, Week4Dates, Week5Dates, Week1Grade, Week2Grade, Week3Grade, Week4Grade, Week5Grade, Week1SubRate, Week2SubRate, Week3SubRate, Week4SubRate, Week5SubRate, CurrentNumericGrade, ClassTime, SelfTestCount, AssessmentCount, AssignmentCount, DiscussionCount, ActivityCount, CurrentCourseLetterGrade, CourseSubmissionRate, AcademicFacilitator, CoInstructor)
		SELECT st.SyStudentId 'StudentId', st.AdEnrollSchedId 'EnrollSchedId', st.AdClassSchedId 'AdClassSchedID', CONVERT(VARCHAR(5), co.Week1StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week1StartDate), 101) 'Week1Dates', CONVERT(VARCHAR(5), co.Week2StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week2StartDate), 101) 'Week2Dates', CONVERT(VARCHAR(5), co.Week3StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week3StartDate), 101) 'Week3Dates', CONVERT(VARCHAR(5), co.Week4StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week4StartDate), 101) 'Week4Dates', CONVERT(VARCHAR(5), co.Week5StartDate, 101) + ' - ' + CONVERT(VARCHAR(5), DATEADD(DAY, 6, co.Week5StartDate), 101) 'Week5Dates', st.Week1Grade * 100 'Week1Grade', st.Week2Grade * 100 'Week2Grade', st.Week3Grade * 100 'Week3Grade', st.Week4Grade * 100 'Week4Grade', st.Week5Grade * 100 'Week5Grade', st.Week1CompletionRate * 100 'Week1SubRate', st.Week2CompletionRate * 100 'Week2SubRate', st.Week3CompletionRate * 100 'Week3SubRate', st.Week4CompletionRate * 100 
			'Week4SubRate', st.Week5CompletionRate * 100 'Week5SubRate', st.Week5Grade * 100 'CurrentNumericGrade', st.TimeInClass 'ClassTime', st.SelfTestsCount 'SelfTestCount', st.AssessmentsCount 'AssessmentCount', st.AssignmentsCount 'AssignmentCount', st.DiscussionsCount 'DiscussionCount', st.ActivitiesCount 'ActivityCount', CASE 
				WHEN (st.Week5Grade * 100) >= 90
					THEN 'A'
				WHEN (st.Week5Grade * 100) >= 80
					THEN 'B'
				WHEN (st.Week5Grade * 100) >= 70
					THEN 'C'
				WHEN (st.Week5Grade * 100) >= 60
					THEN 'D'
				WHEN (st.Week5Grade * 100) < 60
					THEN 'F'
				END 'CurrentCourseLetterGrade', st.CoursePercentage * 100 'CourseSubmissionRate', ins.AcademicFacilitator 'AcademicFacilitator', ins.CoInstructor 'CoInstructor'
		FROM LS_ODS.Students st
		LEFT JOIN LS_ODS.Courses co ON st.AdClassSchedId = co.AdClassSchedId
		LEFT JOIN @Instructors ins ON co.AdClassSchedId = ins.AdClassSchedId
		WHERE st.AdEnrollSchedId IS NOT NULL;

		EXEC LS_ODS.AddODSLoadLog 'Processed Course Weekly Grades', 0;

		--Wait a short time to ensure the data is all written before report generation starts 
		WAITFOR DELAY '00:01';

		/*Send ODS email after steo number 54 */
		EXECUTE LS_ODS.ODS_Email_2

		--**************************************************************************************************************************************** 
		--Process the ActiveSubmissionSummary table 
		--**************************************************************************************************************************************** 
		EXEC LS_ODS.ProcessActiveSubmissionSummary;

		EXEC LS_ODS.AddODSLoadLog 'Active Submission Summary Procesing Complete', 0;

		--**************************************************************************************************************************************** 
		--Process the Total Course Points Earned table 
		--**************************************************************************************************************************************** 
		EXEC LS_ODS.UpsertTotalCoursePointsEarned;

		EXEC LS_ODS.AddODSLoadLog 'Total Course Points Earned Procesing Complete', 0;

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

		EXEC LS_ODS.AddODSLoadLog 'Program Certification Tables Update Complete', 0;

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
		EXEC [MLK-REP-P-SQ02].msdb.dbo.sp_start_job N'E5401A80-B99C-4840-83DE-57DDFDCD6C81';--2016 Server

		--Execute VA Report - New Policy ssrs report 
		--EXEC [MLK-SSR-P-SQ01].msdb.dbo.sp_start_job N'B640B3D8-41EA-45C7-A605-6490D8643B0A';  --2008 Server
		EXEC [MLK-REP-P-SQ02].msdb.dbo.sp_start_job N'F98F0617-E4F1-4F1F-A384-B6EE78BA9EF5';--2016 Server

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

		SET @countofrecords = (
				SELECT COUNT(*)
				FROM Stage.ODS_Duplicates
				WHERE PROCCESED_ON = CONVERT(DATE, GETDATE())
				)

		IF @countofrecords > 0
		BEGIN
			DECLARE @tableHTML NVARCHAR(MAX) = N'';

			SELECT @tableHTML += N'<tr><td>' + CAST(PrimaryKey AS NVARCHAR(10)) + N'</td><td>' + STEP_FAILED_ON + N'</td></tr>'
			FROM Stage.ODS_Duplicates
			WHERE PROCCESED_ON = CONVERT(DATE, GETDATE());

			--SET @tableHTML = N'<html><body><p>Dear Team ,</p>';
			--SET @tableHTML+=N'<html><body><p>Please review the duplicates found in todays ODS process ,</p>'
			SET @tableHTML = N'<table border="1"><tr><th>ID</th><th>Name</th></tr>' + @tableHTML + N'</table>';

			EXEC msdb.dbo.sp_send_dbmail @profile_name = 'EDM_DB_ALERT', @recipients = 'ppoonati@ultimatemedical.edu', @subject = 'Duplicate records found in todays ODS Run ', @body = @tableHTML, @body_format = 'HTML';
		END
	END TRY

	--		 --**************************************************************************************************************************************** 
	--	--Catch block, send email incase of ODS failure
	--	--**************************************************************************************************************************************** 
	BEGIN CATCH
		DROP TABLE

		IF EXISTS #tempmail
			CREATE TABLE #tempmail (EventDetails VARCHAR(240), EventDateTime DATETIME);

		DECLARE @emailsubject NVARCHAR(240);
		DECLARE @html_body NVARCHAR(MAX);
		DECLARE @ERRORBODY NVARCHAR(MAX) = 'Error message: ' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR) + CHAR(13) + CHAR(10) + 'Error severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR) + CHAR(13) + CHAR(10) + 'Error state: ' + CAST(ERROR_STATE() AS NVARCHAR) + CHAR(13) + CHAR(10) + 'Error procedure: ' + COALESCE(ERROR_PROCEDURE(), 'N/A') + CHAR(13) + CHAR(10) + 'Error line number: ' + CAST(ERROR_LINE() AS NVARCHAR) + CHAR(13) + CHAR(10);

		SET @html_body = N'<html><body><p>Dear Team ,</p>';
		SET @html_body += N'<html><body><p>ODS failed due to below error ,</p>'
		SET @html_body += @ERRORBODY
		SET @html_body += N'<p>Here are the steps that have been processed today:</p>';
		SET @emailsubject = 'ODS Process failure-' + CONVERT(VARCHAR(50), DATEADD(DAY, 0, DATEDIFF(DAY, 0, GETDATE())), 101);

		-- Execute the SQL statement and insert the results as a table in the HTML body
		DECLARE @table_html NVARCHAR(MAX);

		SET @table_html = N'<table><thead><tr><th>EventDetails</th><th>EventDateTime</th></tr></thead><tbody>';

		INSERT INTO #tempmail (EventDetails, EventDateTime)
		SELECT EventDetails, EventDateTime
		FROM LS_ODS.ODSLoadLog
		WHERE CONVERT(DATE, EventDateTime) = CONVERT(DATE, GETDATE());

		SELECT @table_html += N'<tr><td>' + EventDetails + N'</td><td>' + CONVERT(VARCHAR, EventDateTime) + N'</td></tr>'
		FROM #tempmail;

		SET @table_html += N'</tbody></table>';
		-- Add the table to the HTML body and close the HTML tags
		SET @html_body += @table_html + N'<p>Best regards,</p><p>EDM TEAM </p></body></html>';

		-- Send the email
		EXEC msdb.dbo.sp_send_dbmail @profile_name = 'EDM_DB_ALERT', @recipients = 'edmteam@ultimatemedical.edu', @subject = @emailsubject, @body = @html_body, @body_format = 'HTML';

		DECLARE @errorMessage VARCHAR(4000)
		DECLARE @procName VARCHAR(255)

		SELECT @errorMessage = error_message()

		SELECT @procName = OBJECT_NAME(@@PROCID)

		SELECT @procName

		RAISERROR ('%sODS failed due to %s', 16, 1, @procName, @errorMessage)
	END CATCH
END;