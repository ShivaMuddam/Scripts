CREATE TABLE tempdb.dbo.Loginfo_Temp
    (
      FileID SQL_VARIANT NULL ,
      Filesize SQL_VARIANT NULL ,
      startoffset SQL_VARIANT NULL ,
      FseqNO SQL_VARIANT NULL ,
      Status SQL_VARIANT NULL ,
      Parity SQL_VARIANT NULL ,
      CreateLSN SQL_VARIANT NULL
    )

CREATE TABLE tempdb.dbo.Loginfo
    (
      DatabaseID INT ,
      FileID SQL_VARIANT NULL ,
      Filesize SQL_VARIANT NULL ,
      startoffset SQL_VARIANT NULL ,
      FseqNO SQL_VARIANT NULL ,
      Status SQL_VARIANT NULL ,
      Parity SQL_VARIANT NULL ,
      CreateLSN SQL_VARIANT NULL
    )

DECLARE @sqlcmd VARCHAR(4000)
DECLARE @Databaseid INT

DECLARE loginfo CURSOR
FOR
    SELECT  'use [' + Name
            + '];insert into tempdb.dbo.Loginfo_Temp (FileID, Filesize, startoffset, FseqNO, Status, Parity, CreateLSN) exec (''dbcc loginfo'')' AS execcmd ,
            database_id
    FROM    sys.databases
    WHERE   ( State = 0 )
    ORDER BY name

OPEN loginfo

FETCH NEXT FROM loginfo
INTO @sqlcmd, @Databaseid

WHILE @@FETCH_STATUS = 0 
    BEGIN

        EXEC(@sqlcmd)

        INSERT  INTO tempdb.dbo.Loginfo
                ( DatabaseID ,
                  FileID ,
                  Filesize ,
                  startoffset ,
                  FseqNO ,
                  Status ,
                  Parity ,
                  CreateLSN
                )
                SELECT  @Databaseid ,
                        FileID ,
                        Filesize ,
                        startoffset ,
                        FseqNO ,
                        Status ,
                        Parity ,
                        CreateLSN
                FROM    tempdb.dbo.Loginfo_Temp

        TRUNCATE TABLE tempdb.dbo.Loginfo_Temp

        FETCH NEXT FROM loginfo
INTO @sqlcmd, @Databaseid

    END

CLOSE loginfo
DEALLOCATE loginfo

SELECT  DatabaseID ,
        DB_NAME(DatabaseID) AS DbName ,
        FileID ,
        COUNT(*) AS NumOfVLFS
FROM    tempdb.dbo.Loginfo
GROUP BY DatabaseID ,
        DB_NAME(DatabaseID) ,
        FileID
ORDER BY DatabaseID

DROP TABLE tempdb.dbo.Loginfo
DROP TABLE tempdb.dbo.Loginfo_Temp

--BACKUP LOG THR_CERT_MPI TO DISK='d:\adtlog.bak'

---- Get Logical file number of the log file
--sp_helpfile 

--select * from sys.master_files where database_id=db_id()

--DBCC SHRINKFILE(2,TRUNCATEONLY)

----Get the logical name from name column in sp_helpfile.
--ALTER DATABASE THR_CERT_MPI
--MODIFY FILE
--(NAME =THR_CERT_MPI_log,SIZE = 1GB)

--DBCC LOGINFO
