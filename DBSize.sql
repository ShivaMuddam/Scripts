SELECT  NAME ,
        physical_name ,
        CAST(( CAST(SIZE * 8 AS FLOAT) ) / 1024 AS VARCHAR) + ' MB' AS [SIZE]
FROM    SYS.MASTER_FILES
ORDER BY size DESC

