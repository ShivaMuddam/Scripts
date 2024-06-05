USE MASTER;
GO
CREATE DATABASE DBA
ON 
( NAME = DBA,
    FILENAME = 'E:\SQLDATA\DBA.mdf',
     FILEGROWTH = 256MB,
     MAXSIZE = Unlimited)
LOG ON
( NAME = DBA_log,
    FILENAME = 'F:\SQLDATA\DBA_log.ldf',
     SIZE = 256MB,
     FILEGROWTH = 128MB,
     MAXSIZE = Unlimited) ;
GO