if not exists (select name from sys.databases where name = N'DebtApps')
begin
    create database DebtApps
end
go  

use DebtApps
go  

if not exists (select * from sys.schemas where name = N'admin')
begin
    exec('create schema admin')
end
go  

if object_id('admin.Log_Run', 'U') is null
begin
  create table admin.Log_Run
  (
    RunID int identity(1,1) primary key,
    Run_Start_Time datetime not null,
    Run_End_Time datetime null
  );
end
go

if object_id('admin.Files', 'U') is null
begin
    create table admin.Files
    (
        ID int identity(1,1) primary key,
        RunID int not null,
        FileID binary(32) not null,
        FileName_orig nvarchar(255) not null,
        Source_Name nvarchar(50) not null,
        Status nvarchar(50) not null,
        Detected_at datetime null,
        Completion_detected_at datetime null,
        Processing_started_at datetime null,
        Processing_completed_at datetime null,
        Control_Strategy nvarchar(50) null,
        Control_File_Path nvarchar(255) null,
        Current_Path nvarchar(255) null,
        Processing_Path nvarchar(255) null,
        Final_Path nvarchar(255),
        constraint FK_Files_LogRun foreign key (RunID) references admin.Log_Run(RunID)
    );
end
go


