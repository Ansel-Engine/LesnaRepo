CREATE TABLE [dbo].[jobstatus1] (
    [statusid]       INT            NULL,
    [pipelinename]   NVARCHAR (50)  NULL,
    [rundate]        DATETIME2 (6)  NULL,
    [runid]          NVARCHAR (500) NULL,
    [pipelinestatus] NVARCHAR (50)  NULL,
    [pipelineid]     NVARCHAR (500) NULL,
    [errorMessage]   NVARCHAR (MAX) NULL,
    [errorcode]      NVARCHAR (20)  NULL
);


GO

