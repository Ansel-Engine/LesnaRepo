CREATE TABLE [dbo].[jobstatus] (
    [statusid]       INT           IDENTITY (1, 1) NOT NULL,
    [pipelinename]   VARCHAR (50)  NULL,
    [rundate]        DATETIME2 (7) NULL,
    [runid]          VARCHAR (500) NULL,
    [pipelinestatus] VARCHAR (50)  NULL,
    [pipelineid]     VARCHAR (500) NULL,
    [errorMessage]   VARCHAR (MAX) NULL,
    [errorcode]      VARCHAR (20)  NULL,
    PRIMARY KEY CLUSTERED ([statusid] ASC)
);


GO

