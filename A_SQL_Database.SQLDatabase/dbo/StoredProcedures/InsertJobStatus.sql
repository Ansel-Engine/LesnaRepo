CREATE PROCEDURE [dbo].[InsertJobStatus]
@pipelinename VARCHAR(50),
@rundate DATETIME2,
@runid varchar(500),
@pipelinestatus VARCHAR(50),
@pipelineid varchar(500),
@errormessge varchar(max),
@errorcode varchar(20)
AS
BEGIN
INSERT INTO dbo.jobstatus (pipelinename, rundate, runid, pipelinestatus, pipelineid, errorMessage, errorcode) VALUES (@pipelinename, @rundate, @runid, @pipelinestatus, @pipelineid, @errormessge, @errorcode);
END;

GO

