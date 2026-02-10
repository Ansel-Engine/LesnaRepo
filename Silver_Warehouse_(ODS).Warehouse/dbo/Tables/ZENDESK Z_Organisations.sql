CREATE TABLE [dbo].[ZENDESK Z_Organisations] (

	[FK_organizations] bigint NULL, 
	[id] bigint NULL, 
	[name] varchar(8000) NULL, 
	[is_shared] varchar(8000) NULL, 
	[created_at] datetime2(6) NULL, 
	[updated_at] datetime2(6) NULL, 
	[group_id] varchar(8000) NULL, 
	[details] varchar(8000) NULL, 
	[notes] varchar(8000) NULL, 
	[external_id] varchar(8000) NULL, 
	[current_tags] varchar(8000) NULL
);