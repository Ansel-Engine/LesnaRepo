[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ZENDESK Z_Organisations_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "FK_organizations", DestinationColumnName = "FK_organizations"], [SourceColumnName = "id", DestinationColumnName = "id"], [SourceColumnName = "name", DestinationColumnName = "name"], [SourceColumnName = "is_shared", DestinationColumnName = "is_shared"], [SourceColumnName = "created_at", DestinationColumnName = "created_at"], [SourceColumnName = "updated_at", DestinationColumnName = "updated_at"], [SourceColumnName = "group_id", DestinationColumnName = "group_id"], [SourceColumnName = "details", DestinationColumnName = "details"], [SourceColumnName = "notes", DestinationColumnName = "notes"], [SourceColumnName = "external_id", DestinationColumnName = "external_id"], [SourceColumnName = "current_tags", DestinationColumnName = "current_tags"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"ZENDESK Z_Organisations" = let
  Source = Sql.Database("10.0.1.12", "LesnaShah"),
  #"Navigation 1" = Source{[Schema = "ZENDESK", Item = "Z_Organisations"]}[Data],
  #"Added custom" = Table.AddColumn(#"Navigation 1", "UploadDate", each Date.From(DateTime.LocalNow()))
in
  #"Added custom";
shared #"ZENDESK Z_Organisations_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "8bc6ea08-fd30-4ca7-be7f-dc76aee17419"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "4e274702-44ef-49d7-815c-5df523e0e4cf"]}[Data],
  TableNavigation = Navigation_2{[Item = "ZENDESK Z_Organisations", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
