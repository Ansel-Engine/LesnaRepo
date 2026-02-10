[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "vw_latest_spanishspeakingcountries_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "official_country_name", DestinationColumnName = "official_country_name"], [SourceColumnName = "flag_description", DestinationColumnName = "flag_description"], [SourceColumnName = "area", DestinationColumnName = "area"], [SourceColumnName = "load_date", DestinationColumnName = "load_date"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"Using SQL vw_latest_spanishspeakingcountries" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "8bc6ea08-fd30-4ca7-be7f-dc76aee17419"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "cde07ee3-3b2d-4924-a2da-d9ac5970b70e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "vw_latest_spanishspeakingcountries", ItemKind = "View"]}[Data]
in
  #"Navigation 2";
shared vw_latest_spanishspeakingcountries_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "8bc6ea08-fd30-4ca7-be7f-dc76aee17419"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "4e274702-44ef-49d7-815c-5df523e0e4cf"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "latest_spanishspeakingcountries_source_from_view"]}[Data]
in
  TableNavigation;
