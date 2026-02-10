[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "spanishspeakingcountries_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ODS_KEY", DestinationColumnName = "ODS_KEY"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "official_country_name", DestinationColumnName = "official_country_name"], [SourceColumnName = "flag_description", DestinationColumnName = "flag_description"], [SourceColumnName = "area", DestinationColumnName = "area"], [SourceColumnName = "load_date", DestinationColumnName = "load_date"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared spanishspeakingcountries = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "8bc6ea08-fd30-4ca7-be7f-dc76aee17419"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "cde07ee3-3b2d-4924-a2da-d9ac5970b70e"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "spanishspeakingcountries", ItemKind = "Table"]}[Data],
  Transform_official_country_name = Table.AddColumn(#"Navigation 3", "Cleaned_official_country_name", each if [official_country_name] = "Republic of Cuba" then "R O Cuba"
else [official_country_name]),
  #"Added ODS_KEY" = Table.TransformColumnTypes(Table.AddColumn(Transform_official_country_name, "ODS_KEY", each [country_name] & "#" & [Cleaned_official_country_name]), {{"ODS_KEY", type text}}),
  #"Reordered columns" = Table.ReorderColumns(#"Added ODS_KEY", {"ODS_KEY", "country_name", "official_country_name", "Cleaned_official_country_name", "flag_description", "area", "load_date"}),
  #"Changed datatype_of_load_date" = Table.TransformColumnTypes(#"Reordered columns", {{"Cleaned_official_country_name", type text}, {"load_date", type datetime}})
in
  #"Changed datatype_of_load_date";
shared spanishspeakingcountries_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "8bc6ea08-fd30-4ca7-be7f-dc76aee17419"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "4e274702-44ef-49d7-815c-5df523e0e4cf"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "spanishspeakingcountries"]}[Data]
in
  TableNavigation;
shared #"CONTROL C_CONTROL_PROCESS_INSTANCE" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "8bc6ea08-fd30-4ca7-be7f-dc76aee17419"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "4e274702-44ef-49d7-815c-5df523e0e4cf"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "CONTROL", Item = "C_CONTROL_PROCESS_INSTANCE"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", let latest = List.Max(#"Navigation 2"[CONTROL_PROC_INST_START_DATETIME]) in each [CONTROL_PROC_INST_START_DATETIME] = latest)
in
  #"Filtered rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Merge_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ODS_KEY", DestinationColumnName = "ODS_KEY"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "official_country_name", DestinationColumnName = "official_country_name"], [SourceColumnName = "flag_description", DestinationColumnName = "flag_description"], [SourceColumnName = "area", DestinationColumnName = "area"], [SourceColumnName = "load_date", DestinationColumnName = "load_date"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Merge = let
  Source = Table.NestedJoin(#"CONTROL C_CONTROL_PROCESS_INSTANCE", {"CONTROL_PROC_INST_START_DATETIME"}, spanishspeakingcountries, {"load_date"}, "spanishspeakingcountries", JoinKind.Inner),
  #"Expanded spanishspeakingcountries" = Table.ExpandTableColumn(Source, "spanishspeakingcountries", {"ODS_KEY", "country_name", "official_country_name", "Cleaned_official_country_name", "flag_description", "area", "load_date"}, {"ODS_KEY", "country_name", "official_country_name", "Cleaned_official_country_name", "flag_description", "area", "load_date"}),
  #"Choose columns" = Table.SelectColumns(#"Expanded spanishspeakingcountries", {"ODS_KEY", "country_name", "official_country_name", "Cleaned_official_country_name", "flag_description", "area", "load_date"})
in
  #"Choose columns";
shared Merge_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "8bc6ea08-fd30-4ca7-be7f-dc76aee17419"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "4e274702-44ef-49d7-815c-5df523e0e4cf"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "spanishspeakingcountries"]}[Data]
in
  TableNavigation;
