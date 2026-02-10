[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Account_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Account = let
  Source = Salesforce.Data("https://login.salesforce.com/", [ApiVersion = 48]),
  #"Navigation 1" = Source{[Name = "Account"]}[Data]
in
  #"Navigation 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Address_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Address = let
  Source = Salesforce.Data("https://login.salesforce.com/", [ApiVersion = 48]),
  #"Navigation 1" = Source{[Name = "Address"]}[Data]
in
  #"Navigation 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Contract_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Contract = let
  Source = Salesforce.Data("https://login.salesforce.com/", [ApiVersion = 48]),
  #"Navigation 1" = Source{[Name = "Contract"]}[Data]
in
  #"Navigation 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Event_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Event = let
  Source = Salesforce.Data("https://login.salesforce.com/", [ApiVersion = 48]),
  #"Navigation 1" = Source{[Name = "Event"]}[Data]
in
  #"Navigation 1";
shared Address_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "8bc6ea08-fd30-4ca7-be7f-dc76aee17419"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "cde07ee3-3b2d-4924-a2da-d9ac5970b70e"]}[Data],
  TableNavigation = Navigation_2{[Id = "SalesForce_Address", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared Event_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "8bc6ea08-fd30-4ca7-be7f-dc76aee17419"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "cde07ee3-3b2d-4924-a2da-d9ac5970b70e"]}[Data],
  TableNavigation = Navigation_2{[Id = "SalesForce-Event", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared Account_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "8bc6ea08-fd30-4ca7-be7f-dc76aee17419"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "cde07ee3-3b2d-4924-a2da-d9ac5970b70e"]}[Data],
  TableNavigation = Navigation_2{[Id = "SalesForce-Account", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared Contract_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "8bc6ea08-fd30-4ca7-be7f-dc76aee17419"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "cde07ee3-3b2d-4924-a2da-d9ac5970b70e"]}[Data],
  TableNavigation = Navigation_2{[Id = "SalesForce-Contract", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
