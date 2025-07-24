Positions

let
    Source = Sql.Database("MKC-SQLCALL", "AgTrax_BI", [Query="SELECT CONTRACT_TYPE, CONTRACT_TYPE_DESC, DPR_GROUP, DPR_GROUP_DESC, DELIVERY_END_DATE, MONTHID_DESC, SUM(REMAINING_BU) REMAINING_BU#(lf)FROM Agtrax_BI..DM_OpenContractsNet with(nolock) #(lf)WHERE DPR_GROUP in ('1','2','3','4','5','7','11')#(lf)GROUP BY CONTRACT_TYPE, CONTRACT_TYPE_DESC, DPR_GROUP, DPR_GROUP_DESC, DELIVERY_END_DATE, MONTHID_DESC#(lf)ORDER BY CONTRACT_TYPE, DELIVERY_END_DATE#(lf)"]),
    #"Renamed Columns" = Table.RenameColumns(Source,{{"CONTRACT_TYPE_DESC", "Contract Type"}, {"DPR_GROUP_DESC", "Commodity"}}),
    #"Changed Type" = Table.TransformColumnTypes(#"Renamed Columns",{{"DELIVERY_END_DATE", type date}})
in
    #"Changed Type"

Position Contract Detail

let
    Source = Sql.Database("MKC-SQLCALL", "MKCGP", [Query="WITH K AS (SELECT LTRIM(RTRIM(CUSTOMER_DESC))+' - '+LTRIM(RTRIM(CUSTOMER)) AS [Account]#(lf)     , LTRIM(RTRIM(BRANCH_DESC))+' - '+CAST(BRANCH_NUMBER AS NVARCHAR(20)) AS [Location]#(lf)     , LTRIM(RTRIM(COMMODITY_CODE))+' - '+LTRIM(RTRIM(COMMODITY_DESC)) [Commodity]#(lf),CONTRACT_TYPE,CONTRACT_TYPE_DESC#(lf),CUSTOMER,CUSTOMER_DESC,CONTRACT_NUMBER,CONTRACT_MAIN#(lf)--,CONTRACT_SUB#(lf)--,THEIR_CONTRACT_NUMBER#(lf),COMMODITY_CODE,COMMODITY_DESC,DPR_GROUP#(lf),DPR_GROUP_DESC#(lf),BRANCH_NUMBER,BRANCH_DESC#(lf),DPR_POSITION#(lf)--,DPR_POSITION_GROUP#(lf)--,ISSUED_DATE,POSTING_DATE#(lf)--,SYSTEM_DATE,DUE_DATE#(lf),REMAINING_BU#(lf),BOARD_PRICE,BASIS_PRICE#(lf),PRICE#(lf),CAST(DELIVERY_START_DATE AS DATE) DELIVERY_START_DATE#(lf),CAST(DELIVERY_END_DATE AS DATE) DELIVERY_END_DATE #(lf),CAST(FUTURES_MONTH AS DATE) FUTURES_MONTH#(lf),YEAR(DELIVERY_END_DATE) as DYear#(lf),Month(DELIVERY_END_DATE) as DMonth#(lf),FORMAT(DELIVERY_END_DATE,'MMM') as DMonthName#(lf),rank() over ( Order by YEAR(DELIVERY_END_DATE),Month(DELIVERY_END_DATE)) as rank#(lf)FROM AgTrax_BI..DM_OpenContracts#(lf)WHERE REMAINING_BU<>0 and DPR_GROUP in ('1','2','3','4','5','7','11'))#(lf)#(lf)SELECT *#(lf),case#(lf)when DYear < year(GETDATE()) OR ( DYear = year (GETDATE()) and DMonth < MONTH(GETDATE())-1) THEN 'Prior'#(lf)WHEN (( DYear = YEAR(GETDATE()) AND ( DMonth = Month(GETDATE()) -1   OR  DMonth >= Month(GETDATE()))) OR ( DYear > YEAR(GETDATE())  AND DMonth < Month(GETDATE()) -1)) THEN CONCAT(DMonthName,DYear)#(lf)else 'Extended' END AS MONTHID#(lf)#(lf)#(lf)FROM K order by DYear,Dmonth#(lf)#(lf)/**** still working on labelling current MONYEAR values *****/"]),
    #"Changed Type1" = Table.TransformColumnTypes(Source,{{"DELIVERY_START_DATE", type date}, {"DELIVERY_END_DATE", type date}, {"FUTURES_MONTH", type date}}),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type1",{{"CONTRACT_NUMBER", "Contract Number"}, {"DPR_POSITION", "Position"}, {"REMAINING_BU", "Remaining_BU"}, {"BASIS_PRICE", "Basis Price"}, {"COMMODITY_DESC", "Commodity Description"}, {"PRICE", "Price"}, {"DELIVERY_START_DATE", "Delivery Start Date"}, {"DELIVERY_END_DATE", "Delivery End Date"}, {"FUTURES_MONTH", "Futures Month"}, {"CONTRACT_TYPE_DESC", "Contract Type"}, {"MONTHID", "MONTHID_"}})
in
    #"Renamed Columns"

Open Contracts

let
    Source = PowerPlatform.Dataflows(null),
    Workspaces = Source{[Id="Workspaces"]}[Data],
    #"f5c6218e-9b50-4e8b-87fc-7d5f7d56dabe" = Workspaces{[workspaceId="f5c6218e-9b50-4e8b-87fc-7d5f7d56dabe"]}[Data],
    #"8b10c865-e7c8-44f1-b47d-ae56f5bb64d6" = #"f5c6218e-9b50-4e8b-87fc-7d5f7d56dabe"{[dataflowId="8b10c865-e7c8-44f1-b47d-ae56f5bb64d6"]}[Data],
    #"Open Contracts_" = #"8b10c865-e7c8-44f1-b47d-ae56f5bb64d6"{[entity="Open Contracts",version=""]}[Data],
    #"Capitalized Each Word" = Table.TransformColumns(#"Open Contracts_",{{"CUSTOMER_DESC", Text.Proper, type text}, {"COMMODITY_DESC", Text.Proper, type text}, {"DPR_GROUP_DESC", Text.Proper, type text}, {"BRANCH_DESC", Text.Proper, type text}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Capitalized Each Word", "CUSTOMER", "CUSTOMER - Copy"),
    #"Removed Columns" = Table.RemoveColumns(#"Duplicated Column",{"CUSTOMER - Copy"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Removed Columns",{{"CUSTOMER", Int64.Type}}),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type",{{"CONTRACT_NUMBER", "PA Contract Number"}, {"THEIR_CONTRACT_NUMBER", "Customer Contract Number"}, {"BOARD_PRICE", "Futures Price"}}),
    #"Replaced Value" = Table.ReplaceValue(#"Renamed Columns","Y","Yes",Replacer.ReplaceText,{"FREIGHT"}),
    #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value","N","No",Replacer.ReplaceText,{"FREIGHT"})
in
    #"Replaced Value1"

let
    Source = PowerPlatform.Dataflows(null),
    Workspaces = Source{[Id="Workspaces"]}[Data],
    #"f5c6218e-9b50-4e8b-87fc-7d5f7d56dabe" = Workspaces{[workspaceId="f5c6218e-9b50-4e8b-87fc-7d5f7d56dabe"]}[Data],
    #"ed2fdca4-93b1-4cfc-be18-379807afd2f1" = #"f5c6218e-9b50-4e8b-87fc-7d5f7d56dabe"{[dataflowId="ed2fdca4-93b1-4cfc-be18-379807afd2f1"]}[Data],
    #"Agtrax Customer Master_" = #"ed2fdca4-93b1-4cfc-be18-379807afd2f1"{[entity="Agtrax Customer Master",version=""]}[Data],
    #"Capitalized Each Word" = Table.TransformColumns(#"Agtrax Customer Master_",{{"CITY", Text.Proper, type text}})
in
    #"Capitalized Each Word"

Apothecary

let
    Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i44FAA==", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [Column1 = _t]),
    #"Changed Type" = Table.TransformColumnTypes(Source,{{"Column1", type text}})
in
    #"Changed Type"

IT Apps DateDim

let
    Source = PowerPlatform.Dataflows(null),
    Workspaces = Source{[Id="Workspaces"]}[Data],
    #"f5c6218e-9b50-4e8b-87fc-7d5f7d56dabe" = Workspaces{[workspaceId="f5c6218e-9b50-4e8b-87fc-7d5f7d56dabe"]}[Data],
    #"ca05a624-6d51-4089-952a-08addb11ade2" = #"f5c6218e-9b50-4e8b-87fc-7d5f7d56dabe"{[dataflowId="ca05a624-6d51-4089-952a-08addb11ade2"]}[Data],
    #"ITApps DateDim_" = #"ca05a624-6d51-4089-952a-08addb11ade2"{[entity="ITApps DateDim",version=""]}[Data]
in
    #"ITApps DateDim_"