SELECT 
    /* Create a data key by combining contract type, customer, commodity, and main contract number */
    CAST(
        CAST([CT].[CONTRACT_TYPE] AS VARCHAR) + '_' + 
        [CT].[CUSTOMER] + '_' + 
        [CT].[COMMODITY_CODE] + '_' + 
        SUBSTRING(LTRIM(RTRIM([CT].[CONTRACT_NUMBER])), 1, CHARINDEX('.', LTRIM(RTRIM([CT].[CONTRACT_NUMBER]))) - 1)
    AS VARCHAR(50)) AS [DATA_KEY],

    [CT].[CONTRACT_TYPE],  -- Numeric contract type (1 = Purchase, 2 = Sale)

    /* Human-readable contract type description */
    CAST(CASE [CT].[CONTRACT_TYPE] 
        WHEN 1 THEN 'Purchase' 
        WHEN 2 THEN 'Sale' 
        ELSE 'Other' 
    END AS VARCHAR(20)) AS [CONTRACT_TYPE_DESC],

    [CT].[CUSTOMER],  -- Customer ID
    [CM].[NAME] AS [CUSTOMER_DESC],  -- Customer name from master table

    [CT].[CONTRACT_NUMBER],  -- Full contract number (e.g., 12345.001)

    /* Extract main contract number (before the dot) */
    CAST(SUBSTRING(LTRIM(RTRIM([CT].[CONTRACT_NUMBER])), 1, CHARINDEX('.', LTRIM(RTRIM([CT].[CONTRACT_NUMBER]))) - 1) AS NUMERIC(12,0)) AS [CONTRACT_MAIN],

    /* Extract sub contract number (after the dot) */
    CAST(SUBSTRING(LTRIM(RTRIM([CT].[CONTRACT_NUMBER])), CHARINDEX('.', LTRIM(RTRIM([CT].[CONTRACT_NUMBER]))) + 1, 3) AS SMALLINT) AS [CONTRACT_SUB],

    [CT].[THEIR_CONTRACT_NUMBER],  -- External contract reference
    [CT].[COMMODITY_CODE],  -- Commodity code
    [IV].[DESCRIPTION] AS [COMMODITY_DESC],  -- Commodity description

    /* Commodity group ID, fallback to commodity code if group not found */
    CAST(ISNULL([DG].[DPR_GROUP], [CT].[COMMODITY_CODE]) AS NUMERIC(3,0)) AS [DPR_GROUP],

    /* Commodity group description, fallback to commodity description */
    CAST(ISNULL([DG].[DPR_GROUP_DESC], [IV].[DESCRIPTION]) AS VARCHAR(25)) AS [DPR_GROUP_DESC],

    [CT].[BRANCH_NUMBER],  -- Branch ID
    [BM].[BRANCH_NAME] AS [BRANCH_DESC],  -- Branch name

    [CT].[DPR_POSITION],  -- DPR position code
    ISNULL([PG].[DPR_POSITION_GROUP], '#Other') AS [DPR_POSITION_GROUP],  

    [CT].[ISSUED_DATE],  -- Date contract was issued
    [CT].[POSTING_DATE],  -- Date contract was posted
    [CT].[SYSTEM_DATE],  -- System entry date
    [CT].[DUE_DATE],  -- Due date for contract

    [CT].[CONTRACT_MASTER_SERIAL_ID],  -- Unique contract ID

    /* Weight-based quantities */
    [CT].[ISSUED_LBS], 
    [CT].[APPLIED_LBS], 
    [CT].[ADJUSTED_LBS], 
    [CT].[SUB_CONT_LBS],

    /* Remaining pounds = issued - applied + adjusted - subcontracted */
    ([CT].[ISSUED_LBS] - [CT].[APPLIED_LBS] + [ADJUSTED_LBS] - [SUB_CONT_LBS]) AS [REMAINING_LBS],

    /* Convert pounds to bushels using standard unit conversion */
    ([CT].[ISSUED_LBS] / [UM].[STANDARD_UNITS_LBS]) AS [ISSUED_BU],
    ([CT].[APPLIED_LBS] / [UM].[STANDARD_UNITS_LBS]) AS [APPLIED_BU],
    ([CT].[ADJUSTED_LBS] / [UM].[STANDARD_UNITS_LBS]) AS [ADJUSTED_BU],
    ([CT].[SUB_CONT_LBS] / [UM].[STANDARD_UNITS_LBS]) AS [SUB_CONT_BU],
    ([CT].[ISSUED_LBS] - [CT].[APPLIED_LBS] + [CT].[ADJUSTED_LBS] - [CT].[SUB_CONT_LBS]) / [UM].[STANDARD_UNITS_LBS] AS [REMAINING_BU],

    [CT].[SUB_CONT_FROM],  -- Source of subcontract
    [CT].[BOARD_PRICE],    -- Futures board price
    [CT].[BASIS_PRICE],    -- Basis price
    [CT].[PRICE],          -- price

    [CT].[SUB_CONTRACTED],  -- Subcontracted flag
    [CT].[CONTRACT_STATUS], -- Contract status 

    [CT].[ANNUAL_INTEREST_RATE],  -- Interest rate
    [CT].[INTEREST_START_DATE],   -- Interest accrual start

    [CT].[CMDELST],  -- Delivery start (raw)
    [CT].[DELIVERY_START_DATE],  -- Delivery start (formatted)
    [CT].[CMDELEN],  -- Delivery end (raw)
    [CT].[DELIVERY_END_DATE],  -- Delivery end (formatted)

    [CT].[FREIGHT],       -- Freight cost
    [CT].[FREIGHT_RATE],  -- Freight rate

    [CT].[CONTRACT_GRADE],  -- Grade of commodity
    [CT].[SIGNED],          -- Signed flag

    [CT].[LOW_POSTING_DATE],  -- Earliest posting date
    [CT].[BACKOUT_DATE],      -- Backout date
    [CT].[BACKOUT_POST_DATE], -- Backout posting date

    [CT].[FUTURES_MONTH],     -- Futures month
    [CT].[CMOXDATE],          
    [CT].[OFFER_EXPIRE_DATE], -- Offer expiration
    [CT].[OFFER_TARGET_POSITION], -- Target position

    [CT].[SERVICE_FEE],       -- Service fee
    [CT].[PRINTED],           -- Printed flag
    [CT].[BROKER],            -- Broker ID
    [CT].[BROKER_ACCOUNT],    -- Broker account
    [CT].[PROFITABILITY]      -- Profitability metric

FROM [dbo].[CA_CONTRACT_MASTER] AS [CT]

/* Join to customer master for customer name */
INNER JOIN [dbo].[AR_CUSTOMER_MASTER_TMA] AS [CM] 
    ON [CT].[CUSTOMER] = [CM].[CUSTOMER]

/* Join to commodity master for descriptions */
LEFT OUTER JOIN [dbo].[CA_COMMODITY_MASTER] AS [IV] 
    ON [CT].[COMMODITY_CODE] = [IV].[COMMODITY_CODE]

/* Join to unit of measure for conversion to bushels */
LEFT OUTER JOIN [dbo].[IV_UNIT_OF_MEASURE] AS [UM] 
    ON [IV].[UOM_BUSHELS] = [UM].[UNIT_OF_MEASURE]

/* Join to position master */
LEFT OUTER JOIN [dbo].[CA_POSITION_MASTER] AS [PM] 
    ON [CT].[DPR_POSITION] = [PM].[DPR_POSITION]

/* Join to branch master for branch name */
LEFT OUTER JOIN [dbo].[IV_BRANCH_MASTER] AS [BM] 
    ON [CT].[BRANCH_NUMBER] = [BM].[BRANCH_NUMBER]

/* Join to position group for grouping logic */
LEFT OUTER JOIN [dbo].[DPR_Position_Group] AS [PG] 
    ON [CT].[DPR_POSITION] = [PG].[DPR_POSITION]

/* Join to commodity group view for grouping and descriptions */
LEFT OUTER JOIN [dbo].[vCOMMODITY_GROUP] AS [DG] 
    ON [CT].[COMMODITY_CODE] = [DG].[COMMODITY_CODE]

/* Filter for open contracts only */
WHERE [CT].[CONTRACT_STATUS] = 'O'

/* Exclude Kansas Ethanol products (commodity codes 30–32) */
  AND [CT].[COMMODITY_CODE] NOT BETWEEN '30' AND '32'
