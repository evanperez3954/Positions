-- Common Table Expression (CTE) combining sales and purchase contract metadata
;WITH ct (
    CTTYPE,         -- Indicator for contract type: 1 = Purchase, 2 = Sale
    CTTYPEDESC,     -- Text label for contract type
    DPRGRP,         -- Commodity group ID (from grouping logic or default code)
    DPRGRPDESC,     -- Description for commodity group
    BRANCHNMBR,     -- Branch number
    BRANCHDESC,     -- Branch description
    DELENDDT        -- Delivery end date (bucketed)
) AS (
    /* --- Sale Contracts Section --- */
    SELECT DISTINCT 
        2, 'Sale',  -- Contract type indicator and description
        ISNULL([DG].[DPR_GROUP], [IV].[COMMODITY_CODE]) AS [DPR_GROUP],  -- Use group if available, else fallback to commodity code
        CAST(ISNULL([DG].[DPR_GROUP_DESC], [IV].[DESCRIPTION]) AS NVARCHAR(25)) AS [DPR_GROUP_DESC],  -- Group description fallback
        [BM].[BRANCH_NUMBER],  -- Branch number
        [BM].[BRANCH_DESC],    -- Branch description
        DM.DATEID              -- Bucketed delivery end date
    FROM 
        /* Delivery date bucketing logic: past, current 12 months, future */
        (
            SELECT DISTINCT 
                CASE 
                    WHEN [DATEID] < CAST(CONVERT(VARCHAR(7), DATEADD(MONTH, -1, GETDATE()), 120) + '-01' AS DATE)
                        THEN CAST('1900-01-01' AS DATE)  -- Past bucket
                    WHEN [DATEID] BETWEEN CAST(CONVERT(VARCHAR(7), DATEADD(MONTH, -1, GETDATE()), 120) + '-01' AS DATE)
                                        AND DATEADD(DAY, -1, CAST(CONVERT(VARCHAR(7), DATEADD(MONTH, 11, GETDATE()), 120) + '-01' AS DATE))
                        THEN [DATEID]  -- Current 12-month window
                    WHEN [DATEID] > DATEADD(DAY, -1, CAST(CONVERT(VARCHAR(7), DATEADD(MONTH, 11, GETDATE()), 120) + '-01' AS DATE))
                        THEN CAST('9999-12-31' AS DATE)  -- Future bucket
                END AS [DATEID]
            FROM [dbo].[DM_DateMaster]
        ) AS [DM]

        /* Valid commodities under code '30' */
        CROSS JOIN (
            SELECT [COMMODITY_CODE], [DESCRIPTION]
            FROM [dbo].[CA_COMMODITY_MASTER]
            WHERE [COMMODITY_CODE] < '30'
        ) AS [IV]

        /* Branches with non-zero remaining bushels */
        CROSS JOIN (
            SELECT [BRANCH_NUMBER], [BRANCH_DESC]
            FROM [dbo].[DM_OpenContracts]
            GROUP BY [BRANCH_NUMBER], [BRANCH_DESC]
            HAVING SUM([REMAINING_BU]) <> 0
        ) AS [BM]

        /* Optional join to commodity group lookup */
        LEFT JOIN [dbo].[vCOMMODITY_GROUP] AS [DG]
        ON [IV].[COMMODITY_CODE] = [DG].[COMMODITY_CODE]

    UNION ALL

    /* --- Purchase Contracts Section (same structure as above) --- */
    SELECT DISTINCT 
        1, 'Purchase',  -- Contract type indicator and description
        ISNULL([DG].[DPR_GROUP], [IV].[COMMODITY_CODE]) AS [DPR_GROUP],
        CAST(ISNULL([DG].[DPR_GROUP_DESC], [IV].[DESCRIPTION]) AS NVARCHAR(25)) AS [DPR_GROUP_DESC],
        [BM].[BRANCH_NUMBER],
        [BM].[BRANCH_DESC],
        DM.DATEID
    FROM 
        (same delivery date logic as above) AS [DM]
        CROSS JOIN (same commodity logic) AS [IV]
        CROSS JOIN (same branch filtering) AS [BM]
        LEFT JOIN [dbo].[vCOMMODITY_GROUP] AS [DG]
        ON [IV].[COMMODITY_CODE] = [DG].[COMMODITY_CODE]
)

-- Final query: aggregate remaining bushels per contract type, group, branch, and delivery date
SELECT 
    ct.cttype AS CONTRACT_TYPE,  -- 1 = Purchase, 2 = Sale
    CAST(ct.cttypedesc AS NVARCHAR(20)) AS CONTRACT_TYPE_DESC,
    ct.dprgrp AS DPR_GROUP,
    ct.dprgrpdesc AS DPR_GROUP_DESC,
    ct.branchnmbr AS BRANCH_NUMBER,
    ct.branchdesc AS BRANCH_NAME,
    CAST(CONVERT(VARCHAR(7), ct.delenddt, 120) + '-01' AS DATE) AS DELIVERY_END_DATE,  -- Normalize to first of month
    CAST(DM.MONTHID_DESC AS NVARCHAR(8)) AS MONTHID_DESC,  -- Month label
    ISNULL(SUM(OC.REMAINING_BU), 0) AS REMAINING_BU  -- Total remaining bushels
FROM ct
/* Join to open contracts to get actual remaining bushels */
LEFT OUTER JOIN [dbo].DM_OpenContracts AS OC
    ON ct.cttype = OC.contract_type
    AND ct.dprgrp = OC.dpr_group
    AND ct.branchdesc = OC.branch_desc
    AND ct.delenddt = CASE 
        WHEN [OC].[DELIVERY_END_DATE] < CAST(CONVERT(VARCHAR(7), DATEADD(MONTH, -1, GETDATE()), 120) + '-01' AS DATE)
            THEN CAST('1900-01-01' AS DATE)
        WHEN [OC].[DELIVERY_END_DATE] BETWEEN CAST(CONVERT(VARCHAR(7), DATEADD(MONTH, -1, GETDATE()), 120) + '-01' AS DATE)
                                          AND DATEADD(DAY, -1, CAST(CONVERT(VARCHAR(7), DATEADD(MONTH, 11, GETDATE()), 120) + '-01' AS DATE))
            THEN [OC].[DELIVERY_END_DATE]
        WHEN [OC].[DELIVERY_END_DATE] > DATEADD(DAY, -1, CAST(CONVERT(VARCHAR(7), DATEADD(MONTH, 11, GETDATE()), 120) + '-01' AS DATE))
            THEN CAST('9999-12-31' AS DATE)
    END

/* Join to get month description */
LEFT OUTER JOIN [dbo].DM_DateMaster AS DM
    ON ct.delenddt = DM.dateid

/* Group by all relevant dimensions */
GROUP BY 
    ct.cttype, 
    ct.cttypedesc,
    ct.dprgrp, 
    ct.dprgrpdesc, 
    ct.branchnmbr, 
    ct.branchdesc,
    CAST(CONVERT(VARCHAR(7), ct.delenddt, 120) + '-01' AS DATE), 
    DM.MONTHID_DESC

