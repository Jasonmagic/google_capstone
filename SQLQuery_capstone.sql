---Filter out duplicate rows, added Quarter_last, Amount_QTD
WITH T3 AS (SELECT [GAAP_Code],[Amount],[Year],[Ticker],[Quarter],[Report Date],[Coverage],[Segment],

				(CASE WHEN T1.[Quarter] > 1 THEN T1.[Quarter] -1 ELSE 4 END) AS [Quarter_last],
				(CASE WHEN T1.[Quarter] > 1 THEN T1.[Year] ELSE T1.[Year]-1 END) AS [Year_last],
	
				[Amount] - (SELECT CASE WHEN T1.[Quarter] = 1 THEN 0 ELSE [Amount] END
							FROM (SELECT DISTINCT * FROM [SEC_v2021_10].[dbo].[XMLDATA]) AS T2
							WHERE	(T2.[GAAP_Code] = T1.[GAAP_Code]) AND
									(T2.[Quarter] = (CASE WHEN T1.[Quarter] > 1 THEN T1.[Quarter] -1 ELSE T1.[Quarter] END)) AND	
									(T2.[Year] = T1.[Year]) AND
									(T2.[CIK] = T1.[CIK]) AND
									(T2.[Segment] = 'MAIN_SEGMENT') AND
									(T2.[Coverage] = 'YTD')
					) AS [Amount_QTD]
			FROM (SELECT DISTINCT * FROM [SEC_v2021_10].[dbo].[XMLDATA]) AS T1
			WHERE (T1.[Ticker] = 'WFC' OR T1.[Ticker] = 'BAC') AND T1.[Segment] = 'MAIN_SEGMENT' AND T1.[Coverage] = 'YTD'), 
			
	T4 AS (SELECT [GAAP_Code],[Year],[Ticker],[Quarter],

				(CASE WHEN T1.[Quarter] > 1 THEN T1.[Quarter] -1 ELSE 4 END) AS [Quarter_last],
				(CASE WHEN T1.[Quarter] > 1 THEN T1.[Year] ELSE T1.[Year]-1 END) AS [Year_last],
	
				[Amount] - (SELECT CASE WHEN T1.[Quarter] = 1 THEN 0 ELSE [Amount] END
							FROM (SELECT DISTINCT * FROM [SEC_v2021_10].[dbo].[XMLDATA]) AS T2
							WHERE	(T2.[GAAP_Code] = T1.[GAAP_Code]) AND
									(T2.[Quarter] = (CASE WHEN T1.[Quarter] > 1 THEN T1.[Quarter] -1 ELSE T1.[Quarter] END)) AND	
									(T2.[Year] = T1.[Year]) AND
									(T2.[CIK] = T1.[CIK]) AND
									(T2.[Segment] = 'MAIN_SEGMENT') AND
									(T2.[Coverage] = 'YTD')
					) AS [Amount_QTD]
			FROM (SELECT DISTINCT * FROM [SEC_v2021_10].[dbo].[XMLDATA]) AS T1
			WHERE (T1.[Ticker] = 'WFC' OR T1.[Ticker] = 'BAC') AND T1.[Segment] = 'MAIN_SEGMENT' AND T1.[Coverage] = 'YTD')

---Added Amount_QTD_LQ (Last Quarter)
SELECT *, 
	(SELECT [Amount_QTD] FROM T4
		WHERE
		(T4.[GAAP_Code] = T3.[GAAP_Code]) AND
		(T4.[Year] = T3.[Year_last]) AND
		(T4.[Ticker] = T3.[Ticker]) AND
		(T4.[Quarter] = T3.[Quarter_last]) AND
		(T4.[Year] = T3.[Year_last])
	)AS [Amount_QTD_prev]
FROM T3