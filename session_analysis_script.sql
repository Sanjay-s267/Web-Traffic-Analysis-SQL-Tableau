-- 1. Create the destination table structure (RawSessionsData)
IF OBJECT_ID('RawSessionsData') IS NOT NULL DROP TABLE RawSessionsData;
CREATE TABLE RawSessionsData (
    SessionID NVARCHAR(50),
    UserID NVARCHAR(50),
    VisitDate DATETIME,
    Device NVARCHAR(50),
    PageViews INT,
    BounceRate DECIMAL(18, 5),
    SessionDurationSeconds INT,
    EngagementScore DECIMAL(18, 5)
);
PRINT 'RawSessionsData table structure created.';

-- 2. Execute BULK INSERT command
BULK INSERT RawSessionsData
FROM 'C:\Users\Precision 5530\Downloads\final_web_traffic_for_sql.csv' 
WITH (
    FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '0x0a', 
    FIRSTROW = 2            -- Skips the header row
);
PRINT 'Data successfully imported into RawSessionsData.';
SELECT COUNT(*) AS TotalRows FROM RawSessionsData;
SELECT TOP 10 * FROM RawSessionsData;
EXEC sp_columns RawSessionsData;
EXEC sp_columns RawSessionsData;
SELECT COUNT(*) AS NullSessionIDs
FROM RawSessionsData
WHERE SessionID IS NULL;
SELECT SessionID, COUNT(SessionID) AS DuplicateCount
FROM RawSessionsData	
GROUP BY SessionID	
HAVING COUNT(SessionID) > 1;
WITH Duplicate_CTE AS (
SELECT *
, ROW_NUMBER() OVER (
PARTITION BY SessionID
ORDER BY SessionID
) AS RowNumber
FROM RawSessionsData
)
DELETE FROM Duplicate_CTE
WHERE RowNumber > 1;
COMMIT;
EXEC sp_columns RawSessionsData;
SELECT MIN([SessionDurationSeconds]) AS MinDuration,
       MAX([SessionDurationSeconds]) AS MaxDuration,
       AVG([SessionDurationSeconds]) AS AvgDuration
FROM RawSessionsData;
SELECT UserID,
COUNT(*) AS TotalSessions,
AVG(SessionDurationSeconds) AS AvgSessionTime
FROM RawSessionsData	
GROUP BY UserID
ORDER BY TotalSessions DESC;
EXEC sp_columns RawSessionsData;
SELECT [Device],
       COUNT(*) AS TotalSessions,
       AVG(SessionDurationSeconds) AS AvgSessionTime
FROM RawSessionsData
GROUP BY [Device]
ORDER BY TotalSessions DESC;
SELECT CAST(VisitDate AS DATE) AS ActivityDay,      
COUNT(*) AS TotalSessions,       
AVG(SessionDurationSeconds) AS AvgSessionTime
FROM RawSessionsData
GROUP BY CAST(VisitDate AS DATE)
ORDER BY ActivityDay;
SELECT SUM([PageViews]) AS TotalPageViews,
       AVG([BounceRate]) AS AvgBounceRate,
       AVG([EngagementScore]) AS AvgEngagementScore
FROM RawSessionsData;
SELECT CAST(VisitDate AS DATE) AS ActivityDay,
       Device,
       COUNT(*) AS TotalSessions,
       AVG(SessionDurationSeconds) AS AvgSessionTime
FROM RawSessionsData
GROUP BY CAST(VisitDate AS DATE), Device
ORDER BY ActivityDay, TotalSessions DESC;
SELECT CAST(VisitDate AS DATE) AS ActivityDay,
Device,	
COUNT(*) AS TotalSessions,
AVG(SessionDurationSeconds) AS AvgSessionTime
FROM RawSessionsData	
GROUP BY CAST(VisitDate AS DATE), Device	
ORDER BY ActivityDay, TotalSessions DESC;
SELECT UserID, VisitDate, Device, PageViews
FROM RawSessionsData
WHERE PageViews > (SELECT AVG(PageViews) FROM RawSessionsData) -- Above average page views
  AND BounceRate < 0.20 -- Low bounce rate (high quality)
ORDER BY PageViews DESC;
SELECT
    CASE
        WHEN EngagementScore < 5000 THEN 'Low'
        WHEN EngagementScore <= 20000 THEN 'Medium'
        ELSE 'High'
    END AS EngagementLevel,
    COUNT(*) AS TotalUsers
FROM RawSessionsData
GROUP BY
    CASE
        WHEN EngagementScore < 5000 THEN 'Low'
        WHEN EngagementScore <= 20000 THEN 'Medium'
        ELSE 'High'
    END;
    SELECT UserID, VisitDate, Device, PageViews, SessionDurationSeconds, EngagementScore,
    DENSE_RANK() OVER (ORDER BY EngagementScore DESC, PageViews DESC) AS EngagementRank
    FROM RawSessionsData	
WHERE SessionDurationSeconds >= 600 -- Sessions longer than 10 minutes	
AND PageViews >= 5	
ORDER BY EngagementRank ASC;
SELECT Device,
CAST(VisitDate AS DATE) AS ActivityDay,	
COUNT(*) AS TotalSessions,	
AVG(BounceRate) AS AvgBounceRate
FROM RawSessionsData	
GROUP BY Device, CAST(VisitDate AS DATE)	
ORDER BY AvgBounceRate DESC;
SELECT CAST(VisitDate AS DATE) AS ActivityDay,
       COUNT(*) AS TotalSessions,
       SUM(PageViews) AS DailyPageViews,
       AVG(SessionDurationSeconds) AS AvgDuration,
       AVG(BounceRate) AS AvgBounceRate,
       AVG(EngagementScore) AS AvgEngagementScore
INTO DailyMetrics
FROM RawSessionsData
GROUP BY CAST(VisitDate AS DATE);
SELECT UserID,
       AVG(PageViews) AS AvgPagesPerSession,
       AVG(SessionDurationSeconds) AS AvgSessionLength,
       CASE
           WHEN AVG(EngagementScore) >= 15000 AND AVG(PageViews) >= 10 THEN 'High_Value'
           WHEN AVG(EngagementScore) >= 5000 THEN 'Mid_Value'
           ELSE 'Low_Value'
       END AS UserTier
FROM RawSessionsData
GROUP BY UserID
ORDER BY UserTier DESC, AvgSessionLength DESC;
SELECT *
FROM DailyMetrics
ORDER BY ActivityDay DESC;
GO
CREATE VIEW HighValueSessions_V AS
SELECT SessionID, UserID, VisitDate, Device, PageViews, EngagementScore
FROM RawSessionsData
WHERE SessionDurationSeconds >= 600
  AND EngagementScore > 15000;
GO
  SELECT TOP 10 *
FROM HighValueSessions_V;
GO
SELECT *
FROM DailyMetrics;
SELECT *
FROM HighValueSessions_V;
SELECT CAST(VisitDate AS DATE) AS ActivityDay,
       COUNT(*) AS TotalSessions,
       SUM(PageViews) AS DailyPageViews,
       AVG(SessionDurationSeconds) AS AvgDuration,
       AVG(BounceRate) AS AvgBounceRate,
       AVG(EngagementScore) AS AvgEngagementScore
INTO DailyMetrics
FROM RawSessionsData
GROUP BY CAST(VisitDate AS DATE);
GO
SELECT UserID,
       AVG(PageViews) AS AvgPagesPerSession,
       AVG(SessionDurationSeconds) AS AvgSessionLength,
       CASE
           WHEN AVG(EngagementScore) >= 15000 AND AVG(PageViews) >= 10 THEN 'High_Value'
           WHEN AVG(EngagementScore) >= 5000 THEN 'Mid_Value'
           ELSE 'Low_Value'
       END AS UserTier
FROM RawSessionsData
GROUP BY UserID;
CREATE VIEW HighValueSessions_V AS
SELECT SessionID, UserID, VisitDate, Device, PageViews, EngagementScore
FROM RawSessionsData
WHERE SessionDurationSeconds >= 600
  AND EngagementScore > 15000;
GO