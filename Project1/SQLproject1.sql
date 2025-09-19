-- Creates database, schemas, tables, and loads data

/**********************************************
                DATABASE SETUP
**********************************************/

/* 0.1 Create database safely */
IF DB_ID(N'SuperstoreDW') IS NULL
BEGIN
    CREATE DATABASE SuperstoreDW;
    PRINT 'Database SuperstoreDW created successfully';
END
ELSE
BEGIN
    PRINT 'Database SuperstoreDW already exists';
END
GO

/* Ensure database is usable */
USE master;
GO
ALTER DATABASE SuperstoreDW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO
USE SuperstoreDW;
GO
ALTER DATABASE SuperstoreDW SET MULTI_USER;
GO

/**********************************************
                SCHEMA CREATION
**********************************************/

/* 0.2 Create schemas */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'raw') 
    EXEC('CREATE SCHEMA raw;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg') 
    EXEC('CREATE SCHEMA stg;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dim') 
    EXEC('CREATE SCHEMA dim;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fact') 
    EXEC('CREATE SCHEMA fact;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'qa')  
    EXEC('CREATE SCHEMA qa;');
GO

/**********************************************
                TABLE CLEANUP
**********************************************/

/* 1. Drop tables in dependency order */
PRINT 'Dropping existing tables...';
IF OBJECT_ID('fact.Sales','U') IS NOT NULL 
    DROP TABLE fact.Sales;
IF OBJECT_ID('dim.Product','U') IS NOT NULL 
    DROP TABLE dim.Product;
IF OBJECT_ID('dim.Customer','U') IS NOT NULL 
    DROP TABLE dim.Customer;
IF OBJECT_ID('dim.SubCategory','U') IS NOT NULL 
    DROP TABLE dim.SubCategory;
IF OBJECT_ID('dim.Category','U') IS NOT NULL 
    DROP TABLE dim.Category;
IF OBJECT_ID('dim.Geography','U') IS NOT NULL 
    DROP TABLE dim.Geography;
IF OBJECT_ID('dim.ShipMode','U') IS NOT NULL 
    DROP TABLE dim.ShipMode;
IF OBJECT_ID('dim.Date','U') IS NOT NULL 
    DROP TABLE dim.Date;
IF OBJECT_ID('stg.Superstore_Typed','U') IS NOT NULL 
    DROP TABLE stg.Superstore_Typed;
IF OBJECT_ID('raw.Superstore','U') IS NOT NULL 
    DROP TABLE raw.Superstore;
IF OBJECT_ID('qa.LoadIssues','U') IS NOT NULL 
    DROP TABLE qa.LoadIssues;
GO

/**********************************************
                RAW LAYER
**********************************************/

/* 1.1 RAW landing table */
PRINT 'Creating raw.Superstore table...';
CREATE TABLE raw.Superstore
(
    OrderID        NVARCHAR(50)  NULL,
    OrderDate      NVARCHAR(50)  NULL,
    ShipDate       NVARCHAR(50)  NULL,
    ShipMode       NVARCHAR(100) NULL,
    CustomerID     NVARCHAR(50)  NULL,
    CustomerName   NVARCHAR(200) NULL,
    Segment        NVARCHAR(100) NULL,
    Country        NVARCHAR(100) NULL,
    City           NVARCHAR(200) NULL,
    [State]        NVARCHAR(200) NULL,
    PostalCode     NVARCHAR(50)  NULL,
    Region         NVARCHAR(100) NULL,
    ProductID      NVARCHAR(50)  NULL,
    Category       NVARCHAR(100) NULL,
    SubCategory    NVARCHAR(100) NULL,
    ProductName    NVARCHAR(400) NULL,
    Sales          NVARCHAR(50)  NULL,
    Quantity       NVARCHAR(50)  NULL,
    Discount       NVARCHAR(50)  NULL,
    Profit         NVARCHAR(50)  NULL,
    IngestedAt     DATETIME2(3)  NOT NULL CONSTRAINT DF_raw_IngestedAt DEFAULT (SYSUTCDATETIME()),
    SourceFile     NVARCHAR(400) NULL
);
GO



/* 1.2 BULK load data */
PRINT 'Loading data from CSV file...';
BEGIN TRY
    BULK INSERT raw.superstore
    FROM 'C:\Project1\superstore.csv' 
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        CODEPAGE = '65001',
        TABLOCK
    );
    PRINT 'Data loaded successfully into raw.Superstore';
END TRY
BEGIN CATCH
    PRINT 'Error loading data: ' + ERROR_MESSAGE();
END CATCH
GO

/**********************************************
                STAGING LAYER
**********************************************/

/* 2.1 Typed staging table */
PRINT 'Creating stg.Superstore_Typed table...';
CREATE TABLE stg.Superstore_Typed
(
    OrderID        VARCHAR(50),
    OrderDate      DATE,
    ShipDate       DATE,
    ShipMode       VARCHAR(100),
    CustomerID     VARCHAR(50),
    CustomerName   VARCHAR(200),
    Segment        VARCHAR(100),
    Country        VARCHAR(100),
    City           VARCHAR(200),
    [State]        VARCHAR(200),
    PostalCode     VARCHAR(20),
    Region         VARCHAR(100),
    ProductID      VARCHAR(50),
    Category       VARCHAR(100),
    SubCategory    VARCHAR(100),
    ProductName    VARCHAR(400),
    Sales          DECIMAL(18,2),
    Quantity       INT,
    Discount       DECIMAL(9,4),
    Profit         DECIMAL(18,2),
    IngestedAt     DATETIME2(3),
    SourceFile     NVARCHAR(400)
);
GO

/* 2.2 Load + type conversion */
PRINT 'Loading data into staging table...';
INSERT INTO stg.Superstore_Typed
(
    OrderID, OrderDate, ShipDate, ShipMode, CustomerID, CustomerName, Segment,
    Country, City, [State], PostalCode, Region, ProductID, Category, SubCategory, ProductName,
    Sales, Quantity, Discount, Profit, IngestedAt, SourceFile
)
SELECT
    NULLIF(LTRIM(RTRIM(OrderID)), ''),
    TRY_CONVERT(date, OrderDate, 120),
    TRY_CONVERT(date, ShipDate, 120),
    NULLIF(LTRIM(RTRIM(ShipMode)), ''),
    NULLIF(LTRIM(RTRIM(CustomerID)), ''),
    NULLIF(LTRIM(RTRIM(CustomerName)), ''),
    NULLIF(LTRIM(RTRIM(Segment)), ''),
    NULLIF(LTRIM(RTRIM(Country)), ''),
    NULLIF(LTRIM(RTRIM(City)), ''),
    NULLIF(LTRIM(RTRIM([State])), ''),
    NULLIF(REPLACE(PostalCode, ' ', ''), ''),
    NULLIF(LTRIM(RTRIM(Region)), ''),
    NULLIF(LTRIM(RTRIM(ProductID)), ''),
    NULLIF(LTRIM(RTRIM(Category)), ''),
    NULLIF(LTRIM(RTRIM(SubCategory)), ''),
    NULLIF(LTRIM(RTRIM(ProductName)), ''),
    TRY_CONVERT(decimal(18,2), Sales),
    TRY_CONVERT(int, Quantity),
    TRY_CONVERT(decimal(9,4), Discount),
    TRY_CONVERT(decimal(18,2), Profit),
    IngestedAt,
    SourceFile
FROM raw.Superstore;
PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows loaded into staging table';
GO

/* 2.3 De-duplication */
PRINT 'Removing duplicates from staging table...';
WITH d AS
(
    SELECT *,
           ROW_NUMBER() OVER (
              PARTITION BY OrderID, ProductID
              ORDER BY IngestedAt DESC, SourceFile DESC
           ) AS rn
    FROM stg.Superstore_Typed
)
DELETE FROM d WHERE rn > 1;
PRINT 'Deduplication complete';
GO

/* 2.4 Basic QA checks */
PRINT 'Running data quality checks...';
CREATE TABLE qa.LoadIssues
(
    IssueType   VARCHAR(100),
    IssueDetail VARCHAR(400),
    [RowCount]  INT,
    CreatedAt   DATETIME2(3) DEFAULT SYSUTCDATETIME()
);

INSERT INTO qa.LoadIssues (IssueType, IssueDetail, [RowCount])
SELECT 'NULL_DATES', 'OrderDate or ShipDate is NULL', COUNT(*)
FROM stg.Superstore_Typed
WHERE OrderDate IS NULL OR ShipDate IS NULL;

INSERT INTO qa.LoadIssues (IssueType, IssueDetail, [RowCount])
SELECT 'NEGATIVE_PROFIT', 'Profit < 0', COUNT(*)
FROM stg.Superstore_Typed
WHERE Profit < 0;

INSERT INTO qa.LoadIssues (IssueType, IssueDetail, [RowCount])
SELECT 'INCONSISTENT_GEOGRAPHY', 'Missing Region/State/City', COUNT(*)
FROM stg.Superstore_Typed
WHERE Region IS NULL OR [State] IS NULL OR City IS NULL;

-- Display QA results
SELECT * FROM qa.LoadIssues;
GO

/**********************************************
                DIMENSIONS
**********************************************/

/* 3.1 Date dimension */
PRINT 'Creating and populating dim.Date...';
CREATE TABLE dim.Date
(
    DateKey         INT        NOT NULL PRIMARY KEY,
    [Date]          DATE       NOT NULL,
    [Year]          INT        NOT NULL,
    [Quarter]       TINYINT    NOT NULL,
    [Month]         TINYINT    NOT NULL,
    [Day]           TINYINT    NOT NULL,
    MonthName       VARCHAR(20) NOT NULL,
    QuarterName     VARCHAR(6)  NOT NULL,
    WeekOfYear      TINYINT     NULL,
    IsWeekend       BIT         NOT NULL
);

DECLARE @Start DATE = (SELECT ISNULL(MIN(OrderDate),'2010-01-01') FROM stg.Superstore_Typed);
DECLARE @End   DATE = (SELECT ISNULL(MAX(ShipDate),'2025-12-31') FROM stg.Superstore_Typed);

;WITH d AS
(
    SELECT @Start AS d
    UNION ALL
    SELECT DATEADD(DAY,1,d) FROM d WHERE d < @End
)
INSERT INTO dim.Date (DateKey,[Date],[Year],[Quarter],[Month],[Day],MonthName,QuarterName,WeekOfYear,IsWeekend)
SELECT  
    CONVERT(INT,FORMAT(d,'yyyyMMdd')),
    d,
    DATEPART(YEAR,d),
    DATEPART(QUARTER,d),
    DATEPART(MONTH,d),
    DATEPART(DAY,d),
    DATENAME(MONTH,d),
    CONCAT('Q',DATEPART(QUARTER,d)),
    DATEPART(WEEK,d),
    CASE WHEN DATENAME(WEEKDAY,d) IN ('Saturday','Sunday') THEN 1 ELSE 0 END
FROM d
OPTION (MAXRECURSION 0);
PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' date records created';
GO

/* 4.1 Other dimension tables */
PRINT 'Creating dimension tables...';

-- ShipMode
CREATE TABLE dim.ShipMode
(
    ShipModeKey INT IDENTITY PRIMARY KEY,
    ShipMode    VARCHAR(100) UNIQUE NOT NULL
);

-- Category
CREATE TABLE dim.Category
(
    CategoryKey INT IDENTITY PRIMARY KEY,
    Category    VARCHAR(100) UNIQUE NOT NULL
);

-- SubCategory
CREATE TABLE dim.SubCategory
(
    SubCategoryKey INT IDENTITY PRIMARY KEY,
    CategoryKey    INT NOT NULL FOREIGN KEY REFERENCES dim.Category(CategoryKey),
    SubCategory    VARCHAR(100) NOT NULL,
    CONSTRAINT UQ_SubCat UNIQUE (CategoryKey, SubCategory)
);

-- Geography
CREATE TABLE dim.Geography
(
    GeographyKey  INT IDENTITY PRIMARY KEY,
    Country       VARCHAR(100) NOT NULL,
    [State]       VARCHAR(200) NOT NULL,
    City          VARCHAR(200) NOT NULL,
    Region        VARCHAR(100) NOT NULL,
    PostalCode    VARCHAR(20)  NULL,
    HashKey       AS CONVERT(BINARY(16), HASHBYTES('MD5', 
                       CONCAT(UPPER(Country),'|',UPPER([State]),'|',UPPER(City),'|',UPPER(Region),'|',ISNULL(PostalCode,''))))
                  PERSISTED
);

-- Customer (SCD Type 2)
CREATE TABLE dim.Customer
(
    CustomerKey     INT IDENTITY PRIMARY KEY,
    CustomerID      VARCHAR(50)    NOT NULL,
    CustomerName    VARCHAR(200)   NOT NULL,
    Segment         VARCHAR(100)   NULL,
    Region          VARCHAR(100)   NULL,
    EffectiveFrom   DATE           NOT NULL,
    EffectiveTo     DATE           NOT NULL,
    IsCurrent       BIT            NOT NULL,
    HashDiff        VARBINARY(32)  NOT NULL
);
CREATE UNIQUE INDEX UX_Customer_Current ON dim.Customer(CustomerID, IsCurrent) WHERE IsCurrent = 1;

-- Product (SCD Type 2)
CREATE TABLE dim.Product
(
    ProductKey     INT IDENTITY PRIMARY KEY,
    ProductID      VARCHAR(50)   NOT NULL,
    ProductName    VARCHAR(400)  NOT NULL,
    SubCategoryKey INT           NOT NULL FOREIGN KEY REFERENCES dim.SubCategory(SubCategoryKey),
    EffectiveFrom  DATE          NOT NULL,
    EffectiveTo    DATE          NOT NULL,
    IsCurrent      BIT           NOT NULL,
    HashDiff       VARBINARY(32) NOT NULL
);
CREATE UNIQUE INDEX UX_Product_Current ON dim.Product(ProductID, IsCurrent) WHERE IsCurrent = 1;
GO

/**********************************************
                FACT TABLE
**********************************************/

PRINT 'Creating fact.Sales table...';
CREATE TABLE fact.Sales
(
    SalesKey      BIGINT IDENTITY PRIMARY KEY,
    OrderID       VARCHAR(50) NOT NULL,
    OrderLineNo   INT         NOT NULL,
    OrderDateKey  INT         NOT NULL FOREIGN KEY REFERENCES dim.Date(DateKey),
    ShipDateKey   INT         NOT NULL FOREIGN KEY REFERENCES dim.Date(DateKey),
    CustomerKey   INT         NOT NULL FOREIGN KEY REFERENCES dim.Customer(CustomerKey),
    ProductKey    INT         NOT NULL FOREIGN KEY REFERENCES dim.Product(ProductKey),
    ShipModeKey   INT         NOT NULL FOREIGN KEY REFERENCES dim.ShipMode(ShipModeKey),
    GeographyKey  INT         NOT NULL FOREIGN KEY REFERENCES dim.Geography(GeographyKey),
    Sales         DECIMAL(18,2) NOT NULL,
    Quantity      INT           NOT NULL,
    Discount      DECIMAL(9,4)  NOT NULL,
    Profit        DECIMAL(18,2) NOT NULL,
    LoadTS        DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_OrderLine UNIQUE (OrderID, OrderLineNo)
);
GO

/**********************************************
                DATA LOADING
**********************************************/

PRINT 'Loading reference dimensions...';

-- ShipMode
MERGE dim.ShipMode AS tgt
USING (SELECT DISTINCT ShipMode FROM stg.Superstore_Typed WHERE ShipMode IS NOT NULL) AS src
    ON tgt.ShipMode = src.ShipMode
WHEN NOT MATCHED THEN INSERT (ShipMode) VALUES (src.ShipMode);
PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' ship modes loaded';

-- Category
MERGE dim.Category AS tgt
USING (SELECT DISTINCT Category FROM stg.Superstore_Typed WHERE Category IS NOT NULL) AS src
    ON tgt.Category = src.Category
WHEN NOT MATCHED THEN INSERT (Category) VALUES (src.Category);
PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' categories loaded';

-- SubCategory
MERGE dim.SubCategory AS tgt
USING (
    SELECT DISTINCT c.Category, s.SubCategory
    FROM stg.Superstore_Typed s
    JOIN dim.Category c ON c.Category = s.Category
    WHERE s.SubCategory IS NOT NULL
) AS src
    ON tgt.SubCategory = src.SubCategory
   AND (SELECT Category FROM dim.Category WHERE CategoryKey = tgt.CategoryKey) = src.Category
WHEN NOT MATCHED THEN
    INSERT (CategoryKey, SubCategory)
    VALUES ((SELECT CategoryKey FROM dim.Category WHERE Category = src.Category), src.SubCategory);
PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' subcategories loaded';

-- Geography
WITH geo AS
(
    SELECT DISTINCT
        Country, [State], City, Region,
        PostalCode = NULLIF(NULLIF(PostalCode,''),'0')
    FROM stg.Superstore_Typed
)
INSERT INTO dim.Geography (Country,[State],City,Region,PostalCode)
SELECT g.Country, g.[State], g.City, g.Region, a.PostalNormalized
FROM geo g
OUTER APPLY (
    SELECT CASE 
             WHEN TRY_CONVERT(INT, g.PostalCode) IS NOT NULL 
                  THEN RIGHT('00000' + CAST(TRY_CONVERT(INT,g.PostalCode) AS VARCHAR(10)), 
                             CASE WHEN LEN(CAST(TRY_CONVERT(INT,g.PostalCode) AS VARCHAR(10))) < 5 THEN 5 ELSE LEN(CAST(TRY_CONVERT(INT,g.PostalCode) AS VARCHAR(10))) END)
             ELSE g.PostalCode
           END AS PostalNormalized
) a
WHERE NOT EXISTS
(
    SELECT 1 
    FROM dim.Geography dg
    WHERE UPPER(dg.Country)=UPPER(g.Country)
      AND UPPER(dg.[State])=UPPER(g.[State])
      AND UPPER(dg.City)=UPPER(g.City)
      AND UPPER(dg.Region)=UPPER(g.Region)
      AND ISNULL(dg.PostalCode,'') = ISNULL(a.PostalNormalized,'')
);
PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' geography records loaded';

-- Customer SCD2
PRINT 'Loading customer dimension (SCD Type 2)...';
IF OBJECT_ID('tempdb..#CustomerToHash') IS NOT NULL
    DROP TABLE #CustomerToHash;

WITH snap AS
(
    SELECT
        CustomerID,
        MAX(CustomerName) AS CustomerName,
        MAX(Segment)      AS Segment,
        MAX(Region)       AS Region
    FROM stg.Superstore_Typed
    WHERE CustomerID IS NOT NULL
    GROUP BY CustomerID
),
to_hash AS
(
    SELECT s.*,
           CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', 
             CONCAT(UPPER(ISNULL(CustomerName,'')),'|',UPPER(ISNULL(Segment,'')),'|',UPPER(ISNULL(Region,'')))))
             AS HashDiff
    FROM snap s
)
SELECT * INTO #CustomerToHash FROM to_hash;

-- Create temp table for MERGE output
IF OBJECT_ID('tempdb..#tmp_out') IS NOT NULL DROP TABLE #tmp_out;
CREATE TABLE #tmp_out 
(
    [Action] VARCHAR(10),
    CustomerID VARCHAR(50),
    CustomerKey INT,
    IsCurrent BIT
);

MERGE dim.Customer AS tgt
USING #CustomerToHash AS src
   ON tgt.CustomerID = src.CustomerID AND tgt.IsCurrent = 1
WHEN NOT MATCHED BY TARGET THEN
    INSERT (CustomerID, CustomerName, Segment, Region, EffectiveFrom, EffectiveTo, IsCurrent, HashDiff)
    VALUES (src.CustomerID, src.CustomerName, src.Segment, src.Region, '1900-01-01', '9999-12-31', 1, src.HashDiff)
WHEN MATCHED AND tgt.HashDiff <> src.HashDiff THEN
    UPDATE SET tgt.EffectiveTo = CAST(GETDATE() AS DATE), tgt.IsCurrent = 0
OUTPUT
    $action, inserted.CustomerID, inserted.CustomerKey, inserted.IsCurrent INTO #tmp_out;

-- Insert new versions for changed customers
;WITH changed AS
(
    SELECT s.*
    FROM #CustomerToHash s
    WHERE EXISTS
    (
        SELECT 1 FROM dim.Customer c
        WHERE c.CustomerID = s.CustomerID AND c.IsCurrent = 0 AND c.EffectiveTo = CAST(GETDATE() AS DATE)
    )
)
INSERT INTO dim.Customer (CustomerID, CustomerName, Segment, Region, EffectiveFrom, EffectiveTo, IsCurrent, HashDiff)
SELECT CustomerID, CustomerName, Segment, Region, CAST(GETDATE() AS DATE), '9999-12-31', 1, HashDiff
FROM changed;

-- Cleanup temp tables
DROP TABLE #CustomerToHash;
DROP TABLE #tmp_out;
PRINT 'Customer dimension loaded';

-- Product SCD2
PRINT 'Loading product dimension (SCD Type 2)...';
WITH snap AS
(
    SELECT
        p.ProductID,
        MAX(p.ProductName)  AS ProductName,
        sc.SubCategoryKey
    FROM stg.Superstore_Typed p
    JOIN dim.Category c       ON c.Category = p.Category
    JOIN dim.SubCategory sc   ON sc.CategoryKey = c.CategoryKey AND sc.SubCategory = p.SubCategory
    WHERE p.ProductID IS NOT NULL
    GROUP BY p.ProductID, sc.SubCategoryKey
),
to_hash AS
(
    SELECT s.*,
           CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', 
             CONCAT(UPPER(ISNULL(ProductName,'')),'|',RIGHT(CONVERT(VARCHAR(32),HASHBYTES('MD5',CAST(SubCategoryKey AS VARBINARY(4))),2),32))))
             AS HashDiff
    FROM snap s
)
-- Store in temp table for reuse
SELECT * INTO #ProductToHash FROM to_hash;

MERGE dim.Product AS tgt
USING #ProductToHash AS src
   ON tgt.ProductID = src.ProductID AND tgt.IsCurrent = 1
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ProductID, ProductName, SubCategoryKey, EffectiveFrom, EffectiveTo, IsCurrent, HashDiff)
    VALUES (src.ProductID, src.ProductName, src.SubCategoryKey, '1900-01-01', '9999-12-31', 1, src.HashDiff)
WHEN MATCHED AND tgt.HashDiff <> src.HashDiff THEN
    UPDATE SET tgt.EffectiveTo = CAST(GETDATE() AS DATE), tgt.IsCurrent = 0;

-- Insert new versions for changed products
INSERT INTO dim.Product (ProductID, ProductName, SubCategoryKey, EffectiveFrom, EffectiveTo, IsCurrent, HashDiff)
SELECT s.ProductID, s.ProductName, s.SubCategoryKey, CAST(GETDATE() AS DATE), '9999-12-31', 1, s.HashDiff
FROM #ProductToHash s
WHERE EXISTS (
    SELECT 1 FROM dim.Product p
    WHERE p.ProductID = s.ProductID AND p.IsCurrent = 0 AND p.EffectiveTo = CAST(GETDATE() AS DATE)
);

DROP TABLE #ProductToHash;
PRINT 'Product dimension loaded';

-- Fact table
PRINT 'Loading fact table...';
WITH lines AS
(
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY s.OrderID ORDER BY s.ProductID, s.ProductName) AS OrderLineNo
    FROM stg.Superstore_Typed s
)
INSERT INTO fact.Sales
(
    OrderID, OrderLineNo, OrderDateKey, ShipDateKey, CustomerKey, ProductKey, ShipModeKey, GeographyKey,
    Sales, Quantity, Discount, Profit
)
SELECT
    l.OrderID,
    l.OrderLineNo,
    d1.DateKey,
    d2.DateKey,
    c.CustomerKey,
    p.ProductKey,
    sm.ShipModeKey,
    g.GeographyKey,
    l.Sales, l.Quantity, l.Discount, l.Profit
FROM lines l
JOIN dim.Date d1 ON d1.[Date] = l.OrderDate
JOIN dim.Date d2 ON d2.[Date] = l.ShipDate
JOIN dim.ShipMode sm ON sm.ShipMode = l.ShipMode
CROSS APPLY (
    SELECT TOP(1) c.*
    FROM dim.Customer c
    WHERE c.CustomerID = l.CustomerID
      AND c.IsCurrent = 1
    ORDER BY c.EffectiveTo DESC
) c
CROSS APPLY (
    SELECT TOP(1) p.*
    FROM dim.Product p
    WHERE p.ProductID = l.ProductID
      AND p.IsCurrent = 1
    ORDER BY p.EffectiveTo DESC
) p
OUTER APPLY (
    SELECT TOP(1) g1.GeographyKey
    FROM dim.Geography g1
    WHERE UPPER(g1.Country)=UPPER(l.Country)
      AND UPPER(g1.[State])=UPPER(l.[State])
      AND UPPER(g1.City)=UPPER(l.City)
      AND UPPER(g1.Region)=UPPER(l.Region)
      AND ISNULL(g1.PostalCode,'') = ISNULL(NULLIF(REPLACE(l.PostalCode,' ',''),''),'')
) g
WHERE NOT EXISTS (
    SELECT 1 FROM fact.Sales f
    WHERE f.OrderID = l.OrderID AND f.OrderLineNo = l.OrderLineNo
);
PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' fact records loaded';
GO

/**********************************************
                VIEWS
**********************************************/

PRINT 'Creating analytical views...';

-- Rolling 30-day revenue & profit per Region
IF OBJECT_ID('qa.v_Rolling30', 'V') IS NOT NULL DROP VIEW qa.v_Rolling30;
GO
CREATE VIEW qa.v_Rolling30 AS
SELECT
    dd.[Date],
    g.Region,
    SUM(fs.Sales)  OVER (PARTITION BY g.Region ORDER BY dd.[Date]
                          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS Sales_30D,
    SUM(fs.Profit) OVER (PARTITION BY g.Region ORDER BY dd.[Date]
                          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS Profit_30D
FROM fact.Sales fs
JOIN dim.Date dd ON dd.DateKey = fs.OrderDateKey
JOIN dim.Geography g ON g.GeographyKey = fs.GeographyKey;
GO

-- Customer cohort analysis
IF OBJECT_ID('qa.v_CustomerCohort','V') IS NOT NULL DROP VIEW qa.v_CustomerCohort;
GO
CREATE VIEW qa.v_CustomerCohort AS
WITH first_buy AS
(
    SELECT 
        c.CustomerKey,
        MIN(d.[Date]) AS FirstOrderDate
    FROM fact.Sales f
    JOIN dim.Customer c ON c.CustomerKey = f.CustomerKey
    JOIN dim.Date d ON d.DateKey = f.OrderDateKey
    GROUP BY c.CustomerKey
),
orders AS
(
    SELECT 
        c.CustomerKey,
        CAST(EOMONTH(d.[Date]) AS DATE) AS OrderMonth
    FROM fact.Sales f
    JOIN dim.Customer c ON c.CustomerKey = f.CustomerKey
    JOIN dim.Date d ON d.DateKey = f.OrderDateKey
    GROUP BY c.CustomerKey, CAST(EOMONTH(d.[Date]) AS DATE)
)
SELECT 
    fb.CustomerKey,
    CohortMonth = CAST(EOMONTH(fb.FirstOrderDate) AS DATE),
    OrderMonth  = o.OrderMonth,
    MonthsSince = DATEDIFF(MONTH, EOMONTH(fb.FirstOrderDate), o.OrderMonth),
    OrdersCount = COUNT(*)
FROM first_buy fb
JOIN orders o ON o.CustomerKey = fb.CustomerKey
GROUP BY fb.CustomerKey, CAST(EOMONTH(fb.FirstOrderDate) AS DATE), o.OrderMonth;
GO

-- Top products by subcategory
IF OBJECT_ID('qa.v_TopProductsBySubCat','V') IS NOT NULL DROP VIEW qa.v_TopProductsBySubCat;
GO
CREATE VIEW qa.v_TopProductsBySubCat AS
WITH agg AS
(
    SELECT sc.SubCategory, p.ProductName,
           SUM(f.Profit) AS Profit
    FROM fact.Sales f
    JOIN dim.Product p     ON p.ProductKey = f.ProductKey
    JOIN dim.SubCategory sc ON sc.SubCategoryKey = p.SubCategoryKey
    GROUP BY sc.SubCategory, p.ProductName
),
ranked AS
(
    SELECT *,
           RANK() OVER (PARTITION BY SubCategory ORDER BY Profit DESC) AS rnk,
           CAST(Profit * 1.0 /
                NULLIF(SUM(Profit) OVER (PARTITION BY SubCategory),0) AS DECIMAL(9,4)) AS ProfitShare
    FROM agg
)
SELECT SubCategory, ProductName, Profit, ProfitShare
FROM ranked
WHERE rnk <= 5;
GO

-- Suspicious discounts
IF OBJECT_ID('qa.v_SuspiciousDiscounts','V') IS NOT NULL DROP VIEW qa.v_SuspiciousDiscounts;
GO
CREATE VIEW qa.v_SuspiciousDiscounts AS
SELECT f.OrderID, f.OrderLineNo, f.Sales, f.Discount, f.Profit
FROM fact.Sales f
WHERE f.Discount > 0
  AND NOT EXISTS (
      SELECT 1
      FROM fact.Sales f2
      WHERE f2.OrderID = f.OrderID
        AND f2.OrderLineNo = f.OrderLineNo
        AND (f2.Profit / NULLIF(f2.Sales,0)) BETWEEN 0.05 AND 0.50
  );
GO

/**********************************************
                INDEXES
**********************************************/

PRINT 'Creating performance indexes...';
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Fact_Date' AND object_id = OBJECT_ID('fact.Sales'))
    CREATE INDEX IX_Fact_Date ON fact.Sales (OrderDateKey) INCLUDE (Sales, Profit, Quantity);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Fact_Region' AND object_id = OBJECT_ID('fact.Sales'))
    CREATE INDEX IX_Fact_Region ON fact.Sales (GeographyKey) INCLUDE (Sales, Profit);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_DimCustomer_NK' AND object_id = OBJECT_ID('dim.Customer'))
    CREATE INDEX IX_DimCustomer_NK ON dim.Customer (CustomerID) INCLUDE (IsCurrent, EffectiveFrom, EffectiveTo);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_DimProduct_NK' AND object_id = OBJECT_ID('dim.Product'))
    CREATE INDEX IX_DimProduct_NK ON dim.Product (ProductID) INCLUDE (IsCurrent, SubCategoryKey);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_DimGeo_lookup' AND object_id = OBJECT_ID('dim.Geography'))
    CREATE INDEX IX_DimGeo_lookup ON dim.Geography (Country, [State], City, Region, PostalCode);
GO

/**********************************************
                COMPLETION
**********************************************/

PRINT '';
PRINT 'Superstore Data Warehouse setup complete';
PRINT '';
GO

-- Verification queries

PRINT 'Verification queries:';
SELECT COUNT(*) AS RawCount FROM raw.Superstore;
PRINT '2. Staging data count:';
SELECT COUNT(*) AS StagingCount FROM stg.Superstore_Typed;
PRINT '3. Fact table count:';
SELECT COUNT(*) AS FactCount FROM fact.Sales;
PRINT '4. Dimension counts:';
SELECT 'Customer' AS Dim, COUNT(*) AS Rows FROM dim.Customer
UNION ALL SELECT 'Product', COUNT(*) FROM dim.Product
UNION ALL SELECT 'Date', COUNT(*) FROM dim.Date;
GO



 
