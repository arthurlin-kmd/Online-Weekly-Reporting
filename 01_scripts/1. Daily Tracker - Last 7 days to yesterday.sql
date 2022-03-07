SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

BEGIN --Delete Temp Tables
	IF OBJECT_ID('tempdb..#REPORTING_DATES') IS NOT NULL
		DROP TABLE #REPORTING_DATES;
	IF OBJECT_ID('tempdb..#ONLINE_STORES') IS NOT NULL
		DROP TABLE #ONLINE_STORES;
	IF OBJECT_ID('tempdb..#DATE_STORE') IS NOT NULL
		DROP TABLE #DATE_STORE;
	IF OBJECT_ID('tempdb..#GET_BUDGET') IS NOT NULL
		DROP TABLE #GET_BUDGET;
	IF OBJECT_ID('tempdb..#GET_SALES') IS NOT NULL
		DROP TABLE #GET_SALES;
	IF OBJECT_ID('tempdb..#GET_WEB_METRICS') IS NOT NULL
		DROP TABLE #GET_WEB_METRICS;
	IF OBJECT_ID('tempdb..#GET_CLEARANCE_SALES') IS NOT NULL
		DROP TABLE #GET_CLEARANCE_SALES;

	PRINT('Delete Temp Tables');
END;


DECLARE -- declare reporting dates
		@start_date int = '20220110',
	    @end_date   int = '20220116'
;

BEGIN --Build Dates and Location matrix
	
	SELECT	-- build date range
		DIM_DATE_KEY,
		DATE_TEXT,
		FIN_YEAR,
		WEEKDAY,
		DAY_OF_WEEK,
		FIN_DAY_OF_YEAR,
		ACC_WEEK_OF_YEAR

		INTO #REPORTING_DATES
	FROM DBO.DIM_DATE
		WHERE DIM_DATE_KEY BETWEEN @start_date AND @end_date
	;	

	CREATE TABLE #ONLINE_STORES	-- build online stores list
	(
		LOCATION_CODE		VARCHAR(5)		NOT NULL PRIMARY KEY,
		LOCATION_NAME		VARCHAR(30)		NOT NULL,
		COUNTRY				VARCHAR(20)		NOT NULL,
		LOCATION_TYPE		VARCHAR(30)		NOT NULL,
		LOCATION_TYPE_DESC	VARCHAR(30)		NOT NULL,
	)
	;

	INSERT INTO #ONLINE_STORES -- new stores will need to be added here 
		(LOCATION_CODE, LOCATION_NAME, COUNTRY, LOCATION_TYPE, LOCATION_TYPE_DESC) 
	VALUES ('196', 'International Online', 'United States', '50', 'International'),
			('199', 'Online NZ', 'New Zealand', '30', 'Mail Order Store'),
			('299', 'Online AU', 'Australia', '30', 'Mail Order Store'),
			('799', 'Online UK', 'United Kingdom', '30', 'Mail Order Store')
	;

	SELECT -- Build final date and location matrix 
		* 
		INTO #DATE_STORE
	FROM #REPORTING_DATES 
		CROSS JOIN 	#ONLINE_STORES;
END;

BEGIN -- Build Budget amount, sales, visits, transaction count,units sold, traffic source - paid, traffic source - organic, clearance sales, and bounces
	
	SELECT	-- get budget
			dl.location_code			AS location_code,
			budget.dim_date_key			AS dim_date_key,
			sum(budget.budget_amount)	AS BUDGETAMOUNT

		INTO #GET_BUDGET
	FROM	fact_budget_location_current_version budget
		join	dim_location dl  on budget.dim_location_key = dl.dim_location_key
	WHERE budget.dim_date_key between @start_date and @end_date
		and dl.location_code in (N'199', N'299', N'799', N'196')
	GROUP BY dl.location_code,
			 budget.dim_date_key
	;

	SELECT	-- get sales
		dl.location_code																					AS	location_code,
		fst.dim_date_key																					AS	dim_date_key,
		sum(fst.sale_amount_excl_gst)																		AS	SALES,
		count(distinct (Case when fst.dim_gift_voucher_key = -1 then fst.sale_transaction else NULL end))	AS	TRANSACTIONCOUNT,
		sum((Case when fst.dim_gift_voucher_key = -1 then fst.sale_qty else NULL end))						AS	UNITSSOLD

		INTO #GET_SALES
	FROM fact_sales_trans fst
		join dim_location dl on fst.dim_location_key = dl.dim_location_key
	WHERE fst.dim_date_key between @start_date and @end_date
		and dl.location_code in (N'199', N'299', N'799', N'196')
	GROUP BY dl.location_code,
			 fst.dim_date_key
	;

	SELECT	-- get web metrics
		dl.location_code															AS	location_code,
		fwss.dim_date_key															AS	dim_date_key,
		sum(fwss.visits)															AS	VISITS,
		sum(fwss.bounces)															AS	BOUNCES,
		sum((Case when dwss.medium in (N'cpc') then fwss.visits else NULL end))		AS	TRAFFICSOURCEPAID,
		sum((Case when dwss.medium in (N'organic') then fwss.visits else NULL end))	AS	TRAFFICSOURCEORGANIC
		
		INTO #GET_WEB_METRICS
	FROM fact_web_stats_session fwss
		join dim_location dl 
			on 	(fwss.dim_location_key = dl.dim_location_key)
		join	dim_web_stats_source dwss
			on 	(fwss.dim_web_stats_source_key = dwss.dim_web_stats_source_key)
	WHERE fwss.dim_date_key between @start_date and @end_date
		and dl.location_code in (N'199', N'299', N'799', N'196')
	GROUP BY dl.location_code,
			 fwss.dim_date_key
	;


	SELECT
		dl.location_code															AS	location_code,
		fst.dim_date_key															AS	dim_date_key,
		sum(fst.sale_amount_excl_gst)												AS	CLEARANCESALES

		INTO #GET_CLEARANCE_SALES
	FROM	fact_sales_trans fst
		join	dim_location dl  
			on	fst.dim_location_key = dl.dim_location_key
		join	dim_product_status dps
			on	fst.dim_product_status_key = dps.dim_product_status_key
	WHERE fst.dim_date_key between @start_date and @end_date
		and dl.location_code in (N'199', N'299', N'799', N'196')
		and	dps.m3_status = 50 -- product clearance code
	GROUP BY dl.location_code,
			 fst.dim_date_key
	;
END;

BEGIN -- create reporting table

	WITH CTE1 AS (
	SELECT
		ds.*,
		gb.BUDGETAMOUNT													AS BUDGETAMOUNT,
		gs.SALES														AS SALES,
		gs.TRANSACTIONCOUNT												AS TRANSACTIONCOUNT,
		gs.UNITSSOLD													AS UNITSSOLD,
		gwm.VISITS														AS VISITS,
		gwM.BOUNCES														AS BOUNCES,
		gwm.TRAFFICSOURCEPAID											AS TRAFFICSOURCEPAID,
		gwm.TRAFFICSOURCEORGANIC										AS TRAFFICSOURCEORGANIC,
		gcs.CLEARANCESALES												AS CLEARANCESALES

	FROM #DATE_STORE ds
		left join #GET_BUDGET gb 
			on ds.LOCATION_CODE		COLLATE SQL_Latin1_General_CP1_CI_AS = gb.location_code 
				and ds.dim_date_key = gb.dim_date_key
		left join #GET_SALES gs 
			on ds.LOCATION_CODE		COLLATE SQL_Latin1_General_CP1_CI_AS = gs.location_code 
				and ds.dim_date_key = gs.dim_date_key
		left join #GET_WEB_METRICS gwm 
			on ds.LOCATION_CODE		COLLATE SQL_Latin1_General_CP1_CI_AS = gwm.location_code 
				and ds.dim_date_key = gwm.dim_date_key
		left join #GET_CLEARANCE_SALES gcs 
			on ds.LOCATION_CODE		COLLATE SQL_Latin1_General_CP1_CI_AS = gcs.location_code 
				and ds.dim_date_key = gcs.dim_date_key
	),
	CTE2 AS (
		SELECT
			DIM_DATE_KEY,
			DATE_TEXT,
			FIN_YEAR,
			WEEKDAY,
			DAY_OF_WEEK,
			FIN_DAY_OF_YEAR,
			ACC_WEEK_OF_YEAR,
			LOCATION_CODE,
			LOCATION_NAME,
			COUNTRY,
			LOCATION_TYPE,
			LOCATION_TYPE_DESC,
			BUDGETAMOUNT,
			ISNULL(SALES,0)															AS SALES,
			ISNULL(TRANSACTIONCOUNT,0)												AS TRANSACTIONCOUNT,
			ISNULL(UNITSSOLD,0)														AS UNITSSOLD,
			ISNULL(VISITS,0)														AS VISITS,
			ISNULL(BOUNCES,0)														AS BOUNCES,
			ISNULL(TRAFFICSOURCEPAID,0)												AS TRAFFICSOURCEPAID,
			ISNULL(TRAFFICSOURCEORGANIC,0)											AS TRAFFICSOURCEORGANIC,
			ISNULL(CLEARANCESALES,0)												AS CLEARANCESALES
		FROM CTE1
	)
	SELECT
		*,
		ISNULL((SALES - BUDGETAMOUNT) / NULLIF(CAST(BUDGETAMOUNT AS FLOAT),0),0)	AS VARIANCE_TO_BUDGET,
		ISNULL(TRANSACTIONCOUNT / NULLIF(CAST(VISITS AS FLOAT),0),0)				AS CONVERSION_RATE,
		ISNULL(SALES / NULLIF(CAST(TRANSACTIONCOUNT AS FLOAT),0),0)					AS AVG_ORDER_VALUE,
		ISNULL(UNITSSOLD / NULLIF(CAST(TRANSACTIONCOUNT AS FLOAT),0),0)				AS ITEM_PER_DOCKET,
		ISNULL(CLEARANCESALES / NULLIF(CAST(SALES AS FLOAT),0),0)					AS CLEARANCE_SALES_PERCENTAGE
	FROM CTE2
END;
