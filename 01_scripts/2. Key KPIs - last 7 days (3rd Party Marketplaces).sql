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

	PRINT('Delete Temp Tables');
END;


DECLARE -- declare reporting dates
		@start_date int = '20190110',
	    @end_date   int = '20220116'
;

BEGIN --Build Dates and Location matrix
	
	SELECT	-- build date range
		DISTINCT
		FIN_YEAR,
		ACC_WEEK_OF_YEAR

		INTO #REPORTING_DATES
	FROM DBO.DIM_DATE
		WHERE --DIM_DATE_KEY >= '20220110'
			DIM_DATE_KEY BETWEEN @start_date AND @end_date
	;	

	CREATE TABLE #ONLINE_STORES	-- build online stores list
	(
		LOCATION_TYPE		VARCHAR(5)		NOT NULL,
		LOCATION_NAME		VARCHAR(30)		NOT NULL,
		COUNTRY				VARCHAR(20)		NOT NULL,
		LOCATION_CODE_INT	VARCHAR(30)		NOT NULL,
	)
	;

	INSERT INTO #ONLINE_STORES -- new stores will need to be added here 
		(LOCATION_TYPE, LOCATION_NAME, COUNTRY, LOCATION_CODE_INT) 
	VALUES ('31', 'NEXT UK', 'United Kingdom', '780'),
		   ('31', 'eBay', 'Australia', '399'),
		   ('31', 'Amazon', 'United Kingdom', '798'),
		   ('31', 'TradeMe', 'New Zealand', '198'),
		   ('31', 'eBay UK', 'United Kingdom', '781'),
		   ('31', 'OttoDE', 'United Kingdom', '782'),
		   ('31', 'AmazonDE', 'United Kingdom', '785'),
		   ('31', 'eBayDE', 'United Kingdom', '786'),
		   ('31', 'TMall', 'Australia', '810'),
		   ('31', 'Amazon AU', 'Australia', '398'),
		   ('31', 'Catch Australia', 'Australia', '397'),
		   ('31', 'Onceit', 'New Zealand', '193')
	;

	SELECT -- Build final date and location matrix 
		* 
		INTO #DATE_STORE
	FROM #REPORTING_DATES 
		CROSS JOIN 	#ONLINE_STORES;
END;


BEGIN -- get metrics

	SELECT	-- get budget
		dd.fin_year																							AS	fin_year,
		dd.acc_week_of_year																					AS	acc_week_of_year,
		dl.location_type																					AS	location_type,
		dl.location_name																					AS	location_name,
		dl.country																							AS	country,
		dl.location_code																					AS	location_code_int,
		sum(budget.budget_amount)																			AS	BUDGETAMOUNT

		INTO #GET_BUDGET
	FROM	fact_budget_location_current_version budget
		join dim_location dl
			on (budget.dim_location_key = dl.dim_location_key)
		join	dim_date dd
			on 	(budget.dim_date_key = dd.dim_date_key)
	WHERE budget.dim_date_key between @start_date and @end_date
		and dl.location_type in (31)
		and dl.location_name not in (N'Wholesaler')
		and dl.country in (N'New Zealand', N'Australia', N'United Kingdom')
	GROUP BY dd.fin_year,
			 dd.acc_week_of_year,
			 dl.location_type,
			 dl.location_name,
		 	 dl.country,
			 dl.location_code
	;

	SELECT	-- get sales
		dd.fin_year																							AS	fin_year,
		dd.acc_week_of_year																					AS	acc_week_of_year,
		dl.location_type																					AS	location_type,
		dl.location_name																					AS	location_name,
		dl.country																							AS	country,
		dl.location_code																					AS	location_code_int,
		sum(fst.sale_amount_excl_gst)																		AS	SALES,
		count(distinct (Case when fst.dim_gift_voucher_key = -1 then fst.sale_transaction else NULL end))	AS	TRANSACTIONCOUNT,
		sum((Case when fst.dim_gift_voucher_key = -1 then fst.sale_qty else NULL end))						AS	UNITSSOLD

		INTO #GET_SALES
	FROM fact_sales_trans fst
		join dim_location dl
			on (fst.dim_location_key = dl.dim_location_key)
		join	dim_date dd
			on 	(fst.dim_date_key = dd.dim_date_key)
	WHERE fst.dim_date_key between @start_date and @end_date
		and dl.location_type in (31)
		and dl.location_name not in (N'Wholesaler')
		and dl.country in (N'New Zealand', N'Australia', N'United Kingdom')
	GROUP BY dd.fin_year,
			 dd.acc_week_of_year,
			 dl.location_type,
			 dl.location_name,
		 	 dl.country,
			 dl.location_code
	;

	SELECT	-- get web metrics
		dd.fin_year																							AS	fin_year,
		dd.acc_week_of_year																					AS	acc_week_of_year,
		dl.location_type																					AS	location_type,
		dl.location_name																					AS	location_name,
		dl.country																							AS	country,
		dl.location_code																					AS	location_code_int,
		sum(fwss.visits)																					AS	VISITS

		INTO #GET_WEB_METRICS
	FROM fact_web_stats_session	fwss
		join dim_location dl
			on 	(fwss.dim_location_key = dl.dim_location_key)
		join	dim_date dd
			on 	(fwss.dim_date_key = dd.dim_date_key)
	WHERE fWSS.dim_date_key between @start_date and @end_date
		and dl.location_type in (31)
		and dl.location_name not in (N'Wholesaler')
		and dl.country in (N'New Zealand', N'Australia', N'United Kingdom')
	GROUP BY dd.fin_year,
			 dd.acc_week_of_year,
			 dl.location_type,
			 dl.location_name,
		 	 dl.country,
			 dl.location_code
	;

END;

BEGIN	-- create reporting table
	
	SELECT
		ds.*,
		gb.BUDGETAMOUNT				AS BUDGETAMOUNT,
		gs.SALES					AS SALES,
		gs.TRANSACTIONCOUNT			AS TRANSACTIONCOUNT,
		gs.UNITSSOLD				AS UNITSSOLD,
		gwm.VISITS					AS VISITS

	FROM #DATE_STORE ds
		left join #GET_BUDGET gb 
			on ds.fin_year					                                     = gb.fin_year
				and ds.acc_week_of_year					                         = gb.acc_week_of_year 
				and ds.location_type	 COLLATE SQL_Latin1_General_CP1_CI_AS	 = gb.location_type
				and ds.location_name	 COLLATE SQL_Latin1_General_CP1_CI_AS	 = gb.location_name
				and ds.location_code_int COLLATE SQL_Latin1_General_CP1_CI_AS	 = gb.location_code_int
		join #GET_SALES gs 
			on ds.fin_year					 					                 = gs.fin_year
				and ds.acc_week_of_year		 					                 = gs.acc_week_of_year 
				and ds.location_type	 COLLATE SQL_Latin1_General_CP1_CI_AS	 = gs.location_type
				and ds.location_name	 COLLATE SQL_Latin1_General_CP1_CI_AS	 = gs.location_name
				and ds.location_code_int COLLATE SQL_Latin1_General_CP1_CI_AS	 = gs.location_code_int
		left join #GET_WEB_METRICS gwm 
			on ds.fin_year					 					                 = gwm.fin_year
				and ds.acc_week_of_year		 					                 = gwm.acc_week_of_year 
				and ds.location_type	 COLLATE SQL_Latin1_General_CP1_CI_AS	 = gwm.location_type
				and ds.location_name	 COLLATE SQL_Latin1_General_CP1_CI_AS	 = gwm.location_name
				and ds.location_code_int COLLATE SQL_Latin1_General_CP1_CI_AS	 = gwm.location_code_int
END;

