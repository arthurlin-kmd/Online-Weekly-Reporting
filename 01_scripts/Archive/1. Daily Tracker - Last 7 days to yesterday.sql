/*
Report: Daily Tracker - Last 7 days to yesterday
Job: 12006
Report Cache Used: No

Number of Columns Returned:		16
Number of Temp Tables:		0

Total Number of Passes:		2
Number of SQL Passes:		2
Number of Analytical Passes:		0

Tables Accessed:
dim_date
dim_location
dim_product_status
dim_web_stats_source
fact_budget_location_current_version
fact_sales_trans
fact_web_stats_session


SQL Statements:
*/
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED


select	distinct coalesce(pa11.dim_date_key, pa12.dim_date_key, pa13.dim_date_key, pa15.dim_date_key)  dim_date_key,
	a17.date_text  date_text,
	coalesce(pa11.location_code, pa12.location_code, pa13.location_code, pa15.location_code)  location_code,
	a16.location_name  location_name,
	a16.country  country,
	a16.location_type  location_type,
	a16.location_type_desc  location_type_desc,
	pa11.BUDGETAMOUNT  BUDGETAMOUNT,
	pa11.SALES  SALES,
	pa11.VISITS  VISITS,
	pa12.WJXBFS1  TRANSACTIONCOUNT,
	pa12.WJXBFS2  UNITSSOLD,
	pa13.TRAFFICSOURCEPAID  TRAFFICSOURCEPAID,
	pa13.TRAFFICSOURCEORGANIC  TRAFFICSOURCEORGANIC,
	pa15.CLEARANCESALES  CLEARANCESALES,
	pa11.WJXBFS1  WJXBFS1 /* bounces, sys gen col name since its not exposed to user but used to calc derived Metrics */
from	(select	coalesce(pa11.location_code, pa12.location_code, pa13.location_code)  location_code,
		coalesce(pa11.dim_date_key, pa12.dim_date_key, pa13.dim_date_key)  dim_date_key,
		pa11.BUDGETAMOUNT  BUDGETAMOUNT,
		pa12.SALES  SALES,
		pa13.VISITS  VISITS,
		pa13.WJXBFS1  WJXBFS1
	from	(select	a12.location_code  location_code,
			a11.dim_date_key  dim_date_key,
			sum(a11.budget_amount)  BUDGETAMOUNT
		from	fact_budget_location_current_version	a11
			join	dim_location	a12
			  on 	(a11.dim_location_key = a12.dim_location_key)
			join	dim_date	a13
			  on 	(a11.dim_date_key = a13.dim_date_key)
		where	(a13.full_date_datetime between '2022-01-10' and '2022-01-16'
		 and a12.location_code in (N'199', N'299', N'799', N'196')
		 and a12.location_code in (N'199', N'299', N'799', N'812', N'196', N'811'))
		group by	a12.location_code,
			a11.dim_date_key
		)	pa11
		full outer join	(select	a12.location_code  location_code,
			a11.dim_date_key  dim_date_key,
			sum(a11.sale_amount_excl_gst)  SALES,
			count(distinct (Case when a11.dim_gift_voucher_key = -1 then a11.sale_transaction else NULL end))  TRANSACTIONCOUNT,
			sum((Case when a11.dim_gift_voucher_key = -1 then a11.sale_qty else NULL end))  UNITSSOLD,
			max((Case when a11.dim_gift_voucher_key = -1 then 1 else 0 end))  GODWFLAG5_1
		from	fact_sales_trans	a11
			join	dim_location	a12
			  on 	(a11.dim_location_key = a12.dim_location_key)
			join	dim_date	a13
			  on 	(a11.dim_date_key = a13.dim_date_key)
		where	(a13.full_date_datetime between '2022-01-10' and '2022-01-16'
		 and a12.location_code in (N'199', N'299', N'799', N'196')
		 and a12.location_code in (N'199', N'299', N'799', N'812', N'196', N'811'))
		group by	a12.location_code,
			a11.dim_date_key
		)	pa12
		  on 	(pa11.dim_date_key = pa12.dim_date_key and 
		pa11.location_code = pa12.location_code)
		full outer join	(select	a12.location_code  location_code,
			a11.dim_date_key  dim_date_key,
			sum(a11.visits)  VISITS,
			sum(a11.bounces)  WJXBFS1
		from	fact_web_stats_session	a11
			join	dim_location	a12
			  on 	(a11.dim_location_key = a12.dim_location_key)
			join	dim_date	a13
			  on 	(a11.dim_date_key = a13.dim_date_key)
		where	(a13.full_date_datetime between '2022-01-10' and '2022-01-16'
		 and a12.location_code in (N'199', N'299', N'799', N'196')
		 and a12.location_code in (N'199', N'299', N'799', N'812', N'196', N'811'))
		group by	a12.location_code,
			a11.dim_date_key
		)	pa13
		  on 	(coalesce(pa11.dim_date_key, pa12.dim_date_key) = pa13.dim_date_key and 
		coalesce(pa11.location_code, pa12.location_code) = pa13.location_code)
	)	pa11
	full outer join	(select	pa01.dim_date_key  dim_date_key,
		pa01.location_code  location_code,
		pa01.TRANSACTIONCOUNT  WJXBFS1,
		pa01.UNITSSOLD  WJXBFS2
	from	(select	a12.location_code  location_code,
			a11.dim_date_key  dim_date_key,
			sum(a11.sale_amount_excl_gst)  SALES,
			count(distinct (Case when a11.dim_gift_voucher_key = -1 then a11.sale_transaction else NULL end))  TRANSACTIONCOUNT,
			sum((Case when a11.dim_gift_voucher_key = -1 then a11.sale_qty else NULL end))  UNITSSOLD,
			max((Case when a11.dim_gift_voucher_key = -1 then 1 else 0 end))  GODWFLAG5_1
		from	fact_sales_trans	a11
			join	dim_location	a12
			  on 	(a11.dim_location_key = a12.dim_location_key)
			join	dim_date	a13
			  on 	(a11.dim_date_key = a13.dim_date_key)
		where	(a13.full_date_datetime between '2022-01-10' and '2022-01-16'
		 and a12.location_code in (N'199', N'299', N'799', N'196')
		 and a12.location_code in (N'199', N'299', N'799', N'812', N'196', N'811'))
		group by	a12.location_code,
			a11.dim_date_key
		)	pa01
	where	pa01.GODWFLAG5_1 = 1
	)	pa12
	  on 	(pa11.dim_date_key = pa12.dim_date_key and 
	pa11.location_code = pa12.location_code)
	full outer join	(select	a12.location_code  location_code,
		a11.dim_date_key  dim_date_key,
		sum((Case when a13.medium in (N'cpc') then a11.visits else NULL end))  TRAFFICSOURCEPAID,
		sum((Case when a13.medium in (N'organic') then a11.visits else NULL end))  TRAFFICSOURCEORGANIC
	from	fact_web_stats_session	a11
		join	dim_location	a12
		  on 	(a11.dim_location_key = a12.dim_location_key)
		join	dim_web_stats_source	a13
		  on 	(a11.dim_web_stats_source_key = a13.dim_web_stats_source_key)
		join	dim_date	a14
		  on 	(a11.dim_date_key = a14.dim_date_key)
	where	(a14.full_date_datetime between '2022-01-10' and '2022-01-16'
	 and a12.location_code in (N'199', N'299', N'799', N'196')
	 and a12.location_code in (N'199', N'299', N'799', N'812', N'196', N'811')
	 and (a13.medium in (N'cpc')
	 or a13.medium in (N'organic')))
	group by	a12.location_code,
		a11.dim_date_key
	)	pa13
	  on 	(coalesce(pa11.dim_date_key, pa12.dim_date_key) = pa13.dim_date_key and 
	coalesce(pa11.location_code, pa12.location_code) = pa13.location_code)
	full outer join	(select	a12.location_code  location_code,
		a11.dim_date_key  dim_date_key,
		sum(a11.sale_amount_excl_gst)  CLEARANCESALES
	from	fact_sales_trans	a11
		join	dim_location	a12
		  on 	(a11.dim_location_key = a12.dim_location_key)
		join	dim_product_status	a13
		  on 	(a11.dim_product_status_key = a13.dim_product_status_key)
		join	dim_date	a14
		  on 	(a11.dim_date_key = a14.dim_date_key)
	where	(a14.full_date_datetime between '2022-01-10' and '2022-01-16'
	 and a12.location_code in (N'199', N'299', N'799', N'196')
	 and a12.location_code in (N'199', N'299', N'799', N'812', N'196', N'811')
	 and a13.m3_status in (50))
	group by	a12.location_code,
		a11.dim_date_key
	)	pa15
	  on 	(coalesce(pa11.dim_date_key, pa12.dim_date_key, pa13.dim_date_key) = pa15.dim_date_key and 
	coalesce(pa11.location_code, pa12.location_code, pa13.location_code) = pa15.location_code)
	join	dim_location	a16
	  on 	(coalesce(pa11.location_code, pa12.location_code, pa13.location_code, pa15.location_code) = a16.location_code)
	join	dim_date	a17
	  on 	(coalesce(pa11.dim_date_key, pa12.dim_date_key, pa13.dim_date_key, pa15.dim_date_key) = a17.dim_date_key)

/*
[Analytical engine calculation steps:
	1.  Calculate derived elements on: <Location> in the dataset
	2.  Calculate metric: <Items Per Docket> in the dataset
	3.  Calculate metric: <Clearance Sales %> in the dataset
	4.  Calculate metric: <Bounce Rate> in the dataset
	5.  Calculate derived elements on: <Location> in the view
	6.  Calculate metric: <Variance> in the view
	7.  Calculate metric: <Conv. Rate> in the view
	8.  Calculate metric: <Avg Order Value> in the view
	9.  Calculate metric: <Items Per Docket> in the view
	10.  Calculate metric: <Clearance Sales %> in the view
	11.  Calculate metric: <Bounce Rate> in the view
	12.  Perform cross-tabbing
]
*/