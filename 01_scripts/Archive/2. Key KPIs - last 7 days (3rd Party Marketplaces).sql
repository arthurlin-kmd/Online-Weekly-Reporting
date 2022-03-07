/*
Report: Key KPIs - last 7 days (3rd Party Marketplaces)
Job: 11985
Report Cache Used: Yes

Query Engine Execution Start Time:		18/01/2022 3:19:48 p.m.
Query Engine Execution Finish Time:		18/01/2022 3:19:56 p.m.

Query Generation Time:		0:00:00.06
Total Elapsed Time in Query Engine:		0:00:07.76
	Sum of Query Execution Time:		0:00:07.33
	Sum of Data Fetching and Processing Time:		0:00:00.00
		Sum of Data Transfer from Datasource(s) Time:		0:00:00.00
	Sum of Analytical Processing Time:		0:00:00.00
	Sum of Other Processing Time:		0:00:00.43

Sum of Template Calculate Time		0:00:00.00
Sum of AE Data Persisting Time		0:00:00.00
Sum of Cube Publish Time		0:00:00.00


Number of Rows Returned:		6
Number of Columns Returned:		10
Number of Temp Tables:		0

Total Number of Passes:		3
Number of Datasource Query Passes:		3
Number of Analytical Query Passes:		0

DB User:		BIService
DB Instance:		Kathmandu Data Warehouse

Tables Accessed:
fact_web_stats_session	
dim_date	
dim_location	
fact_sales_trans	


SQL Statements:

Pass0 - 	Query Pass Start Time:		18/01/2022 2:19:48 a.m.
	Query Pass End Time:		18/01/2022 2:19:48 a.m.
	Query Execution:	0:00:00.00
	Data Fetching and Processing:	0:00:00.00
	  Data Transfer from Datasource(s):	0:00:00.00
	Other Processing:	0:00:00.02
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

Pass1 - 	Query Pass Start Time:		18/01/2022 2:19:48 a.m.
	Query Pass End Time:		18/01/2022 2:19:55 a.m.
	Query Execution:	0:00:07.32
	Data Fetching and Processing:	0:00:00.00
	  Data Transfer from Datasource(s):	0:00:00.00
	Other Processing:	0:00:00.02
	Rows selected: 6
*/
select	distinct coalesce(pa11.acc_week_of_year, pa12.acc_week_of_year)  acc_week_of_year,
	coalesce(pa11.country, pa12.country)  country,
	coalesce(pa11.location_type, pa12.location_type)  location_type,
	a13.location_type_desc  location_type_desc,
	coalesce(pa11.location_code_int, pa12.location_code_int)  location_code_int,
	coalesce(pa11.location_name, pa12.location_name)  location_name,
	pa11.SALES  SALES,
	pa12.WJXBFS1  TRANSACTIONCOUNT,
	pa12.WJXBFS2  UNITSSOLD,
	pa11.VISITS  VISITS
from	(select	coalesce(pa11.acc_week_of_year, pa12.acc_week_of_year)  acc_week_of_year,
		coalesce(pa11.location_type, pa12.location_type)  location_type,
		coalesce(pa11.location_name, pa12.location_name)  location_name,
		coalesce(pa11.country, pa12.country)  country,
		coalesce(pa11.location_code_int, pa12.location_code_int)  location_code_int,
		pa11.SALES  SALES,
		pa12.VISITS  VISITS
	from	(select	a13.acc_week_of_year  acc_week_of_year,
			a12.location_type  location_type,
			a12.location_name  location_name,
			a12.country  country,
			a12.location_code  location_code_int,
			sum(a11.sale_amount_excl_gst)  SALES,
			count(distinct (Case when a11.dim_gift_voucher_key = -1 then a11.sale_transaction else NULL end))  TRANSACTIONCOUNT,
			sum((Case when a11.dim_gift_voucher_key = -1 then a11.sale_qty else NULL end))  UNITSSOLD,
			max((Case when a11.dim_gift_voucher_key = -1 then 1 else 0 end))  GODWFLAG4_1
		from	fact_sales_trans	a11
			join	dim_location	a12
			  on 	(a11.dim_location_key = a12.dim_location_key)
			join	dim_date	a13
			  on 	(a11.dim_date_key = a13.dim_date_key)
		where	(a13.full_date_datetime between '2022-01-10' and '2022-01-16'
		 and a12.location_type in (31)
		 and a12.location_name not in (N'Wholesaler')
		 and a12.country in (N'New Zealand', N'Australia', N'United Kingdom'))
		group by	a13.acc_week_of_year,
			a12.location_type,
			a12.location_name,
			a12.country,
			a12.location_code
		)	pa11
		full outer join	(select	a13.acc_week_of_year  acc_week_of_year,
			a12.location_type  location_type,
			a12.location_name  location_name,
			a12.country  country,
			a12.location_code  location_code_int,
			sum(a11.visits)  VISITS
		from	fact_web_stats_session	a11
			join	dim_location	a12
			  on 	(a11.dim_location_key = a12.dim_location_key)
			join	dim_date	a13
			  on 	(a11.dim_date_key = a13.dim_date_key)
		where	(a13.full_date_datetime between '2022-01-10' and '2022-01-16'
		 and a12.location_type in (31)
		 and a12.location_name not in (N'Wholesaler')
		 and a12.country in (N'New Zealand', N'Australia', N'United Kingdom'))
		group by	a13.acc_week_of_year,
			a12.location_type,
			a12.location_name,
			a12.country,
			a12.location_code
		)	pa12
		  on 	(pa11.acc_week_of_year = pa12.acc_week_of_year and 
		pa11.country = pa12.country and 
		pa11.location_code_int = pa12.location_code_int and 
		pa11.location_name = pa12.location_name and 
		pa11.location_type = pa12.location_type)
	)	pa11
	full outer join	(select	pa01.location_code_int  location_code_int,
		pa01.country  country,
		pa01.location_name  location_name,
		pa01.location_type  location_type,
		pa01.acc_week_of_year  acc_week_of_year,
		pa01.TRANSACTIONCOUNT  WJXBFS1,
		pa01.UNITSSOLD  WJXBFS2
	from	(select	a13.acc_week_of_year  acc_week_of_year,
			a12.location_type  location_type,
			a12.location_name  location_name,
			a12.country  country,
			a12.location_code  location_code_int,
			sum(a11.sale_amount_excl_gst)  SALES,
			count(distinct (Case when a11.dim_gift_voucher_key = -1 then a11.sale_transaction else NULL end))  TRANSACTIONCOUNT,
			sum((Case when a11.dim_gift_voucher_key = -1 then a11.sale_qty else NULL end))  UNITSSOLD,
			max((Case when a11.dim_gift_voucher_key = -1 then 1 else 0 end))  GODWFLAG4_1
		from	fact_sales_trans	a11
			join	dim_location	a12
			  on 	(a11.dim_location_key = a12.dim_location_key)
			join	dim_date	a13
			  on 	(a11.dim_date_key = a13.dim_date_key)
		where	(a13.full_date_datetime between '2022-01-10' and '2022-01-16'
		 and a12.location_type in (31)
		 and a12.location_name not in (N'Wholesaler')
		 and a12.country in (N'New Zealand', N'Australia', N'United Kingdom'))
		group by	a13.acc_week_of_year,
			a12.location_type,
			a12.location_name,
			a12.country,
			a12.location_code
		)	pa01
	where	pa01.GODWFLAG4_1 = 1
	)	pa12
	  on 	(pa11.acc_week_of_year = pa12.acc_week_of_year and 
	pa11.country = pa12.country and 
	pa11.location_code_int = pa12.location_code_int and 
	pa11.location_name = pa12.location_name and 
	pa11.location_type = pa12.location_type)
	join	dim_location	a13
	  on 	(coalesce(pa11.location_type, pa12.location_type) = a13.location_type)
/*
Pass2 - 	Query Pass Start Time:		18/01/2022 2:19:55 a.m.
	Query Pass End Time:		18/01/2022 2:19:55 a.m.
	Query Execution:	0:00:00.00
	Data Fetching and Processing:	0:00:00.00
	  Data Transfer from Datasource(s):	0:00:00.00
	Other Processing:	0:00:00.00
[Populate Report Data]

[Analytical engine calculation steps:
	1.  Calculate derived elements on: <Location: Country> in the dataset
	2.  Perform dynamic aggregation over <Week Of Year (Accounting), Location: Type, Location: Code>
	3.  Calculate derived elements on: <Location: Country> in the view
	4.  Perform cross-tabbing
]
*/