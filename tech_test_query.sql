-- I'm not sure how to define dt_report for a ticket lasted for a range of time, 
-- that is, open_time and close_time were at different dates
-- so I define the date of close_time as dt_report
with dates as 
(
	select date_trunc('day', dd):: date as dt_report
	from generate_series
					( '2020-06-01'::timestamp 
					, '2020-09-30'::timestamp
					, '1 day'::interval) dd
),
august as 
(
    select 
        t.login_hash,
        t.server_hash,
        t.symbol,
				trade_date,
				t.volume,
				sum(volume) over(partition by login_hash, server_hash, symbol order by trade_date) as sum_volume_2020_08
				from 
		(
			select login_hash, server_hash, symbol, date(close_time) as trade_date, sum(volume) as volume
			from trades 
			where close_time >= '2020-08-01' and close_time < '2020-09-01'
			group by login_hash, server_hash, symbol, trade_date
		) t
),
agg as 
(
    select 
        t.login_hash,
        t.server_hash,
        t.symbol,
				trade_date,
        t.volume,
        sum(t.volume) over (partition by t.login_hash, t.server_hash, t.symbol order by t.trade_date) as sum_volume_prev_all,
        sum(t.volume) over (partition by t.login_hash, t.server_hash, t.symbol order by t.trade_date range between interval '6 day' preceding and current row) as sum_volume_prev_7d,
        t.cnt,
				sum(cnt) over (partition by t.login_hash order by t.trade_date range between interval '6 day' preceding and current row) as trade_count_prev_7d
    from 
		(
			select login_hash, server_hash, symbol, date(close_time) as trade_date, sum(volume) as volume, count(ticket_hash) as cnt
			from trades 
			group by login_hash, server_hash, symbol, trade_date
		) t
),
dense_ranking as 
(
	select *
	,
	dense_rank() over(partition by login_hash, symbol order by sum_volume_prev_7d desc) as rank_volume_symbol_prev_7d,
	dense_rank() over(partition by login_hash order by trade_count_prev_7d desc) as rank_count_prev_7d
	from agg
)
,
first_trade as 
(
	select login_hash, server_hash, symbol, min(open_time) as date_first_trade
	from trades
	group by login_hash, server_hash, symbol
),
-- select * from dense_ranking order by trade_date

enabled_users as 
(
	select distinct login_hash , server_hash, currency
	from users
	where enable = 1
)

select row_number() over() as id, * from
(
	select 

	*
	from 
	(
		select 
		dates.*,
		data1.login_hash,
		data1.server_hash,
		data1.symbol,
		data1.currency,
		data1.sum_volume_prev_7d,
		data1.sum_volume_prev_all,
-- 		data1.volume,data1.trade_count_prev_7d,
		data1.rank_volume_symbol_prev_7d,
		data1.rank_count_prev_7d,
		august.sum_volume_2020_08,
		first_trade.date_first_trade,

		row_number() over (order by dates.dt_report, data1.login_hash, data1.server_hash, data1.symbol ) as row_number

		from dates
		left join 
		(
			select dense_ranking.*, enabled_users.currency
			from enabled_users
			join dense_ranking
			on dense_ranking.login_hash = enabled_users.login_hash 
			and dense_ranking.server_hash = enabled_users.server_hash
			
		) data1
		on dates.dt_report = data1.trade_date
		left join first_trade
		on data1.login_hash = first_trade.login_hash
		and data1.server_hash = first_trade.server_hash
		and data1.symbol = first_trade.symbol
		left join august
		on data1.login_hash = august.login_hash
		and data1.server_hash = august.server_hash
		and data1.symbol = august.symbol
		and data1.trade_date = august.trade_date
	)	data2
	order by row_number desc
)data3
