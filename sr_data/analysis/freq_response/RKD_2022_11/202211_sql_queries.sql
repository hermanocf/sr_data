SELECT value, "_unit", "timestamp" 
FROM monitoring_indicatorvalues mi where indicator_id = 8 and (value::float < 59.964 or value::float > 60.036) and data_quality = 'good' and power_meter_id = 646623 order by "timestamp" desc limit 100;


SELECT count(1) 
FROM public.monitoring_indicatorvalues mi where indicator_id = 8 and (value::float < 59.964 or value::float > 60.036) and data_quality = 'good' and power_meter_id = 646623 and timestamp > '2022-10-29' and timestamp < '2022-11-06';



SELECT count(1) 
FROM public.monitoring_indicatorvalues mi where indicator_id = 8 and value::float < 59.964 and data_quality = 'good' and power_meter_id = 646623 and timestamp > '2022-10-29' and timestamp < '2022-11-06';


SELECT count(1) 
FROM monitoring_indicatorvalues mi where indicator_id = 8 and  value::float > 60.036 and data_quality = 'good' and power_meter_id = 646623  and timestamp > '2022-10-29' and timestamp < '2022-11-06';


SELECT (select "timestamp", value, _unit, indicator_id  
FROM public.monitoring_indicatorvalues mi where data_quality = 'good' and power_meter_id = 646623 and timestamp > '2022-10-29' and timestamp < '2022-11-06' and (indicator_id = 8 or indicator_id = 7) order by "timestamp")
(select "timestamp", value from monitoring_indicatorvalues mi where indicator_id = 8 and data_quality = 'good' and power_meter_id = 646623 and (value::float < 59.964 or value::float > 60.036) and timestamp > '2022-10-29' and timestamp < '2022-11-06')

-------
select timestamp, MAX (value::float) filter (where indicator_id = 21 and data_quality = 'good') and "timestamp" between '2022-10-30' and '2022-11-05') as active_control
from monitoring_indicatorvalues mi where timestamp between '2022-10-30' and '2022-11-05' group by "timestamp";


-------

select timestamp,
	MAX (value::float) filter (where power_meter_id = '646623' and indicator_id = 8 and data_quality = 'good' and timestamp between '2022-10-30' and '2022-11-05') as freq,
	MAX (value::float) filter (where power_meter_id = '646623' and indicator_id = 7 and data_quality = 'good' and timestamp between '2022-10-30' and '2022-11-05') as active_power,
	MAX (value::float) filter (where power_meter_id = '646623' and indicator_id = 8 and data_quality = 'good' and (value::float < 59.964 or value::float > 60.036) and timestamp between '2022-10-30' and '2022-11-05') as freq_event,
	MAX (value::float) filter (where indicator_id = 21 and data_quality = 'good' and "timestamp" between '2022-10-30' and '2022-11-05') as active_control,
	MAX (value::float) filter (where indicator_id = 22 and data_quality = 'good' and "timestamp" between '2022-10-30' and '2022-11-05') as pfr_target,
	MAX (value::float) filter (where indicator_id = 12 and data_quality = 'good' and "timestamp" between '2022-10-30' and '2022-11-05') as target,
	MAX (value::float) filter (where indicator_id = 1 and data_quality = 'good' and "timestamp" between '2022-10-30' and '2022-11-05') as operational,
	MAX (value::float) filter (where indicator_id = 14 and data_quality = 'good' and "timestamp" between '2022-10-30' and '2022-11-05') as op_ctrl,
	SUM ((ABS(60 - value::float) - 0.036) * (100/((((1/100) * (value::float)) - 0.036) * 10)) / 0.1) filter (where power_meter_id = '646623' and indicator_id = 8 and (value::float < 59.964 or value::float > 60.036) and timestamp between '2022-10-30' and '2022-11-05') as freq_base_response
	into "202211_indicatorvalues_fbr__pivot"
	from monitoring_indicatorvalues mi where timestamp between '2022-10-30' and '2022-11-05'
group by "timestamp";
--------

select timestamp, freq, freq_event, active_power, freq_base_response 
from "202211_indicatorvalues_fbr__pivot"
where freq_event notnull ;


select *, lag(freq_event) over (order by "timestamp") as prev_freq_event from "202211_indicatorvalues_fbr__pivot" ifp where freq notnull ;


with freq_event_analysis as (select *, lag(freq_event) over (order by "timestamp") as prev_freq_event from "202211_indicatorvalues_fbr__pivot" ifp where freq notnull)
select *, coalesce ((freq_event - prev_freq_event)/prev_freq_event *100,null) as percent_change into "202211_freq_event_table" from freq_event_analysis;

select count(freq_base_response) from "202211_freq_event_table" fet where prev_freq_event isnull;


select count(freq_base_response) from "202211_freq_event_table" fet where prev_freq_event isnull and freq > 60.036;


select count(freq_base_response) from "202211_freq_event_table" fet where prev_freq_event isnull and freq < 59.964;


select sum(ABS(freq_base_response)) from "202211_freq_event_table" fet where prev_freq_event isnull;


select sum(ABS(freq_base_response)) from "202211_freq_event_table" fet;


select count(1) from "202211_indicatorvalues_fbr__pivot"


select count(active_power) from "202211_freq_event_table" fet;



with timediff as (select *, lag(timestamp) over (order by "timestamp") as prev_freq_event_time from "202211_indicatorvalues_fbr__pivot" ifp where freq_event notnull)
select *, coalesce ((timestamp - prev_freq_event_time),null) as time_difference into "202211_freq_event_table_test" from timediff;



select sum(time_difference) from "202211_freq_event_table_test" fett where time_difference = '00:00:02';