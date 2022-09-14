SELECT
    value,
    "_unit",
    "timestamp"
FROM
    public.monitoring_indicatorvalues mi
where
    indicator_id = 8
    and (
        value :: float < 59.964
        or value :: float > 60.036
    )
    and data_quality = 'good'
    and power_meter_id = 646623
order by
    "timestamp" desc
limit
    100;

SELECT
    count(1)
FROM
    public.monitoring_indicatorvalues mi
where
    indicator_id = 8
    and (
        value :: float < 59.964
        or value :: float > 60.036
    )
    and data_quality = 'good'
    and power_meter_id = 646623
    and timestamp > '2022-06-01'
    and timestamp < '2022-06-30';

SELECT
    count(1)
FROM
    public.monitoring_indicatorvalues mi
where
    indicator_id = 8
    and value :: float < 59.964
    and data_quality = 'good'
    and power_meter_id = 646623
    and timestamp > '2022-06-01'
    and timestamp < '2022-06-30';

SELECT
    count(1)
FROM
    monitoring_indicatorvalues mi
where
    indicator_id = 8
    and value :: float > 60.036
    and data_quality = 'good'
    and power_meter_id = 646623
    and timestamp > '2022-06-01'
    and timestamp < '2022-06-30';

SELECT
    (
        select
            "timestamp",
            value,
            _unit,
            indicator_id
        FROM
            public.monitoring_indicatorvalues mi
        where
            data_quality = 'good'
            and power_meter_id = 646623
            and timestamp > '2022-06-01'
            and timestamp < '2022-06-30'
            and (
                indicator_id = 8
                or indicator_id = 7
            )
        order by
            "timestamp"
    ) (
        select
            "timestamp",
            value
        from
            monitoring_indicatorvalues mi
        where
            indicator_id = 8
            and data_quality = 'good'
            and power_meter_id = 646623
            and (
                value :: float < 59.964
                or value :: float > 60.036
            )
            and timestamp > '2022-06-01'
            and timestamp < '2022-06-30'
    )
select
    timestamp,
    MAX (value :: float) filter (
        where
            power_meter_id = '646623'
            and indicator_id = 8
            and data_quality = 'good'
            and timestamp between '2022-06-01'
            and '2022-06-30'
    ) as freq,
    MAX (value :: float) filter (
        where
            power_meter_id = '646623'
            and indicator_id = 7
            and data_quality = 'good'
            and timestamp between '2022-06-01'
            and '2022-06-30'
    ) as active_power,
    MAX (value :: float) filter (
        where
            power_meter_id = '646623'
            and indicator_id = 8
            and data_quality = 'good'
            and (
                value :: float < 59.964
                or value :: float > 60.036
            )
            and timestamp between '2022-06-01'
            and '2022-06-30'
    ) as freq_event,
    max (
        (ABS(60 - value :: float) - 0.036) * (100 /((((1 / 100) * (value :: float)) - 0.036) * 10)) / 0.1
    ) filter (
        where
            power_meter_id = '646623'
            and indicator_id = 7
            and data_quality = 'good'
            and timestamp between '2022-06-01'
            and '2022-06-30'
    ) as freq_base_response into "202206_indicatorvalues__pivot"
from
    monitoring_indicatorvalues mi
where
    timestamp between '2022-06-01'
    and '2022-06-30'
group by
    "timestamp";

select
    timestamp,
    MAX (value :: float) filter (
        where
            power_meter_id = '646623'
            and indicator_id = 8
            and data_quality = 'good'
            and timestamp between '2022-06-01'
            and '2022-06-30'
    ) as freq,
    MAX (value :: float) filter (
        where
            power_meter_id = '646623'
            and indicator_id = 7
            and data_quality = 'good'
            and timestamp between '2022-06-01'
            and '2022-06-30'
    ) as active_power,
    MAX (value :: float) filter (
        where
            power_meter_id = '646623'
            and indicator_id = 8
            and data_quality = 'good'
            and (
                value :: float < 59.964
                or value :: float > 60.036
            )
            and timestamp between '2022-06-01'
            and '2022-06-30'
    ) as freq_event,
    SUM (
        (ABS(60 - value :: float) - 0.036) * (100 /((((1 / 100) * (value :: float)) - 0.036) * 10)) / 0.1
    ) filter (
        where
            power_meter_id = '646623'
            and indicator_id = 8
            and (
                value :: float < 59.964
                or value :: float > 60.036
            )
            and timestamp between '2022-06-01'
            and '2022-06-30'
    ) as freq_base_response into "202206_indicatorvalues_fbr__pivot"
from
    monitoring_indicatorvalues mi
where
    timestamp between '2022-06-01'
    and '2022-06-30'
group by
    "timestamp";

select
    timestamp,
    freq,
    freq_event,
    active_power,
    freq_base_response
from
    "202206_indicatorvalues_fbr__pivot"
where
    freq_event notnull;

select
    *,
    lag(freq_event) over (
        order by
            "timestamp"
    ) as prev_freq_event
from
    "202206_indicatorvalues_fbr__pivot" ifp
where
    freq notnull;

with freq_event_analysis as (
    select
        *,
        lag(freq_event) over (
            order by
                "timestamp"
        ) as prev_freq_event
    from
        "202206_indicatorvalues_fbr__pivot" ifp
    where
        freq notnull
)
select
    *,
    coalesce (
        (freq_event - prev_freq_event) / prev_freq_event * 100,
        null
    ) as percent_change into "202206_freq_event_table"
from
    freq_event_analysis;

select
    count(freq_base_response)
from
    "202206_freq_event_table" fet
where
    prev_freq_event isnull;

select
    count(freq_base_response)
from
    "202206_freq_event_table" fet
where
    prev_freq_event isnull
    and freq > 60.036;

select
    count(freq_base_response)
from
    "202206_freq_event_table" fet
where
    prev_freq_event isnull
    and freq < 59.964;

select
    sum(ABS(freq_base_response))
from
    "202206_freq_event_table" fet
where
    prev_freq_event isnull;

select
    sum(ABS(freq_base_response))
from
    "202206_freq_event_table" fet;

select
    count(1)
from
    "202206_indicatorvalues_fbr__pivot"
select
    count(active_power)
from
    "202206_freq_event_table" fet;

select
    *,
    (
        case
            when freq notnull then timediff()
        )
        from
            "202206_freq_event_table" fet
        order by
            "timestamp"
        set
            @first_freq_event = (
                select
                    min(timestamp)
                from
                    "202206_freq_event_table" fet
                where
                    freq_event notnull create "202206_freq_event_test"
                select
                    *
                from
                    (
                        select
                            timestamp,
                            freq,
                            active_power,
                            freq_event,
                            freq with timediff as (
                                select
                                    *,
                                    lag(timestamp) over (
                                        order by
                                            "timestamp"
                                    ) as prev_freq_event_time
                                from
                                    "202206_indicatorvalues_fbr__pivot" ifp
                                where
                                    freq_event notnull
                            )
                        select
                            *,
                            coalesce ((timestamp - prev_freq_event_time), null) as time_difference into "202206_freq_event_table_test"
                        from
                            timediff;

select
    sum(time_difference)
from
    "202206_freq_event_table_test" fett
where
    time_difference = '00:00:02';