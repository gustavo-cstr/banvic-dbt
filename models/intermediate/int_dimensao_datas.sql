with
    data_spine as (
        {{
            dbt_utils.date_spine(
                datepart="day",
                start_date="cast('2020-01-01' as date)",
                end_date="cast('2030-01-01' as date)"
            )
        }}
    )

    , criar_datas as (
        select
            row_number() over(order by date_spine.date_day) as pk_data
            , cast(date_spine.date_day as date) as dt_data
            , extract(day from date_spine.date_day) as dia
            , extract(week from date_spine.date_day) as semana
            , extract(month from date_spine.date_day) as mes
            , extract(year from date_spine.date_day) as ano
            , extract(dow from date_spine.date_day) as dia_da_semana
            , extract(quarter from date_spine.date_day) as trimestre
            , case
                when extract(dow from date_spine.date_day)  in (0, 6) then true
                else false
            end as is_final_de_semana
        from date_spine
    )

select *
from criar_datas
