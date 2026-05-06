with
    datas as (
        select *
        from {{ ref('int_dimensao_datas') }}
    )

select *
from datas
