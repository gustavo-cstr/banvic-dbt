with
    contas as (
        select *
        from {{ ref('int_fato_contas') }}
    )

select *
from contas
