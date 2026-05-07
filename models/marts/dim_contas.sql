with
    contas as (
        select *
        from {{ ref('int_dimensao_contas') }}
    )

select *
from contas
