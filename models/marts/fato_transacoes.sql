with
    transacoes as (
        select *
        from {{ ref('int_fato_transacoes') }}
    )

select *
from transacoes
