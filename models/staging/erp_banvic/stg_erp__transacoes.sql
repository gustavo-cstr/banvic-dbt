
with
    fonte_transacoes as (
        select *
        from {{ source('erp', 'transacoes') }}
    )

    , renomeado as (
        select
            cod_transacao as pk_transacao
            , cast(num_conta as integer) as fk_conta
            , cast(data_transacao as timestamp) as dt_transacao
            , cast(nome_transacao as string) as nome_transacao
            , cast(valor_transacao as numeric) as valor_transacao
        from fonte_transacoes
    )

select *
from renomeado
