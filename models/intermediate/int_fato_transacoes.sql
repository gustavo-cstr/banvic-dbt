with
    transacoes as (
        select *
        from {{ ref('stg_erp__transacoes') }}
    )

    , datas as (
        select *
        from {{ ref('int_dimensao_datas') }}
    )

    , transacoes_enriquecido as (
        select
            transacoes.pk_transacao
            , transacoes.fk_conta
            , datas.pk_data as fk_data
            , transacoes.nome_transacao
            , transacoes.valor_transacao
        from transacoes
        left join datas on cast(transacoes.dt_transacao as date) = datas.dt_data
    )

select *
from transacoes_enriquecido
