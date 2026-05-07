with
    contas as (
        select *
        from {{ ref('stg_erp__contas') }}
    )

    , selecionar_colunas as (
        select 
            pk_conta
            , fk_cliente
            , fk_agencia
            , fk_colaborador
            , saldo_total
            , saldo_disponivel
            , ts_ultimo_lancamento
        from stg_erp__contas
    )    

select *
from selecionar_colunas