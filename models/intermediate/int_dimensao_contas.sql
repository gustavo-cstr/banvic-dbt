with
    contas as (
        select *
        from {{ ref('stg_erp__contas') }}
    )

    , selecionar_colunas as (
        select 
            pk_conta
            , numero_conta
            , tipo_conta
            , ts_abertura_conta
        from stg_erp__contas
    )    

select *
from selecionar_colunas