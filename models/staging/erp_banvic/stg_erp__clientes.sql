
with 
    fonte_cliente as (
        select *
        from {{ source('erp', 'clientes') }}
    )

    , renomeado as (
        select 
            cast(cod_cliente as int) as pk_cliente
            , cast(cod_localidade as int) as fk_localidade
            , cast(primeiro_nome as string) || ' ' || cast(ultimo_nome as string) as nome_cliente 
            , cast(email as string) as email_cliente 
            , tipo_cliente
            , cast(data_inclusao as timestamp) as ts_inclusao 
            , regexp_replace(cpfcnpj, '[^a-zA-Z0-9]', '') as cpfcnpj_cliente
            , cast(data_nascimento as date) as dt_nascimento_cliente
            , cast(endereco as string) as endereco_cliente
            , regexp_replace(cep, '[^a-zA-Z0-9]', '') as cep_cliente
        from fonte_cliente
    )

select *
from renomeado
