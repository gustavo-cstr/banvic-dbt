with
    fonte_colaboradores as (
        select *
        from {{ source('erp', 'colaboradores') }}
    )

    , renomeado as (
        select
            cast(cod_colaborador as int) as pk_colaborador
            , cast(cod_localidade as int) as fk_localidade
            , cast(cod_gerente as int) as fk_gerente
            , cast(primeiro_nome as string) || ' ' || cast(ultimo_nome as string) as nome_colaborador
            , cast(email as string) as email_colaborador
            , regexp_replace(cpf, '[^a-zA-Z0-9]', '') as cpf_colaborador
            , cast(data_nascimento as date) as dt_nascimento_colaborador
            , cast(endereco as string) as endereco_colaborador
            , regexp_replace(cep, '[^a-zA-Z0-9]', '') as cep_colaborador
        from fonte_colaboradores
    )

select *
from renomeado
