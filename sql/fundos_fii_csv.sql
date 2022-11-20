CREATE TABLE IF NOT EXISTS fundos_invetimentos(
    id_cota SERIAL PRIMARY KEY,
    data_extracao DATE,
    codigo_fundo VARCHAR NOT NULL,
    setor VARCHAR,
    preco_atual FLOAT,
    liquidez_diaria FLOAT,
    dividendo FLOAT,
    dy FLOAT,
    dy_3_acumulado FLOAT,
    dy_6_acumulado FLOAT,
    dy_12_acumulado FLOAT,
    dy_3_media FLOAT,
    dy_6_media FLOAT,
    dy_12_media FLOAT,
    dy_ano FLOAT,
    variacao_preco FLOAT,
    rentabilidade_periodo FLOAT,
    rentabilidade_acumulada FLOAT,
    patrimonio_liquido FLOAT,
    vpa FLOAT,
    p_vpa FLOAT,
    dy_patrimonial FLOAT,
    variacao_patrimonial FLOAT,
    rentabilidade_patrimonial_periodo FLOAT,
    rentabilidade_patrimonial_acumulada FLOAT,
    vacancia_fisica FLOAT,
    vacancia_financeira FLOAT,
    quantidade_ativos INTEGER
);

COPY fundos_invetimentos(codigo_fundo, setor, preco_atual, liquidez_diaria, dividendo, dy, dy_3_acumulado, dy_6_acumulado,
                        dy_12_acumulado,dy_3_media,dy_6_media,dy_12_media,dy_ano,variacao_preco,rentabilidade_periodo,
                        rentabilidade_acumulada,patrimonio_liquido,vpa,p_vpa,dy_patrimonial,variacao_patrimonial,
                        rentabilidade_patrimonial_periodo,rentabilidade_patrimonial_acumulada,vacancia_fisica,
                        vacancia_financeira,quantidade_ativos) FROM '../csv/fundos_investimentos.csv' DELIMITER ';' CSV HEADER