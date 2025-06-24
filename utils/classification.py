import numpy as np
import pandas as pd

def classify_setores(df: pd.DataFrame) -> pd.Series:
    cond = [
        (df['tipo_de_pedido'].isin(['S01 - ENTREGA A CLIENTES']) & df['box'].between(557,584)), # Fracionado Pesados
        (df['tipo_de_pedido'].isin(['S13 - ABASTECIMENTO DE LOJA BOA',
                                    'S14 - ABASTECIMENTO DE LOJA QEB',
                                    'S46 - ABASTECIMENTO RETIRA LOJA',
                                    'S48 - ABASTECIMENTO CEL RJ',
                                    'S11 - TRANSF. LOJA VIA DEPOSITO BOA']) & df['box'].between(595,638)), # EAD - Abastecimento de Lojas
        (df['tipo_de_pedido'].isin(['S13 - ABASTECIMENTO DE LOJA BOA',
                                    'S14 - ABASTECIMENTO DE LOJA QEB',
                                    'S46 - ABASTECIMENTO RETIRA LOJA',
                                    'S48 - ABASTECIMENTO CEL RJ',
                                    'S11 - TRANSF. LOJA VIA DEPOSITO BOA']) & df['box'].between(277,326)), # Polo - Abastecimento de Lojas
        (df['tipo_de_pedido'].isin(['S01 - ENTREGA A CLIENTES']) & ((df['box'].between(331,386) )| (df['box'].between(399,412)))), # Ribeirão Preto
        (df['tipo_de_pedido'].isin(['S01 - ENTREGA A CLIENTES']) & df['box'].between(413,584)), # Capital 
        (df['tipo_de_pedido'].isin(['S53 - TRANSFERENCIA ENTRE CDS']) & df['box'].between(595,638)), # EAD - Balanço
        (df['tipo_de_pedido'].isin(["S04 - TRANSF EAD AUTOMATICA"]) & df['box'].between(387,398)), # Uberlândia
        df['tipo_de_pedido'].isin(['S13 - ABASTECIMENTO DE LOJA BOA',
                                    'S14 - ABASTECIMENTO DE LOJA QEB',
                                    'S46 - ABASTECIMENTO RETIRA LOJA',
                                    'S48 - ABASTECIMENTO CEL RJ',
                                    'S11 - TRANSF. LOJA VIA DEPOSITO BOA']), # Abastecimento de Lojas
        df['tipo_de_pedido'].isin(['S53 - TRANSFERENCIA ENTRE CDS']), # Balanço
        df['tipo_de_pedido'].isin(['S01 - ENTREGA A CLIENTES', 'S02 - RETIRA CLIENTE DEPOSITO']), # Entrega a Cliente
        df['tipo_de_pedido'].isin(['S05 - TRANSF EAD PROGRAMADA', 'S04 - TRANSF EAD AUTOMATICA']), # EAD
        df['tipo_de_pedido'].isin(["S39 - EXPEDICAO LEVES",
                                   "S39M - EXPEDICAO LEVES",
                                   "S39R - Single line",
                                   'S39P - EXPEDICAO LEVES',
                                   'S39I - EXPEDICAO LEVES']) # Leves
    ]
    values = [
        'Fracionado Pesados'
        'EAD - Abastecimento de Lojas',
        'Polo - Abastecimento de Lojas',
        'Ribeirao Preto',
        'Capital',
        'EAD - Balanco',
        'Uberlandia',
        'Abastecimento de Lojas',
        'Balanco',
        'Entrega a Cliente',
        'EAD',
        'Leves',
        'Fracionado Pesados'
    ]
    df['box'] = pd.to_numeric(df['box'], errors='coerce').fillna(-1).astype(int)
    return np.select(cond, values, default='Outras Saidas')

def cancel_classify_setores(df: pd.DataFrame) -> pd.Series:
    cond = [
        df['tipo_de_pedido'].isin(['S13 - ABASTECIMENTO DE LOJA BOA',
                                    'S14 - ABASTECIMENTO DE LOJA QEB',
                                    'S46 - ABASTECIMENTO RETIRA LOJA',
                                    'S48 - ABASTECIMENTO CEL RJ',
                                    'S11 - TRANSF. LOJA VIA DEPOSITO BOA',
                                    'S12 - TRANSF.LOJA VIA DEPOSITO QEB']), # Abastecimento de Lojas
        df['tipo_de_pedido'].isin(['S53 - TRANSFERENCIA ENTRE CDS']), # Balanço
        df['tipo_de_pedido'].isin(['S01 - ENTREGA A CLIENTES', 'S02 - RETIRA CLIENTE DEPOSITO']), # Entrega a Cliente
        df['tipo_de_pedido'].isin(['S05 - TRANSF EAD PROGRAMADA', 'S04 - TRANSF EAD AUTOMATICA']), # EAD
        df['tipo_de_pedido'].isin(["S39 - EXPEDICAO LEVES",
                                   "S39M - EXPEDICAO LEVES",
                                   "S39R - SINGLE LINE",
                                   'S39P - EXPEDICAO LEVES',
                                   'S39I - EXPEDICAO LEVES']) # Leves
    ]
    values = [
        'Abastecimento de Lojas',
        'Balanco',
        'Entrega a Cliente',
        'EAD',
        'Leves'
    ]
    return np.select(cond,values,default='Outras saidas')