from pathlib import Path

# Leitura particionada padrã/fixa para todos os pipelines
CHUNK_SIZE = 100_000

# Caminho padrão/fixo do arquivo log
LOG_PATH = r'C:\Users\2960006959\Desktop\project\data_pipeline\logs\pipeline.log'

BASE_PATH = Path(r'C:\Users\2960006959\Desktop\project\data_pipeline\data')

PIPELINE_PATHS = {
        'time_lead_olpn': {
        'parquet_load': BASE_PATH / 'parquet' / 'data_load',
        'output_parquet': BASE_PATH / 'parquet' / 'data_time_lead_olpn'
    },
        'bottleneck_box': {
        'parquet_load': BASE_PATH / 'parquet' / 'data_load',
        'parquet_putaway': BASE_PATH / 'parquet' / 'data_putaway',
        'output_parquet': BASE_PATH / 'parquet' / 'data_bottleneck_box'
    },
        'bottleneck_salao': {
        'parquet_packed': BASE_PATH / 'parquet' / 'data_packed',
        'parquet_putaway': BASE_PATH / 'parquet' / 'data_putaway',
        'output_parquet': BASE_PATH / 'parquet' / 'data_bottleneck_salao'
    },
        'jornada': {
        'raw': BASE_PATH / 'raw' / 'data_jornada',
        'processed': BASE_PATH / 'processed' / 'data_jornada',
        'parquet': BASE_PATH / 'parquet' / 'data_jornada'
    },
        'olpn': {
        'raw': BASE_PATH / 'raw' / 'data_olpn',
        'processed': BASE_PATH / 'processed' / 'data_olpn',
        'parquet': BASE_PATH / 'parquet' / 'data_olpn'
    },
        'packing': {
        'raw': BASE_PATH / 'raw' / 'data_packing',
        'processed': BASE_PATH / 'processed' / 'data_packing',
        'parquet': BASE_PATH / 'parquet' / 'data_packing'
    },
        'picking': {
        'raw': BASE_PATH / 'raw' / 'data_picking',
        'processed': BASE_PATH / 'processed' / 'data_picking',
        'parquet': BASE_PATH / 'parquet' / 'data_picking'
    },
        'cancel': {
        'raw': BASE_PATH / 'raw' / 'data_cancel',
        'processed': BASE_PATH / 'processed' / 'data_cancel',
        'parquet': BASE_PATH / 'parquet' / 'data_cancel'
    },
        'putaway': {
        'raw' : BASE_PATH / 'raw' / 'data_putaway',
        'processed' : BASE_PATH / 'processed' / 'data_putaway',
        'parquet' : BASE_PATH / 'parquet' / 'data_putaway'
    },
        'load': {
        'raw' : BASE_PATH / 'raw' / 'data_load',
        'processed' : BASE_PATH / 'processed' / 'data_load',
        'parquet' : BASE_PATH / 'parquet' / 'data_load'
    }
}
# Configurações dos pipelines
PIPELINE_CONFIG = {
        'bottleneck_salao': {
        'read_columns_packed': [
            'olpn',
            'data_hora_fim_olpn'
        ],
        'read_columns_putaway': [
            'olpn',
            'data_hora_putaway'
        ],
        'datetime_columns': [
            'data_hora_fim_olpn',
            'data_hora_putaway'
        ],
        'column_type': {
            'olpn': 'string'
        }
    },
        'bottleneck_box': {
        'read_columns_load': [
                'olpn',
                'data_hora_load'
                ],
        'read_columns_putaway': [
                'olpn',
                'data_hora_putaway'
                ],
        'datetime_columns': [
            'data_hora_putaway',
            'data_hora_load'
        ],
        'column_type': {
            'olpn': 'string'
        }

    },
        'time_lead_olpn': {
        'read_columns': [
            'olpn',
            'data_hora_load',
            'data_pedido'
        ],
        'datetime_columns': [
            'data_hora_load',
            'data_pedido'
        ],
        'column_types': {
            'olpn': 'string'
        },
    },
        'olpn': {
        'remove_columns': [
                'Audit Status',
                'Cod Setor Item',
                'Inventory Type ID',
                'Data Limite Expedição',
                'Data Prevista Entrega',
                'Marcação de EAD',
                'Numero da Gaiola',
                'Status oLPN',
                'Tarefa Status',
                'Data do Pedido',
                'Filial',
                'Data locação pedido',
                'Shipment',
                'Filial Destino',
                'Status Pedido',
                'Pedido de Venda',
                'Wave',
                'Descrição'
        ],
        'rename_columns': {
                'Último Update oLPN': 'data_hora_ultimo_update_olpn',
                'TOTE': 'tote',
                'Tarefa': 'tarefa',
                'Grupo de Tarefa': 'grupo_de_tarefa',
                'Item': 'item',
                'Local de Picking': 'local_de_picking',
                'Qtde. Peças Item': 'qt_pecas',
                'Volume': 'volume',
                'BOX': 'box',
                'Desc Setor Item': 'desc_setor_item',
                'Tipo de pedido': 'tipo_de_pedido',
                'Pedido': 'pedido',
                'oLPN': 'olpn'
        },
        'column_types': {
                'tote': 'string',
                'tarefa': 'string',
                'grupo_de_tarefa': 'string',
                'item': 'Int64',
                'descricao': 'string',
                'local_de_picking': 'string',
                'qt_pecas': 'Int64',
                'box': 'Int64',
                'desc_setor_item': 'string',
                'tipo_de_pedido': 'string',
                'pedido': 'string',
                'olpn': 'string'
        },
        'datetime_columns':  [
                'data_hora_ultimo_update_olpn'
        ],
        'encoding': 'utf-16',
        'sep' : '\t'
    },
        'picking': {
        'remove_columns': [
                'Filial',
                'Status Tarefa',
                'Tipo de Transação',
                'Qtde Alocada',
                'Task Moviment',
                'Pull Location for Task Detail',
                'Destination Location for Task Detail',
                'Wave',
                'Nome',
                'Data da Tarefa (Create)',
                'Data e Hora da Assinatura da Tarefa',
                'Descrição',
                'Local Destino',
                'Inventory Type',
                'Status Detalhe da Tarefa'
        ],
        'rename_columns': {
                'Tarefa': 'tarefa',
                'Qtde requerida': 'qt_requerida',
                'Qtde Separada': 'qt_separada',
                'Usuário': 'usuario',
                'Data do Inicio da Tarefa': 'data_hora_inicio_tarefa',
                'Data de Finalização da Tarefa': 'data_hora_fim_tarefa',
                'Data de Finalização da oLPN': 'data_hora_fim_olpn',
                'Order ID': 'pedido',
                'oLPN': 'olpn',
                'Item': 'item',
                'Setor': 'desc_setor_item',
                'Tipo de Pedido': 'tipo_de_pedido',
                'Local de Coleta': 'local_de_picking',
                'BOX': 'box'
        },
        'column_types': {
                'tarefa': 'string',
                'qt_requerida': 'Int64',
                'qt_separada': 'Int64',
                'usuario': 'string',
                'pedido': 'Int64',
                'olpn': 'string',
                'item': 'Int64',
                'desc_setor_item': 'string',
                'tipo_de_pedido': 'string',
                'local_de_picking': 'string',
                'box': 'Int64'
                        },
        'datetime_columns':  [
                'data_hora_inicio_tarefa',
                'data_hora_fim_tarefa',
                'data_hora_fim_olpn'
        ],
        'encoding': 'utf-16',
        'sep' : '\t'
    },
        'cancel' : {
        'remove_columns': [
                'Filial',
                'Inventory Type ID',
                'Pedido de Venda',
                'Carga',
                'Destinatário',
                'Descrição do item',
                'Qtde Original',
                'Qtde Expedida',
                'Data integração WMS',
                'Código Reference Text'
        ],
        'rename_columns': {
                'Pedido': 'pedido',
                'Tipo da Ordem ': 'tipo_de_pedido',
                'Qtde Ajustada': 'qt_pecas',
                'Data do Cancelamento': 'data_cancelamento',
                'Usuário': 'usuario',
                ' Motivo Secondary Reference Text': 'motivo_cancelamento'
        },
        'column_types': {
                'pedido': 'string',
                'tipo_de_pedido': 'string',
                'qt_pecas': 'Int64',
                'usuario': 'string',
                'motivo_cancelamento': 'string',
                'item': 'Int64'
        },
        'datetime_columns': [
                'data_cancelamento'
        ],
        'encoding': 'utf-16',
        'sep': '\t'
    },
        'packing' : {
        'remove_columns': [
                'Filial',
                'Inventory Type ID',
                'Pallet',
                'Descrição item',
                'Data Pedido',
                'Nome do Usuário',
                'Shipment',
                'Pedido venda',
                'Nota Fiscal',
                'Embala',
                'Facility ID',
                'Tipo de Pedido',
                'Data LOAD',
                'Pedido de Venda',
                'Descrição do Item'
        ],
        'rename_columns': {
                'OLPN': 'olpn',
                'Pedido': 'pedido',
                'Item': 'item',
                'Setor': 'desc_setor_item',
                'Tipo Pedido': 'tipo_de_pedido',
                'Data Packed': 'data_hora_packed',
                'Usuário': 'usuario',
                'Quantidade': 'qt_pecas',
                'BOX': 'box'
        },
        'column_types': {
                'olpn': 'string',
                'pedido': 'string',
                'item': 'Int64',
                'desc_setor_item': 'string',
                'tipo_de_pedido': 'string',
                'usuario': 'string',
                'qt_pecas': 'Int64',
                'box': 'Int64'
        },
        'datetime_columns': [
                'data_hora_packed'
        ],
        'encoding': 'utf-16',
        'sep': '\t'
    },
        'load' : {
        'remove_columns': [
                'Facility ID',
                'Inventory Type ID',
                'Nome do Usuário',
                'Shipment',
                'Pedido de Venda',
                'Nota Fiscal',
                'Descrição do Item'
        ],
        'rename_columns': {
                'OLPN': 'olpn',
                'Pedido': 'pedido',
                'Tipo de Pedido': 'tipo_de_pedido',
                'Data LOAD': 'data_hora_load',
                'Usuário': 'usuario',
                'Quantidade': 'qt_pecas',
                'BOX': 'box',
                'Item': 'item',
                'Data Pedido': 'data_pedido'
        },
        'column_types': {
                'olpn': 'string',
                'pedido': 'string',
                'tipo_de_pedido': 'string',
                'usuario': 'string',
                'qt_pecas': 'Int64',
                'box': 'Int64',
                'Item': 'Int64'
        },
        'datetime_columns': [
                'data_hora_load',
                'data_pedido'
        ],
        'encoding': 'utf-16',
        'sep': '\t'
    },
        'putaway' : {
        'remove_columns': [
                'Filial',
                'Data',
                'Status',
                'DESCRIÇÃO ITEM',
                'Item Attribute1',
                'Transaction Type',
                'Inventory Type ID'
        ],
        'rename_columns': {
                'Order': 'pedido',
                'OLPN': 'olpn',
                'ITEM': 'item',
                'QT': 'qt_pecas',
                'Setor': 'desc_setor_item',
                'Tipo de Pedido': 'tipo_de_pedido',
                'BOX': 'box',
                'DATA DO EVENTO': 'data_hora_putaway',
                'USUÁRIO': 'usuario'
        },
        'column_types': {
                'pedido': 'string',
                'olpn': 'string',
                'item': 'Int64',
                'qt_pecas': 'Int64',
                'desc_setor_item': 'string',
                'tipo_de_pedido': 'string',
                'box': 'Int64',
                'usuario': 'string'
        },
        'datetime_columns': [
                'data_hora_putaway'
        ],
        'encoding': 'utf-16',
        'sep': '\t'
    },
        'jornada' : {
        'remove_columns': [],
        'rename_columns': {
                'dia': 'data',
                'matricula': 'matricula',
                'cod': 'cod',
                'hora': 'hora'
        },
        'column_types': {
                'matricula': 'string',
                'cod': 'string',
        },
        'datetime_columns': [
                'data'
        ],
        'encoding': 'ascii',
        'sep': ';'
    },
        'padrao' : {
        'remove_columns': [],
        'rename_columns': {},
        'column_types': {},
        'datetime_columns': [],
        'encoding': 'utf-16',
        'sep': '\t'
    },
}

MOTIVOS_OFICIAIS = {
    1: 'MOTIVO DESCONTINUADO (OUTROS)',
    2: 'DIVERGENCIA DE SALDO COM PCOM x WMS',
    3: 'PENDENCIA DE ARMAZENAGEM',
    4: 'ENDEREÇO VAZIO',
    5: 'NF SEM TRATAMENTO',
    6: 'LEITURA IMEI',
    7: 'ERRO OPERACIONAL',
    8: 'AVARIA',
    9: 'AVARIA/EAD',
    10: 'SALDO INSUFICIENTE',
    11: 'FALTA EAD',
    12: 'TRANSPORTADORA DECLINADA',
    13: 'SKU DIVERGENTE',
    14: 'PROGRAMACAO INDEVIDA',
    15: 'ILPN VOANDO',
    16: 'EXCESSO DE CUBAGEM',
    17: 'DTF RETORNO',
    18: 'FALTA DE COMPOSICAO',
    19: 'FALTA/CROSS',
    20: 'AVARIA/CROSS',
    21: 'CARRETA EM POSTO FISCAL',
    22: 'CARRETA NO SHOW',
    23: 'PEDIDO SEM TRACKING NUMBER',
    24: 'CRL/BARRAR ENTREGA',
    25: 'IE NAO CADASTRADA NA REGIAO',
    26: 'ERRO DE CEP',
    27: 'SOLICITACAO GESTAO'
}

MAPEAMENTO_TEXTUAL = {
    2: [r'diver[g|n]encia.*saldo.*pcom.*wms'],
    3: [r'pend[e|a]ncia.*armazen'],
    4: [r'endere[çc]o.*vazio'],
    5: [r'nf.*sem.*trat'],
    6: [r'imei'],
    7: [r'erro.*(operacional|armazenagem)'],
    8: [r'^avaria$'],
    9: [r'avaria.?/?ead'],
    10: [r'saldo.*insuf'],
    11: [r'falta.*ead'],
    12: [r'transportadora.*declinada|transportadora.*nao atende'],
    13: [r'sku.*diverg'],
    14: [r'programa[çc][aã]o.*ind'],
    15: [r'ilpn.*voando|lpn.*voando'],
    16: [r'excesso.*cubagem|cubagem.*excesso'],
    17: [r'dtf.*retorno'],
    18: [r'falta.*composi'],
    19: [r'cross'],
    20: [r'avaria.*cross'],
    21: [r'posto.*fiscal'],
    22: [r'no show'],
    23: [r'sem.*tracking'],
    24: [r'barrar.*entrega|crl'],
    25: [r'ie.*n[aã]o.*cadast'],
    26: [r'erro.*cep'],
    27: [r'solicita.*gest[aã]o'],
    1: [r'outro|outros']
}

REGRAS_DIRETAS = [
        (6, ['imei']),
        (5, ['nf', 'nota fiscal']),
        (4, ['endereco', 'vazio']),
        (14, ['progamacao indevida']),
        (3, ['armazenagem']),
        (2, ['wms'], ['pcom', 'saldo', 'x']),
        (10, ['saldo']),
        (27, ['gerente', 'autorizado', 'solicitado']),
        (18, ['composicao', 'composic']),
        (16, ['cubagem', 'problema de volume', 'nao coube', 'excesso']),
        (24, ['cliente'], ['nao quis', 'recusou', 'recusa']),
        (22, ['no show', 'nao apareceu']),
        (21, ['posto fiscal', 'parado na receita']),
]