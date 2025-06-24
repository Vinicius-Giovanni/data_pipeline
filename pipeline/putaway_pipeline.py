import pandas as pd
import numpy as np
from pipeline.base_pipeline import BasePipeline
from utils.classification import classify_setores
from utils.logging_utils import setup_logging

logger = setup_logging()

class PutawayPipeline(BasePipeline):
    def __init__(self):
        super().__init__('putaway')

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        cfg = self.config

        df.drop(columns=cfg.get('remove_columns', []), errors='ignore', inplace=True)
        df.rename(columns=cfg.get('rename_columns', {}), inplace=True)

        for col, dtype in cfg.get('column_types', {}).items():
            if col in df.columns:
                if col == 'box':
                    df[col] = pd.to_numeric(
                        df[col].astype(str).str.extract(r'(\d+)', expand=False),
                        errors='coerce'
                    ).astype('Int64')
                else:
                    df[col] = df[col].astype(dtype)

        for col in cfg.get('datetime_columns', []):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        if 'data_hora_putaway' in df.columns:
            df['data_putaway'] = df['data_hora_putaway'].dt.date
        else:
            logger.warning('Coluna "data_hora_putaway" ausente. Cálculo de tempo será ignorado.')
        
        if {'box', 'data_putaway', 'data_hora_putaway'}.issubset(df.columns):
            time_lead_box = df.groupby(['box', 'data_putaway'])['data_hora_putaway'].agg(['min', 'max']).reset_index()
            time_lead_box['duracao_segundos_box_dia'] = (time_lead_box['max'] - time_lead_box['min']).dt.total_seconds()
            df = df.merge(time_lead_box[['box', 'data_putaway', 'duracao_segundos_box_dia']],
                          how='left', on=['box', 'data_putaway'])
        else:
            logger.warning('Dados insuficientes para calcular tempo de box por dia.')

        df['duracao_segundos_box_dia'] = df.get('duracao_segundos_box_dia', pd.Series(dtype='float')).astype('Int64')

        if 'data_hora_putaway' in df.columns:
            df['hora'] = df['data_hora_putaway'].dt.strftime('%H:00:00')
            df['data_criterio'] = df['data_hora_putaway'].dt.strftime('%d-%m-%Y')
            df['mes_ano'] = df['data_hora_putaway'].dt.strftime('%m-%Y')
        else:
            df['hora'] = df['data_criterio'] = df['mes_ano'] = 'indefinido'

        df['setores'] = classify_setores(df)

        # Limpeza final
        df.drop(columns=['key_1', 'data_putaway_x', 'data_putaway_x', 'data_putaway'], inplace=True, errors='ignore')

        logger.info(f'Pré-processamento finalizado: {df.shape[0]} linhas, {df.shape[1]} colunas.')
        return df