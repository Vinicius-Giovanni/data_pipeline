import pandas as pd
import numpy as np
from pipeline.base_pipeline import BasePipeline
from utils.classification import classify_setores
from utils.logging_utils import setup_logging

logger = setup_logging()

class LoadPipeline(BasePipeline):
    def __init__(self):
        super().__init__('load')

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        cfg = self.config

        df.drop(columns=cfg.get('remove_columns', []), errors='ignore', inplace=True)
        df.rename(columns=cfg.get('rename_columns', {}), inplace=True)

        for col, dtype in cfg.get('column_types', {}).items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

        for col in cfg.get('datetime_columns',[]):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col],errors='coerce')
        
        if 'data_hora_load' in df.columns:
            df['data_load'] = df['data_hora_load'].dt.date
        else:
            logger.warning('Coluna "data_hora_load" ausente. Cálculo de tempo será ignorado.')

        if {'box', 'data_load', 'data_hora_load'}.issubset(df.columns):
            time_lead_box = df.groupby(['box', 'data_load'])['data_hora_load'].agg(['min', 'max']).reset_index()
            time_lead_box['duracao_segundos_box_dia'] = (time_lead_box['max'] - time_lead_box['min']).dt.total_seconds()
            df = df.merge(time_lead_box[['box', 'data_load', 'duracao_segundos_box_dia']],
                          how='left', on=['box', 'data_load'])
        else:
            logger.warning('Dados insuficientes para calcular tempo de box por dia.')
        
        df['duracao_segundos_box_dia'] = df.get('duracao_segundos_box_dia', pd.Series(dtype='float')).astype('Int64')

        if 'data_hora_load' in df.columns:
            df['hora'] = df['data_hora_load'].dt.strftime('%H:00:00')
            df['data_criterio'] = df['data_hora_load'].dt.strftime('%d-%m-%Y')
            df['mes_ano'] = df['data_hora_load'].dt.strftime('%m-%Y')
        else:
            df['hora'] = df['data_criterio'] = df['mes_ano'] = 'indefinido'

        df['setores'] = classify_setores(df)

        # Limpeza final
        df.drop(columns=['key_1', 'data_load_x', 'data_load_y', 'data_load'], inplace=True, errors='ignore')

        logger.info(f'Pré-processamento finalizado: {df.shape[0]} linhas, {df.shape[1]} colunas.')
        return df