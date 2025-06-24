import pandas as pd
import numpy as np
from pipeline.base_pipeline import BasePipeline
from utils.classification import classify_setores
from utils.logging_utils import setup_logging

logger = setup_logging()

class OlpnPipeline(BasePipeline):
    def __init__(self):
        super().__init__('olpn')

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        cfg = self.config

        df.drop(columns=cfg.get('remove_columns', []), errors='ignore', inplace=True)
        df.rename(columns=cfg.get('rename_columns', {}), inplace=True)

        for col, dtype in cfg.get('column_types', {}).items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

        for col in cfg.get('datetime_columns', []):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        if 'volume' in df.columns and df['volume'].dtype == 'object':
            df['volume'] = pd.to_numeric(df['volume'].str.replace(',', '.', regex=False), errors='coerce')

        if 'local_de_picking' in df.columns:
            df['local_de_picking'] = df['local_de_picking'].astype('string')
            split_cols = df['local_de_picking'].str.split('-', expand=True)

            df['rua'] = split_cols[0] if split_cols.shape[1] > 0 else pd.NA
            df['endereco'] = split_cols[1] if split_cols.shape[1] > 1 else pd.NA
            df['nivel'] = split_cols[2] if split_cols.shape[1] > 2 else pd.NA


        df['setores'] = classify_setores(df)

        df['rua'] = df['rua'].fillna('')
        df['endereco'] = df['endereco'].fillna('')

        df['localizacao'] = np.where(
            (df['rua'].isin(['CP1', 'CS1', 'P02', 'R01', 'R02'])) | (df['endereco'] == 'PAR'),
            'P.A.R',
            'Salao'
        )

        if 'data_hora_ultimo_update_olpn' in df.columns:
            df['mes_ano'] = df['data_hora_ultimo_update_olpn'].dt.strftime('%m-%Y')
            df['data_criterio'] = df['data_hora_ultimo_update_olpn'].dt.strftime('%d-%m-%Y')
            df['hora'] = df['data_hora_ultimo_update_olpn'].dt.strftime('%H:00:00')
        else:
            logger.warning("Coluna 'data_hora_ultimo_update_olpn' não encontrada para particionamento.")
            df['mes_ano'] = 'indefinido'

        return df