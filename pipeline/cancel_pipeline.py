import pandas as pd
import numpy as np
from unidecode import unidecode
from pipeline.base_pipeline import BasePipeline
from utils.classification import cancel_classify_setores
from utils.logging_utils import setup_logging
from utils.transformation import normalizar_motivo

logger = setup_logging()

class CancelPipeline(BasePipeline):
    def __init__(self):
        super().__init__('cancel')

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

        df['setores'] = cancel_classify_setores(df)

        if 'motivo_cancelamento' in df.columns:
            df['motivo_cancelamento'] = (
                df['motivo_cancelamento']
                .fillna('')
                .str.strip()
                .replace('','sem motivo')
                .apply(lambda x: unidecode(x).lower())
            )

            result = df['motivo_cancelamento'].apply(normalizar_motivo)
            df['motivo_codigo'] = result.map(lambda x: x[0])
            df['motivo_descricao'] = result.map(lambda x: x[1])

        df['hora'] = df['data_cancelamento'].dt.strftime('%H:00:00')
        df['data_criterio'] = df['data_cancelamento'].dt.strftime('%d-%m-%Y')
        df['mes_ano'] = df['data_cancelamento'].dt.strftime('%m-%Y')

        return df