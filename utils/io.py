import pandas as pd
from pathlib import Path
from utils.logging_utils import setup_logging
from config.settings import CHUNK_SIZE, PIPELINE_CONFIG

logger = setup_logging()

def read_all_csv(folder: Path, pipeline_key: str) -> pd.DataFrame:

    logger.info(f"Iniciando leitura dos arquivos CSV para o pipeline '{pipeline_key}' no diretório '{folder}'.")

    config = PIPELINE_CONFIG.get(pipeline_key)

    if not config:
        logger.error(f'Configuração para o pipeline "{pipeline_key}" não encontrada.')
        return pd.DataFrame()
    
    encoding = config.get('encoding', 'utf-8')
    sep = config.get('sep', ';')

    dataframes = []
    for file in folder.glob('*.csv'):
        try:
            for chunk in pd.read_csv(
                file, encoding=encoding,
                sep=sep,
                chunksize=CHUNK_SIZE,
                low_memory=False
            ):
                dataframes.append(chunk)
        except Exception as e:
            logger.error(f'Erro ao ler {file.name}: {e}')
    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

def read_parquet_with_tote(folder: Path) -> pd.DataFrame:
    dataframes = []
    for file in folder.glob('*.parquet'):
        try:
            df = pd.read_parquet(file, columns=['olpn', 'tote'])
            dataframes.append(df)
        except Exception as e:
            logger.warning(f'Erro ao ler {file.name}: {e}')
    return pd.concat(dataframes, ignore_index=True).drop_duplicates(subset='olpn') if dataframes else pd.DataFrame()
            
