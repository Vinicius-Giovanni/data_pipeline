import os
import pandas as pd
import re
from typing import Union, Tuple
from unidecode import unidecode
from pathlib import Path
from config.settings import PIPELINE_CONFIG, MOTIVOS_OFICIAIS, MAPEAMENTO_TEXTUAL, REGRAS_DIRETAS
from utils.logging_utils import setup_logging

logger = setup_logging()

def export_by_month(df: pd.DataFrame, output_folder: Path, pipeline_key: str):
    config = PIPELINE_CONFIG.get(pipeline_key)
    if not config:
        logger.error(f'Pipeline "{pipeline_key}" não encontrado no settings.')
        return 
    
    for mes_ano, group in df.groupby('mes_ano'):
        output_file = output_folder / f'dados_{mes_ano}.csv'
        group.drop(columns='mes_ano').to_csv(output_file, index=False, encoding='utf-16', sep='\t')
        logger.info(f'Salvo: {output_file.name} ({len(group):,} linhas)'.replace(',', '.'))

def convert_csv_to_parquet(folder_csv: Path, folder_parquet: Path, pipeline_key: str):
    config = PIPELINE_CONFIG.get(pipeline_key)
    if not config:
        logger.error(f'Pipeline "{pipeline_key}" não encontrado no settings.')
        return 
    
    encoding = config.get('encoding', 'utf-8')
    sep = config.get('sep', ',')
    dtype = config.get('column_types', {})
    parse_dates = config.get('datetime_columns', [])

    for file in os.listdir(folder_csv):
        if not file.endswith('.csv'):
            continue

        csv_path = folder_csv / file
        parquet_path = folder_parquet / f'{os.path.splitext(file)[0]}.parquet'

        try:
            df = pd.read_csv(
                csv_path,
                sep=sep,
                encoding=encoding,
                dtype=dtype,
                parse_dates=parse_dates,
                low_memory=False
            )

            df.to_parquet(parquet_path, engine='pyarrow', index=False)
            logger.info(f'Convertido: {file} -> {parquet_path.name}')
        except Exception as e:
            logger.error(f'Erro ao converter {file}: {e}')

def normalizar_motivo(valor: Union[str, float]) -> Tuple[int, str]:
    try:
        if pd.isna(valor) or str(valor).strip() =='':
            return 1, MOTIVOS_OFICIAIS[1]
        
        valor = unidecode(str(valor).lower().strip())
        valor = re.sub(r'[^\w\s]','',valor)

        if valor.isdigit():
            codigo = int(valor)
            if codigo in MOTIVOS_OFICIAIS:
                return codigo, MOTIVOS_OFICIAIS[codigo]
            
        for codigo, must_have, *optional in REGRAS_DIRETAS:
            if all(term in valor for term in must_have):
                if not optional or any(opt in valor for opt in optional[0]):
                    return codigo, MOTIVOS_OFICIAIS[codigo]
        
        for codigo, padroes in MAPEAMENTO_TEXTUAL.items():
            if any(re.search(p, valor) for p in padroes):
                return codigo, MOTIVOS_OFICIAIS[codigo]
        
        return 1,MOTIVOS_OFICIAIS[1]
    except Exception as e:
        logger.warning(f"Erro ao normalizar motivo '{valor}': {e}")
        return 1, MOTIVOS_OFICIAIS[1]