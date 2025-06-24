from pathlib import Path
import pandas as pd
from config.settings import PIPELINE_CONFIG, PIPELINE_PATHS
from utils.logging_utils import setup_logging

logger = setup_logging()

class TimeLeadOLPNPipeline:
    def __init__(self):
        self.pipeline_key = 'time_lead_olpn'
        self.cfg = PIPELINE_CONFIG[self.pipeline_key]
        self.paths = PIPELINE_PATHS[self.pipeline_key]

    def read_parquet_files(self, folder: Path) -> pd.DataFrame:
        dfs = []
        for file in folder.rglob('*.parquet'):
            try:
                df = pd.read_parquet(file, columns=self.cfg['read_columns'])
                dfs.append(df)
                logger.info(f'Lido: {file.name}')
            except Exception as e:
                logger.warning(f'Erro ao ler {file.name}: {e}')
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            logger.warning('DataFrame de entrada vazio.')
            return df
        
        for col, dtype in self.cfg.get('column_types', {}).items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)
            
        for col in self.cfg.get('datetime_columns', []):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        df = df.drop_duplicates(subset='olpn', keep='first')
        df = df.dropna(subset=['data_hora_load'])

        df['diferenca_segundos'] = (
            (df['data_hora_load'] - df['data_pedido'])
            .dt.total_seconds()
            .abs()
            .astype('Int64')
            )
        
        df['hora'] = df['data_hora_load'].dt.strftime('%H:00:00')
        df['data_criterio'] = df['data_hora_load'].dt.strftime('%d-%m-%Y')
        df['mes_ano'] = df['data_hora_load'].dt.strftime('%m-%Y')

        return df
    
    def export_by_month(self, df: pd.DataFrame, output_folder: Path):
        output_folder.mkdir(parents=True, exist_ok=True)
        for mes_ano, group in df.groupby('mes_ano'):
            file_path = output_folder / f'dados_{mes_ano}.parquet'
            group.drop(columns='mes_ano').to_parquet(file_path, index=False)
            logger.info(f'Exportando: {file_path.name} ({len(group)} linhas)')

    def run(self) -> pd.DataFrame:
        try:
            input_path = self.paths['parquet_load']
            output_path = self.paths['output_parquet']

            logger.info('Iniciando leitura dos dados...')
            df = self.read_parquet_files(input_path)

            if df.empty:
                logger.warning('Nenhum dado encontrado para processar.')
                return pd.DataFrame()
            
            logger.info("Executando transformação...")
            df_proc = self.preprocess(df)

            if df_proc.empty:
                logger.warning("Nenhum dado restante após o preprocessamento.")
                return pd.DataFrame()
            
            logger.info("Exportando por mês...")
            self.export_by_month(df_proc, output_path)

            logger.info('Pipeline time_lead_olpn concluído com sucesso.')
            return df_proc

        except Exception as e:
            logger.error(f'Erro na execução do pipeline time_lead_olpn: {e}')
            return pd.DataFrame()