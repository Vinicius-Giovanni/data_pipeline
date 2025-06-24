from pathlib import Path
import pandas as pd
from config.settings import PIPELINE_CONFIG, PIPELINE_PATHS
from utils.logging_utils import setup_logging

logger = setup_logging()

class BottleneckSalaoPipeline:
    def __init__(self):
        self.pipeline_key = 'bottleneck_salao'
        self.cfg = PIPELINE_CONFIG[self.pipeline_key]
        self.paths = PIPELINE_PATHS[self.pipeline_key]

    def read_parquet_folder(self, folder_path: Path, columns: list[str]) -> pd.DataFrame:
        dfs = []
        for file in Path(folder_path).rglob('*.parquet'):
            try:
                df = pd.read_parquet(file, columns=columns)
                dfs.append(df)
                logger.info(f'Lido: {file.name}')
            except Exception as e:
                logger.warning(f'Erro ao ler {file.name}: {e}')
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    def run(self) -> pd.DataFrame:
        try:
            logger.info('Iniciando leitura dos dados...')

            df_packed = self.read_parquet_folder(
                self.paths['parquet_packed'],
                self.cfg['read_columns_packed']
            )
            df_putaway = self.read_parquet_folder(
                self.paths['parquet_putaway'],
                self.cfg['read_columns_putaway']
            )

            if df_packed.empty or df_putaway.empty:
                logger.warning('Dados de packed ou putaway estão vazios.')
                return pd.DataFrame()

            df_packed['olpn'] = df_packed['olpn'].astype(self.cfg['column_type']['olpn'])
            df_putaway['olpn'] = df_putaway['olpn'].astype(self.cfg['column_type']['olpn'])

            df_all = pd.merge(df_packed, df_putaway, on='olpn', how='left')
            df_all = df_all.drop_duplicates(subset='olpn', keep='first')
            df_all = df_all.dropna(subset=['data_hora_putaway'])

            for col in self.cfg['datetime_columns']:
                if col in df_all.columns:
                    df_all[col] = pd.to_datetime(df_all[col], errors='coerce')
            
            df_all['diferenca_segundos'] = (
                (df_all['data_hora_putaway'] - df_all['data_hora_fim_olpn'])
                .dt.total_seconds()
            ).astype('Int64')

            df_all['data_criterio'] = df_all['data_hora_putaway'].dt.strftime('%d-%m-%Y')
            df_all['mes_ano'] = df_all['data_hora_putaway'].dt.strftime('%m-%Y')

            output_path = self.paths['output_parquet']
            output_path.mkdir(parents=True, exist_ok=True)

            logger.info('Exportando por mês...')
            for name, group in df_all.groupby('mes_ano'):
                file_path = output_path / f'dados_{name}.parquet'
                group.drop(columns='mes_ano').to_parquet(file_path, index=False)
                logger.info(f'Arquivo gerado: {file_path.name} ({len(group)} linhas)')
            
            logger.info('Pipeline BOTTLENECK_SALAO executado com sucesso.')
            return df_all
        
        except Exception as e:
            logger.error(f'Erro na execução do pipeline bottleneck_salao: {e}')
            return pd.DataFrame()