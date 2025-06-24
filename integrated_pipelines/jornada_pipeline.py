import os
from pathlib import Path
import pandas as pd
from utils.logging_utils import setup_logging
from config.settings import PIPELINE_CONFIG, PIPELINE_PATHS

logger = setup_logging()

class JornadaPipeline:
    def __init__(self):
        self.pipeline_key = 'jornada'
        self.cfg = PIPELINE_CONFIG[self.pipeline_key]
        self.paths = PIPELINE_PATHS[self.pipeline_key]

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df.drop(columns=self.cfg.get('remove_columns', []), errors='ignore', inplace=True)
        df.rename(columns=self.cfg.get('rename_columns', {}), inplace=True)

        for col, dtype in self.cfg.get('column_types', {}).items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)
        
        for col in self.cfg.get('datetime_columns', []):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

        if 'hora' in df.columns:
            df = df.loc[df['hora'].notna() & (df['hora'] != '')].copy()
            df['hora'] = pd.to_datetime(df['hora'], format='%H:%M', errors='coerce').dt.strftime('%H:%M')
            df['jornada_segundos'] = pd.to_timedelta(df['hora'] + ':00').dt.total_seconds().astype('Int64')

        df['matricula_formatada'] = df['matricula'].astype('Int64').astype(str).str.zfill(8)
        df['cod'] = pd.to_numeric(df['cod'], errors='coerce').fillna(0).astype('Int64').astype(str)
        df['login'] = df['cod'] + df['matricula_formatada'] + '@viavarejo.com.br'
        df['data_criterio'] = df['data'].dt.strftime('%d-%m-%Y')
        df['mes_ano'] = df['data'].dt.strftime('%m-%Y')

        required_cols = ['data', 'hora', 'jornada_segundos', 'login']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f'Colunas ausentes após preprocessamento: {missing_cols}')
            return pd.DataFrame()
        
        return df

    def export_by_month(self, df: pd.DataFrame, output_folder: Path):
        output_folder.mkdir(parents=True, exist_ok=True)
        for mes_ano, group in df.groupby('mes_ano'):
            output_file = output_folder / f'dados_{mes_ano}.csv'
            group.drop(columns='mes_ano').to_csv(
                output_file,
                index=False,
                encoding=self.cfg.get('encoding', 'ascii'),
                sep=self.cfg.get('sep', ';')
            )
            logger.info(f'Salvo: {output_file.name} ({len(group):.0f} linhas)')

    def convert_csv_to_parquet(self, folder_csv: Path, folder_parquet: Path):
        folder_parquet.mkdir(parents=True, exist_ok=True)
        encoding = self.cfg.get('encoding', 'ascii')
        sep = self.cfg.get('sep', ';')
        dtype = self.cfg.get('column_types', {})
        parse_dates = self.cfg.get('datetime_columns', [])

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

    def run(self) -> pd.DataFrame:
        try:
            input_path = self.paths['raw']
            processed_path = self.paths['processed']
            parquet_path = self.paths['parquet']

            input_path.mkdir(parents=True, exist_ok=True)
            processed_path.mkdir(parents=True, exist_ok=True)
            parquet_path.mkdir(parents=True, exist_ok=True)

            all_files = list(input_path.glob('*.csv'))
            if not all_files:
                logger.warning('Nenhum arquivo CSV encontrado para processar.')
                return pd.DataFrame()

            final_df = pd.DataFrame()

            for file in all_files:
                logger.info(f'Processando: {file.name}')
                df = pd.read_csv(
                    file,
                    sep=self.cfg.get('sep', ';'),
                    encoding=self.cfg.get('encoding', 'ascii'),
                    dtype=self.cfg.get('column_types', {}),
                    low_memory=False
                )
                if not df.empty:
                    df_proc = self.preprocess(df)
                    if not df_proc.empty:
                        self.export_by_month(df_proc, processed_path)
                        final_df = pd.concat([final_df, df_proc], ignore_index=True)

            self.convert_csv_to_parquet(processed_path, parquet_path)
            return final_df

        except Exception as e:
            logger.error(f'Falha no pipeline jornada: {e}')
            return pd.DataFrame()
