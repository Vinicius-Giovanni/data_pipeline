from pathlib import Path
from utils.io import read_all_csv
from utils.transformation import export_by_month, convert_csv_to_parquet
from utils.logging_utils import setup_logging
from config.settings import PIPELINE_CONFIG

logger = setup_logging()

class BasePipeline:
    def __init__(self, config_key: str):
        self.key = config_key
        self.config = PIPELINE_CONFIG.get(config_key)

        if not self.config:
            logger.error(f'Pipeline "{config_key}" não encontrado no PIPELINE_CONFIG.')
            raise ValueError(f'Pipeline "{config_key}" inválido.')
        
    def run(self, input_path: Path, output_csv_dir: Path, output_parquet_dir: Path):
        logger.info(f'Executando pipeline: {self.key.upper()}')

        df = read_all_csv(input_path, pipeline_key=self.key)

        if df.empty:
            logger.warning('Nenhum dado encontrado no diretório de entrada.')
            return df

        df = self.preprocess(df)

        if 'mes_ano' not in df.columns:
            logger.error('Coluna "mes_ano" ausente após preprocessamento. Abortando exportação.')
            return df

        output_csv_dir.mkdir(parents=True, exist_ok=True)
        output_parquet_dir.mkdir(parents=True, exist_ok=True)

        export_by_month(df, output_csv_dir, self.key)
        convert_csv_to_parquet(output_csv_dir, output_parquet_dir, self.key)

        logger.info(f'Pipeline "{self.key}" finalizado com sucesso.')
        return df
    
    def preprocess(self, df):
        raise NotImplementedError('Subclasse deve implementar esta função.')
