from pathlib import Path
import time
from utils.logging_utils import setup_logging
from pipeline.olpn_pipeline import OlpnPipeline
from pipeline.picking_pipeline import PickingPipeline
from pipeline.packing_pipeline import PackingPipeline
from pipeline.cancel_pipeline import CancelPipeline
from pipeline.load_pipeline import LoadPipeline
from pipeline.putaway_pipeline import PutawayPipeline
from integrated_pipelines.jornada_pipeline import JornadaPipeline
from integrated_pipelines.time_lead_olpn import TimeLeadOLPNPipeline
from integrated_pipelines.bottleneck_box import BottleneckBoxPipeline
from integrated_pipelines.bottleneck_salao import BottleneckSalaoPipeline
from config.settings import PIPELINE_PATHS

logger = setup_logging()

def process_pipeline():
    start_time = time.time()
    paths = PIPELINE_PATHS
    
    # --- Pipelines baseados na BasePipeline
    pipelines = {
        'olpn': OlpnPipeline(),
        'picking': PickingPipeline(),
        'cancel': CancelPipeline(),
        'load': LoadPipeline(),
        'putaway': PutawayPipeline(),
        'packing': PackingPipeline()
    }

    for key, pipeline in pipelines.items():
        logger.info(f'Iniciando execução do pipeline: {key.upper()}')

        input_path = paths[key]['raw']
        processed_path = paths[key]['processed']
        parquet_path = paths[key]['parquet']

        processed_path.mkdir(parents=True, exist_ok=True)
        parquet_path.mkdir(parents=True, exist_ok=True)

        df = pipeline.run(input_path, processed_path, parquet_path)

        if not df.empty:
            logger.info(f'Pipeline {key.upper()} executado com sucesso.')
        else:
            logger.warning(f'Pipeline {key.upper()} não gerou dados.')

    # --- Pipeline jornada (Estrutura autogerenciada) ---
    logger.info('Iniciando execução do pipeline: JORNADA')
    jornada = JornadaPipeline()
    df_jornada = jornada.run()

    if not df_jornada.empty:
        logger.info('Pipeline JORNADA executado com sucesso.')
    else:
        logger.warning('Pipeline JORNADA não gerou dados.')

    # --- Pipeline Time Lead OLPN (Estrutura autogerenciada) ---
    logger.info('Iniciando execução do pipeline: TIME_LEAD_OLPN')
    time_lead_pipeline = TimeLeadOLPNPipeline()
    df_time_lead = time_lead_pipeline.run()

    if not df_time_lead.empty:
        logger.info('Pipeline TIME_LEAD_OLPN executado com sucesso.')
    else:
        logger.warning('Pipeline TIME_LEAD_OLPN não gerou dados.')

    total_time = time.time() - start_time
    logger.info(f'Processamento finalizado em {total_time:.2f} segundos.')

    # --- Pipeline bottleneck_salao (Estrutura autogerenciada) ---
    logger.info('Iniciando execução do pipeline: BOTTLENECK_SALAO')
    bottleneck_salao = BottleneckSalaoPipeline()
    df_bottleneck_salao = bottleneck_salao.run()

    if not df_bottleneck_salao.empty:
        logger.info('Pipeline BOTTLENECK_SALAO executado com sucesso.')
    else:
        logger.warning('Pipeline BOTTLENECK_SALAO não gerou dados.')

    # --- Pipeline bottleneck_box (Estrutura autogerenciada) ---
    logger.info('Iniciando execução do pipeline: BOTTLENECK_BOX')
    bottleneck_box = BottleneckBoxPipeline()
    df_bottleneck_box = bottleneck_box.run()

    if not df_bottleneck_box.empty:
        logger.info('Pipeline BOTTLENECK_BOX executado com sucesso.')
    else:
        logger.warning('Pipeline BOTTLENECK_BOX não gerou dados.')

if __name__ == '__main__':
    process_pipeline()
