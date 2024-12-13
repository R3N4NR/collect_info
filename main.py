import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
import logging
from config.logging_config import setup_logging
from system.collector import update_system_metrics, prepare_data_for_db
from system.metrics import collect_system_metrics
from db.database import connect_to_postgresql

def main():
    setup_logging()

    try:
        # Conectar ao banco de dados
        conn = connect_to_postgresql()

        # Coletar as métricas do sistema
        data = collect_system_metrics()

        # Preparar dados para o banco de dados
        prepare_data_for_db(data)

        # Inserir os dados no banco de dados
        update_system_metrics(conn)
        print(sys.path)
    except Exception as e:
        # Log de erro crítico se ocorrer algum problema
        logging.critical(f"Erro fatal, o script não pode continuar: {e}")


if __name__ == "__main__":
    main()
