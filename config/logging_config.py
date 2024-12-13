import logging

def setup_logging():
    # Configura o logger para gravar os logs em um arquivo
    logging.basicConfig(
        filename='app.log',  # Nome do arquivo de log
        level=logging.DEBUG,  # Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format='%(asctime)s - %(levelname)s - %(message)s'  # Formato dos logs
    )
    # Se preferir mostrar logs no console também, adicione o StreamHandler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logging.getLogger().addHandler(console_handler)

