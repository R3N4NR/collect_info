import psycopg
import time
from datetime import datetime
from queue import Queue
import logging
import psutil
from getmac import get_mac_address
import socket
import cpuinfo

# Configuração do log
logging.basicConfig(
    filename='system_metrics.log',  # Arquivo onde os logs serão salvos
    level=logging.DEBUG,  # Nível de log (DEBUG para registrar tudo)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Formato do log
)

# Criar um "berço" (buffer) para armazenar os dados antes de enviar ao banco de dados
data_buffer = Queue()

# Função para coletar as métricas do sistema
def collect_system_metrics():
    discos_info = []  # Lista que armazenará as informações de todas as partições
    # Coleta de dados
    hostname = socket.gethostname()
    cpu_info = cpuinfo.get_cpu_info()
    cpu_percent = psutil.cpu_percent(interval=1)
    virtual_memory = psutil.virtual_memory()
    memoria_total = virtual_memory.total / (1024 ** 3)
    memoria_livre = virtual_memory.available / (1024 ** 3)
    ram_percent = virtual_memory.percent
    disk_usage = psutil.disk_usage('/')
    local_ip = socket.gethostbyname(socket.gethostname())
    
    # Substituir os ":" no MAC address por "-" antes de usá-lo
    mac = get_mac_address().upper()
    if mac:
        mac = mac.replace(":", "-")

        partitions = psutil.disk_partitions()  # Obtém as partições do disco
    
    for partition in partitions:
        # Verificando o acesso à partição e coletando as informações de disco
        try:
            disk_usage = psutil.disk_usage(partition.mountpoint)  # Obtém informações de uso para cada partição
            
            # Convertendo os valores para GB e incluindo os dados na lista
            disk_data = {
                "disco": partition.device,
                "espaco_total_GB": disk_usage.total / (1024 ** 3),  # Convertendo para GB
                "espaco_livre_GB": disk_usage.free / (1024 ** 3),  # Convertendo para GB
                "uso_percentual": disk_usage.percent
            }

            # Verificando se os valores foram capturados corretamente
            logging.debug(f"Informações do disco: {disk_data}")
            discos_info.append(disk_data)
        
        except PermissionError:
            # Caso não seja possível acessar a partição, registrar o erro
            logging.error(f"Erro ao acessar a partição {partition.device}.")
            continue

    # Coleta das métricas gerais do sistema
    return {
        'hostname': hostname,  # Nome do host do computador
        'ip_local': local_ip,
        'mac': mac,
        'cpu_info': cpu_info['brand_raw'],
        'cpu_percent': cpu_percent,
        'memoria_total': memoria_total,
        'memoria_livre': memoria_livre,
        'ram_percent': ram_percent,
        'partitions': discos_info  # Retorna a lista completa de informações das partições
    }

# Função para preparar os dados e colocá-los no buffer
def prepare_data_for_db(data):
    try:
        data_buffer.put(data)  # Coloca os dados no "berço" (buffer)
        logging.info(f"Dados preparados e colocados no buffer: {data}")
    except Exception as e:
        logging.error(f"Erro ao preparar os dados para o buffer: {e}")

# Função para obter o id_computador a partir do MAC address
def get_computer_id_by_mac(cursor, mac):
    cursor.execute('''SELECT id FROM computadores WHERE mac = %s''', (mac,))
    result = cursor.fetchone()
    return result[0] if result else None

# Função para inserir ou atualizar os dados no banco de dados
def update_system_metrics(db_connection):
    cursor = db_connection.cursor()

    while True:
        try:
            if not data_buffer.empty():
                # Retira dados do buffer
                data = data_buffer.get()
                logging.debug(f"Dados retirados do buffer: {data}")  # Verifique os dados retirados do buffer
                id_computador = get_computer_id_by_mac(cursor, data['mac'])
                if not id_computador:
                    logging.error(f"Erro: Computador com MAC {data['mac']} e hostname {data['hostname']} não encontrado no banco de dados.")
                    continue  # Continua o loop sem processar este dado
                
                # Verifica se o computador já existe na tabela dados_monitoramento
                cursor.execute('''
                    SELECT id FROM dados_monitoramento WHERE id_computador = %s
                ''', (id_computador,))
                existing_computer = cursor.fetchone()
        
                if existing_computer:
                    # Se o computador já existe, faz a atualização
                    cursor.execute('''
                        UPDATE dados_monitoramento
                        SET hostname = %s, id_computador = %s, ip_local = %s, cpu_info = %s, cpu_percent = %s, 
                            memoria_total = %s, memoria_livre = %s, ram_percent = %s, data_coleta = %s
                        WHERE id = %s
                    ''', (
                        data['hostname'], id_computador, data['ip_local'], data['cpu_info'], data['cpu_percent'],
                        data['memoria_total'], data['memoria_livre'], data['ram_percent'], datetime.now(), existing_computer[0]
                    ))
                else:
                    # Se o computador não existe, insere um novo registro
                    cursor.execute('''
                        INSERT INTO dados_monitoramento (hostname, id_computador, ip_local, cpu_info, cpu_percent, memoria_total, memoria_livre, ram_percent, data_coleta)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    ''', (
                        data['hostname'], id_computador, data['ip_local'], data['cpu_info'], data['cpu_percent'],
                        data['memoria_total'], data['memoria_livre'], data['ram_percent'], datetime.now()
                    ))
                id_dados_monitoramento = cursor.fetchone()[0]  # Recupera o ID do novo registro

                db_connection.commit()  # Commit da transação para garantir que os dados sejam persistidos
                logging.info(f"Dados inseridos ou atualizados no banco para o computador MAC {data['mac']}")

                # Inserir os dados das partições
                for disk in data['partitions']:
                # Verifica se o disco já existe
                    cursor.execute('''
                        SELECT id FROM discos WHERE id_computador = %s AND disco = %s
                        ''', (id_computador, disk['disco']))
                    existing_disk = cursor.fetchone()

                    if existing_disk:
                # Se o disco já existe, faz a atualização
                        cursor.execute('''
                        UPDATE discos
                        SET disco_total = %s, disco_livre = %s, disk_percent = %s
                        WHERE id = %s
                        ''', (
                        disk['espaco_total_GB'], disk['espaco_livre_GB'], disk['uso_percentual'], existing_disk[0]
                         ))
                        logging.info(f"Disco {disk['disco']} atualizado para o computador MAC {data['mac']}")
                    else:
                    # Se o disco não existe, insere um novo registro
                        cursor.execute('''
                        INSERT INTO discos (id_computador, disco, disco_total, disco_livre, disk_percent)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id
                        ''', (
                        id_computador, disk['disco'], disk['espaco_total_GB'], disk['espaco_livre_GB'], disk['uso_percentual']
                        ))
                        id_disco = cursor.fetchone()[0]  # Recupera o id do novo disco inserido
                        logging.info(f"Disco {disk['disco']} inserido para o computador MAC {data['mac']}")

                    # Agora, atualiza a tabela dados_monitoramento com o id do novo disco
                        cursor.execute('''
                        UPDATE dados_monitoramento
                        SET id_discos = %s
                        WHERE id_computador = %s
                        ''', (id_disco, id_computador))

                        logging.info(f"ID do disco {id_disco} foi atualizado na tabela dados_monitoramento para o computador MAC {data['mac']}")

                    db_connection.commit()  # Commit para as inserções dos discos
                    logging.info(f"Dados de discos inseridos para o computador MAC {data['mac']}")

            # Atraso para simular um processo sequencial e evitar sobrecarga
            time.sleep(1)

        except psycopg.Error as e:
            logging.error(f"Erro ao executar comando SQL: {e}")
            db_connection.rollback()  # Rollback em caso de erro na transação
        except Exception as e:
            logging.error(f"Erro inesperado ao processar os dados: {e}")

# Função principal que coordena a execução
def main():
    try:
        # Conectar ao banco de dados
        conn = psycopg.connect(
            dbname="monitoramento",
            user="postgres",
            password="root",
            host="localhost",  # ou o IP do servidor do banco de dados
            port="5432"  # Porta padrão do PostgreSQL
        )
        logging.info("Conexão ao banco de dados estabelecida com sucesso.")

        # Coleta as métricas do sistema
        data = collect_system_metrics()

        # Exibe os dados coletados
        logging.info(f"Dados coletados: {data}")

        # Envia os dados para o buffer
        prepare_data_for_db(data)

        # Processa a inserção dos dados no banco de dados
        update_system_metrics(conn)

    except Exception as e:
        logging.critical(f"Erro fatal, o script não pode continuar: {e}")

# Executa a função principal
if __name__ == "__main__":
    main()
