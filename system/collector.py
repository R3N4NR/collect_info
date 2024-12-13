import time
from datetime import datetime
from queue import Queue
import logging
from db.database import connect_to_postgresql
from system.metrics import collect_system_metrics
from db.tables import create_database, create_tables

# Criar buffer para dados
data_buffer = Queue()

def prepare_data_for_db(data):
    try:
        data_buffer.put(data)
        logging.info(f"Dados preparados e colocados no buffer: {data}")
    except Exception as e:
        logging.error(f"Erro ao preparar os dados para o buffer: {e}")

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
                cursor.execute('''SELECT id FROM dados_monitoramento WHERE id_computador = %s''', (id_computador,))
                existing_computer = cursor.fetchone()

                if existing_computer:
                    # Se o computador já existe, faz a atualização
                    cursor.execute('''UPDATE dados_monitoramento
                                      SET hostname = %s, id_computador = %s, ip_local = %s, cpu_info = %s, cpu_percent = %s, 
                                          memoria_total = %s, memoria_livre = %s, ram_percent = %s, data_coleta = %s
                                      WHERE id = %s''', (
                        data['hostname'], id_computador, data['ip_local'], data['cpu_info'], data['cpu_percent'],
                        data['memoria_total'], data['memoria_livre'], data['ram_percent'], datetime.now(), existing_computer[0]
                    ))
                    id_dados_monitoramento = existing_computer[0]
                else:
                    # Se o computador não existe, insere um novo registro
                    cursor.execute('''INSERT INTO dados_monitoramento (hostname, id_computador, ip_local, cpu_info, cpu_percent, memoria_total, memoria_livre, ram_percent, data_coleta)
                                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                      RETURNING id''', (
                        data['hostname'], id_computador, data['ip_local'], data['cpu_info'], data['cpu_percent'],
                        data['memoria_total'], data['memoria_livre'], data['ram_percent'], datetime.now()
                    ))
                    id_dados_monitoramento = cursor.fetchone()[0]
                    if id_dados_monitoramento:
                        id_dados_monitoramento = id_dados_monitoramento[0]
                    else:
                        logging.error("Erro ao inserir dados na tabela dados_monitoramento.")
                        continue

                # Inserir os dados das partições
                for disk in data['partitions']:
                    cursor.execute('''SELECT id FROM discos WHERE id_computador = %s AND disco = %s''', (id_computador, disk['disco']))
                    existing_disk = cursor.fetchone()

                    if existing_disk:
                        # Se o disco já existe, faz a atualização
                        cursor.execute('''UPDATE discos
                                          SET disco = %s, disco_total = %s, disco_livre = %s, disk_percent = %s, data_coleta = %s
                                          WHERE id = %s''', (
                            disk['disco'],disk['espaco_total_GB'], disk['espaco_livre_GB'], disk['uso_percentual'], datetime.now(), existing_disk[0]
                        ))
                        id_discos = existing_disk[0]
                        logging.info(f"Disco {disk['disco']} atualizado para o computador MAC {data['mac']}")
                    else:
                        # Se o disco não existe, insere um novo registro
                        cursor.execute('''INSERT INTO discos (id_computador, disco, disco_total, disco_livre, disk_percent)
                                          VALUES (%s, %s, %s, %s, %s)
                                          RETURNING id''', (
                            id_computador, disk['disco'], disk['espaco_total_GB'], disk['espaco_livre_GB'], disk['uso_percentual'], datetime.now()
                        ))
                        id_discos = cursor.fetchone()
                        if id_discos:
                            id_discos = id_discos[0]
                            logging.info(f"Disco {disk['disco']} inserido para o computador MAC {data['mac']}")
                        else:
                            logging.error(f"Erro ao inserir o disco {disk['disco']} para o computador MAC {data['mac']}")
                            continue

                    # Atualiza a tabela dados_monitoramento com o id do disco
                    cursor.execute('''UPDATE dados_monitoramento SET id_discos = %s WHERE id = %s''', (id_discos, id_dados_monitoramento))

                db_connection.commit()  # Commit centralizado após todas as transações
                logging.info(f"Dados de discos e do computador MAC {data['mac']} atualizados ou inseridos com sucesso.")
                
            time.sleep(1)

        except Exception as e:
            logging.error(f"Erro inesperado ao processar os dados: {e}")

def get_computer_id_by_mac(cursor, mac):
    cursor.execute('''SELECT id FROM computadores WHERE mac = %s''', (mac,))
    result = cursor.fetchone()
    return result[0] if result else None
