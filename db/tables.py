import psycopg
from db.database import connect_to_postgresql
def create_database():
    conn = connect_to_postgresql("postgres")
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'monitoramento'")
    exists = cursor.fetchone()

    if not exists:
        cursor.execute("CREATE DATABASE monitoramento")
        print("Banco de dados 'monitoramento' criado com sucesso!")
    else:
        print("Banco de dados 'monitoramento' já existe.")

    cursor.close()
    conn.close()

def create_tables():
    conn = connect_to_postgresql("monitoramento")
    cursor = conn.cursor()

    cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS computadores (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        mac VARCHAR(20) UNIQUE NOT NULL,
        hostname VARCHAR(255) UNIQUE NOT NULL,
        data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS discos (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_computador UUID NOT NULL,
        disco VARCHAR(255) NOT NULL,
        disco_total DECIMAL(10,2) NOT NULL,
        disco_livre DECIMAL(10,2) NOT NULL,
        disk_percent DECIMAL(5,2) NOT NULL,
        data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_computador FOREIGN KEY (id_computador) REFERENCES computadores(id) ON DELETE CASCADE
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dados_monitoramento (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        id_computador UUID REFERENCES computadores(id) ON DELETE CASCADE,
        id_discos UUID REFERENCES discos(id) ON DELETE CASCADE,
        hostname VARCHAR(255),
        cpu_info VARCHAR(255),
        cpu_percent DECIMAL(5,2),
        memoria_total DECIMAL(10,2),
        memoria_livre DECIMAL(10,2),
        ram_percent DECIMAL(5,2),
        ip_local VARCHAR(20),
        data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    conn.commit()
    print("Tabelas criadas com sucesso!")

    cursor.close()
    conn.close()
