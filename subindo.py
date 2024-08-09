import pyodbc

def create_connection(driver, server, database, user, password, port):
    connection = None
    try:
        connection = pyodbc.connect(
            f'DRIVER={{{driver}}};'
            f'SERVER={server},{port};'
            f'DATABASE={database};'
            f'UID={user};'
            f'PWD={password}'
        )
        print("Connection to SQL Server DB successful")
    except pyodbc.Error as e:
        print(f"The error '{e}' occurred")

    return connection

def insert_data(connection, table_name, data):
    cursor = connection.cursor()
    placeholders = ', '.join(['?'] * len(data[0]))
    columns = ', '.join(data[0].keys())
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    for entry in data:
        cursor.execute(sql, list(entry.values()))

    connection.commit()


driver = "ODBC Driver 17 for SQL Server"
server = "187.0.198.167"
user = "victor.oliveira"
password = "@primo01"
database = "DADOS_EXCEL"
port = 41433

# Data to be inserted
data = [
    {"campo1": "Fluminense", "campo2": "Campeão", "campo3": "da América"},
    {"campo1": "Remadores da Gávea", "campo2": "Time de", "campo3": "Verme"},
    {"campo1": "Vai", "campo2": "Virar", "campo3": "Baile"}
    # Add more rows as needed
]

# Assuming the table is in the 'dbo' schema
table_name = "dbo.data_teste"

connection = create_connection(driver, server, database, user, password, port)

if connection:
    print("Código está correto e a conexão foi estabelecida com sucesso.")
    insert_data(connection, table_name, data)
    print("Dados inseridos com sucesso.")
else:
    print("Código está correto, mas não foi possível estabelecer a conexão.")
