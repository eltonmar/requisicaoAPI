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
        print("Connection to MySQL successful")
    except pyodbc.Error as e:
        print(f"The error '{e}' occurred")

    return connection


driver = "ODBC Driver 17 for SQL Server"
server = "187.0.198.167"
user = "victor.oliveira"
password = "@primo01"
database = "DADOS_EXCEL"
port = 41433

connection = create_connection(driver, server, database, user, password, port)

if connection:
    print("Código está correto e a conexão foi estabelecida com sucesso.")
else:
    print("Código está correto, mas não foi possível estabelecer a conexão.")
