import requests
import datetime
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
        print("Connection to SQL Server successful")
    except pyodbc.Error as e:
        print(f"The error '{e}' occurred")

    return connection


def insert_data(connection, startdate, enddate, companyNumber, amount, total):
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO BD_RECEBIVEIS_MENSAL (StartDate, EndDate, CompanyNumber, ValorTotal, Quantidade)
    VALUES (?, ?, ?, ?, ?)
    """
    cursor.execute(insert_query, (startdate, enddate, companyNumber, amount, total))
    connection.commit()


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


def get_tokens():
    url = "https://api.userede.com.br/redelabs/oauth/token"
    body = {
        "grant_type": "password",
        "username": "elton.marinho@bagaggio.com.br",
        "password": ":5@A9hPr9-Po"
    }

    headers = {
        "Authorization": "Basic N2I3OWIyNjUtNjFjMi00YmJiLThlNmItZGE2NDNjMDliMThiOjI3bWJXMnpDeFE="
    }

    response = requests.post(url, data=body, headers=headers)

    if response.status_code == 200:
        data = response.json()
        access_token = data.get("access_token", "")
        refresh_token = data.get("refresh_token", "")
        token_type = data.get("token_type", "")
        expires_in = data.get("expires_in", "")
        scope = data.get("scope", "")

        return access_token, refresh_token, token_type, expires_in, scope
    else:
        print("Falha na obtenção do token. Status code:", response.status_code)
        return None, None, None, None, None


def refresh_access_token(refresh_token):
    url = "https://api.userede.com.br/redelabs/oauth/token"
    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    headers = {
        "Authorization": "Basic N2I3OWIyNjUtNjFjMi00YmJiLThlNmItZGE2NDNjMDliMThiOjI3bWJXMnpDeFE="
    }

    response = requests.post(url, data=body, headers=headers)

    if response.status_code == 200:
        data = response.json()
        access_token = data.get("access_token", "")
        refresh_token = data.get("refresh_token", "")

        return access_token, refresh_token
    else:
        print("Falha na atualização do token. Status code:", response.status_code)
        return None, None


def main():
    access_token, refresh_token, _, _, _ = get_tokens()

    if not access_token or not refresh_token:
        print("Falha na obtenção do token.")
        return

    url = "https://api.userede.com.br/redelabs/merchant-statement/v2/receivables/summary"

    companyNumbers = [
        3016412
    ]


    data = datetime.datetime.now()

    lista_meses = [(data.month + i) % 12 or 12 for i in range(13)]

    for companyNumber in companyNumbers:
        current_year = data.year
        for month in lista_meses:
            if month == 1:
                current_year += 1
            if month in [1, 3, 5, 7, 8, 10, 12]:
                dayfin = 31
            elif month in [4, 6, 9, 11]:
                dayfin = 30
            else:
                dayfin = 28

            if data.month == month:
                dayinit = 1
            else:
                dayinit = 1

            startdate = f"{current_year:02d}-{month:02d}-{dayinit:02d}"
            enddate = f"{current_year:02d}-{month:02d}-{dayfin:02d}"

            params = {
                "startDate": startdate,
                "endDate": enddate,
                "parentCompanyNumber": companyNumber
            }

            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer " + access_token
            }

            response = requests.get(url, params=params, headers=headers)

            if response.status_code == 200:
                content = response.json().get('content')
                if content:
                    amount = content[0]['amount']
                    total = content[0]['total']
                    print(f"Empresa {companyNumber}, Data {startdate}: Amount = {amount}, Total = {total}")
                    insert_data(connection, startdate, enddate, companyNumber, amount, total)
                else:
                    print(f"Empresa {companyNumber}, Data {startdate}: Sem dados.")
                    insert_data(connection, startdate, enddate, companyNumber, 0, 0)
            elif response.status_code == 401:
                new_access_token, new_refresh_token = refresh_access_token(refresh_token)
                if new_access_token:
                    access_token = new_access_token
                    refresh_token = new_refresh_token
                    headers["Authorization"] = "Bearer " + access_token
                    response = requests.get(url, params=params, headers=headers)
                    if response.status_code == 200:
                        content = response.json().get('content')
                        if content:
                            amount = content[0]['amount']
                            total = content[0]['total']
                            print(f"Empresa {companyNumber}, Data {startdate}: Amount = {amount}, Total = {total}")
                            insert_data(connection, startdate, enddate, companyNumber, amount, total)
                        else:
                            print(f"Empresa {companyNumber}, Data {startdate}: Sem dados.")
                            insert_data(connection, startdate, enddate, companyNumber, 0, 0)
                    else:
                        print(f"Erro para a empresa {companyNumber}: {response.status_code}")
                else:
                    print("Falha na atualização do token.")
            else:
                print(f"Erro para a empresa {companyNumber}: {response.status_code}")


if __name__ == "__main__":
    main()
