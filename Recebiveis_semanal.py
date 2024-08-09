import requests
import datetime
import pyodbc
import schedule
import time

def job():
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

    def truncate_table(connection):
        try:
            with connection.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE BD_RECEBIVEIS_SEMANAL")
                connection.commit()
                print("Tabela foi truncada com sucesso.")
        except pyodbc.Error as e:
            print(f"O erro foi: {e}")

    def insert_data(connection, startdate, enddate, companyNumber, amount, total):
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO BD_RECEBIVEIS_SEMANAL (StartDate, EndDate, CompanyNumber, ValorTotal, Quantidade)
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

        truncate_table(connection)

        url = "https://api.userede.com.br/redelabs/merchant-statement/v2/receivables/summary"

        companyNumbers = [
            3016412, 84232633, 84232668, 32143460, 3111628, 73938807, 32144024, 32144261,
            95515208, 75539098, 87805405, 73986313, 95400206, 32144890, 85788600, 32144970,
            32145128, 32145268, 92320384, 32145403, 32145454, 32145497, 32145586, 32145667,
            23816090, 84417072, 84232706, 33810311, 32145772, 84232749, 86819747, 84233052,
            84233079, 91494117, 84382104, 91289548, 84233117, 86006118, 86011553, 84233184,
            75538695, 95515216, 73887145, 86930168, 95515224, 75527073, 95515232, 73986461,
            88244296, 95515240, 75557487, 82370591, 87814773, 95515267, 73836281, 87815397,
            84232897, 86982109, 84232935, 195618, 86820052, 95102450, 95515275, 75539330,
            91590663, 95593543, 75539268, 89351746, 95515283, 75557584, 84233028, 82370680,
            82370702, 88244202, 88244440, 32145330, 82370583, 82370605, 75539527, 93419244,
            95515291, 75557614, 95515313, 82370664, 82370494, 75557525, 95593659, 75539535,
            83830200, 94959579, 83830030, 95207864, 82370648, 82370672, 94959455, 82370699,
            95006346, 82370737, 95367217, 82370753, 82370788, 82370621, 82370710, 82370745,
            82370923, 95207872, 82370800, 82370850, 95515321, 82370940, 95515330, 82370885,
            82370907, 83928731, 83928774, 83928790, 83928812, 83967460, 83885625, 84037857,
            84013842, 88244580, 88645940, 88646203, 88646351, 89162420, 89339622, 89322908,
            89912764, 89910354, 90246225, 91489520, 91808316, 91917697, 92069207, 92761860,
            92969992, 92953298, 93053827, 93530218, 95515356, 93569580, 93717393, 95515372,
            95515402, 95515410, 95515445, 95515496, 95515500, 95008292, 95515518, 95515526,
            95207880, 95515534, 95515542, 95515550, 94959331, 95008500, 95515569, 95515577,
            94959102, 95008845, 94959072, 94959056, 95012095, 94959013, 95207899, 95562052,
            95370234, 95703330, 95634380, 95562362, 95645063, 95944168, 95770941, 57040117,
            85544426, 58460560, 66359732, 85544388, 82445249, 95367225, 86891740, 73392642,
            72982217, 85485993, 82445222, 94764182, 82445265, 95400729, 85544680, 88450295,
            88437795, 88729222, 90104951, 90092147, 90086155, 91581869, 91861675, 91860849,
            92054668, 92175473, 92124410, 92117791, 92223389, 92414443, 94338442, 94003114,
            93889100, 94234906, 94090483, 94212732, 94572593, 94434107, 94378398, 94852804,
            94543828, 95400460, 94765774, 95071512, 95071431, 95593446, 95367160, 95587543,
            95587632, 95645403, 95425764, 95477756, 95743332, 95747761, 95817930, 95927549,
            95927409, 95946039, 95945652, 96106468, 95946179, 96009454, 96107375, 96194766,
            96216964, 96276134, 96276231, 96286130, 96286164, 96286202, 96286237, 96299843,
            96296941, 96310073, 96313510, 96299754
        ]

        data = datetime.datetime.now()

        for companyNumber in companyNumbers:
            for i in range(30):
                day = data + datetime.timedelta(days=i)
                if day.weekday() != 5 and day.weekday() != 6:
                    startdate = f"{day.year:02d}-{day.month:02d}-{day.day:02d}"
                    enddate = f"{day.year:02d}-{day.month:02d}-{day.day:02d}"

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
                                    print(
                                        f"Empresa {companyNumber}, Data {startdate}: Amount = {amount}, Total = {total}")
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

    schedule.every().day.at("06:00").do(main)

    while True:
        schedule.run_pending()
        time.sleep(0.5)

if __name__ == "__main__":
    job()
