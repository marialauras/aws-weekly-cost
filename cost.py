#imports
import boto3
import click
import pandas as pd
import calendar
from calendar import monthrange
from datetime import datetime,timedelta
import os.path
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

SERVICE_NAME = 'ce'

scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = Credentials.from_service_account_file('', scopes=scopes)
gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

def obter_intervalo_semana(numero_semana):
  
    data_atual = datetime.now()

    primeiro_dia_mes = datetime(data_atual.year, data_atual.month, 1)

    _, ultimos_dias_mes = calendar.monthrange(data_atual.year, data_atual.month)

    numero_semanas = (ultimos_dias_mes + primeiro_dia_mes.weekday()) // 7 + 1

    # Validar se o número da semana está dentro do intervalo válido
    if 1 <= numero_semana <= numero_semanas:
        # Calcular o primeiro dia da semana
        primeiro_dia_semana = primeiro_dia_mes + timedelta(days=(numero_semana - 1) * 7)

        # Calcular o último dia da semana
        if numero_semana < numero_semanas:
            ultimo_dia_semana = primeiro_dia_semana + timedelta(days=7)
        else:
            # Se for a última semana, ajustar o último dia para o último dia do mês
            ultimo_dia_semana = datetime(data_atual.year, data_atual.month, ultimos_dias_mes)

        # Verificar se o primeiro dia da semana é do mês anterior
        if primeiro_dia_semana.month < data_atual.month:
            primeiro_dia_semana = primeiro_dia_mes

        # Verificar se o último dia da semana é do próximo mês (para semanas não finais)
        if numero_semana < numero_semanas and ultimo_dia_semana.month > data_atual.month:
            ultimo_dia_semana = datetime(data_atual.year, data_atual.month, ultimos_dias_mes)

        # Ajustar o último dia da semana se ele for maior que o último dia do mês
        if ultimo_dia_semana.day > ultimos_dias_mes:
            ultimo_dia_semana = datetime(data_atual.year, data_atual.month, ultimos_dias_mes)

        # Imprimir os resultados (opcional, pode ser removido)
        print(primeiro_dia_semana, ultimo_dia_semana)

        return primeiro_dia_semana, ultimo_dia_semana
    else:
        return None
    
#Coletando o custo
def coletar_custo(bclient: object, inicio: str, fim: str) -> list:
    info = []

    while True:
        data = bclient.get_cost_and_usage(
            TimePeriod={
                'Start': inicio,
                'End':  fim,
            },
            Granularity='MONTHLY',
            Metrics=[
                  'BlendedCost'
            ], 
            GroupBy=[
                {
                    'Type': 'DIMENSION',
                    'Key': 'LINKED_ACCOUNT',
                },           
            ],        
        )

        info += data['ResultsByTime']
        token = data.get('NextPageToken')
        if not token:
            break
    return info 

def preencher_tabela_1(results: list, week: str, start, end, bclient):
    rows = []
    anterior = ''

    i = 0

    print(results)
    
    for result_by_time in results:
        for group in result_by_time['Groups']:
            if 'BlendedCost' in group['Metrics']:
                amount = float(group['Metrics']['BlendedCost']['Amount'])
                
            
                if amount < 0.00001:
                    continue

                rows.append({
                    'Week': 'Savings Plans',
                    'Account': group['Keys'][0],
                    'Cost': format((amount), '0.3f').replace('.', ',')        
                })
    
    return pd.DataFrame(rows) 

def preencher_tabela(results: list, week: str, start, end, bclient):
    rows = []
    anterior = ''

    i = 0

    print(results)
    
    for result_by_time in results:
        for group in result_by_time['Groups']:
            if 'BlendedCost' in group['Metrics']:
                amount = float(group['Metrics']['BlendedCost']['Amount'])
                
            
                if amount < 0.00001:
                    continue

                rows.append({
                    'Week': week + ' (' + start + " " + end + ')',
                    'Account': group['Keys'][0],
                    'Cost': format((amount), '0.3f').replace('.', ',')        
                })
    
    return pd.DataFrame(rows) 
    

def selecionar_semanas(end_date):
    current_date = datetime.now()
    return current_date > end_date

def formatar_para_string(data):
    return data.strftime("%Y-%m-%d")


def coletar_savings_plans(bclient: object, inicio: str, fim: str) -> list:
    info = []
    charges = ['Savings Plans for AWS Compute usage']

    while True:
        data = bclient.get_cost_and_usage(
            TimePeriod={
                'Start': inicio,
                'End':  '2024-03-31',
            },
            Granularity='MONTHLY',
            Metrics=[
                  'BlendedCost'
            ], 
            GroupBy=[
                {
                    'Type': 'DIMENSION',
                    'Key': 'LINKED_ACCOUNT',
                },
                {
                    'Type': 'DIMENSION',
                    'Key': 'SERVICE',
                },
            ], 
            Filter={
                'Dimensions': {
                'Key': 'SERVICE',
                'Values': charges,
                }
            }            
        )

        info += data['ResultsByTime']
        token = data.get('NextPageToken')
        if not token:
            break
        
        

    return info




@click.command()
@click.option('-P', '--profile', help='profile name')
@click.option('-S', '--start', help='start date (default: 1st date of current month)')
@click.option('-E', '--end', help='end date (default: last date of current month)')
def report(profile:str, start: str, end: str):
    df = pd.DataFrame()
    if not start or not end:
        semana = []
        i = 1
        for i in range(1,6):
            semana = obter_intervalo_semana(i)
            if(selecionar_semanas(semana[1])):
                start = formatar_para_string(semana[0])
                end = formatar_para_string(semana[1])
                
                dia_anterior = semana[1] - timedelta(days=1)
                end_inf = formatar_para_string(dia_anterior)
                
                bclient = boto3.Session(profile_name=profile).client(SERVICE_NAME)
                resultado = coletar_custo(bclient, start, end)
                indice = "Semana " + str(i)
                if(i == 1):
                    resultado = coletar_savings_plans(bclient, start, end)
                    df = preencher_tabela_1(resultado,indice,start,end_inf,bclient)
                    resultado = coletar_custo(bclient, start, end)
                    df = pd.concat([df, preencher_tabela(resultado,indice,start,end_inf,bclient)], ignore_index=True)
                    df = df.sort_values(by='Account', ascending=False)
                else:
                    df = pd.concat([df, preencher_tabela(resultado,indice,start,end_inf,bclient)], ignore_index=True)
                    df = df.sort_values(by='Account', ascending=False)
                    df.head()
                        
            else:
            
                print("oi")
                #
                pivot_df = df.pivot_table(index=['Account'], columns='Week', values='Cost', aggfunc='sum', fill_value=0).reset_index()
                # Adiciona rótulos apenas para as colunas 'Semana 1', 'Semana 2', 'Semana 3' e 'Semana 4'
                pivot_df.columns = ['Account'] + [f'{week}' for week in pivot_df.columns[1:]]
                #pivot_df[['Reservation']] = pivot_df['Account'].apply(lambda x: pd.Series(get_reservation_utilization(bclient, start, end,x)))
                

                #df_filtrado = pivot_df[pivot_df.apply(aumento, axis=1)]
                pivot_df.to_excel("excel.xlsx", index=False)

                df = pivot_df

                # Abra a planilha pelo nome ou pelo ID
                spreadsheet = gc.open_by_key('X')

                # Especifique a folha pela sua posição ou nome
                nome_da_folha = "WEEK"
                worksheet = spreadsheet.worksheet(nome_da_folha)

                values = [df.columns.values.tolist()] + df.values.tolist()

                # Limpe os dados existentes na folha antes de adicionar novos dados
                #worksheet.clear()

                # Atualize a folha com os novos dados
                worksheet.update(values)

    
            
if __name__ == '__main__':
    report()
   
