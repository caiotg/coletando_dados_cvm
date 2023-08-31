import pandas as pd
import requests
import os
import zipfile

def pegando_dados_cvm(caminhoDados):

    anos = range(2010, 2023)

    urlBase = f'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/'

    os.chdir(caminhoDados)

    for ano in anos:

        download = requests.get(urlBase + f'DFP/DADOS/dfp_cia_aberta_{ano}.zip')

        open(f'dfp_cia_aberta_{ano}.zip', 'wb').write(download.content)

def criando_base_dados(caminhoDados):

    os.chdir(caminhoDados)

    listaDemonstracoes = []

    for arquivo in os.listdir(os.getcwd()):

        arquivoZip = zipfile.ZipFile(arquivo)

        tipo = 'dfp'

        ano = arquivo[-8: -4]

        for planilha in arquivoZip.namelist():

            if planilha != f'{tipo}_cia_aberta_{ano}.csv':

                demonstracao = pd.read_csv(arquivoZip.open(planilha), sep= ';', decimal= ',', encoding= 'ISO-8859-1', chunksize=1000)
                demonstracao = pd.concat(demonstracao, ignore_index= True)
                demonstracao['tipo_doc'] = len(demonstracao) * [tipo]

                listaDemonstracoes.append(demonstracao)

    baseDados = pd.concat(listaDemonstracoes)
    
    baseDados[['con_ind', 'tipo_dem']] = baseDados['GRUPO_DFP'].str.split('-', expand= True)
    baseDados['tipo_dem'] = baseDados['tipo_dem'].str.strip()
    baseDados['con_ind'] = baseDados['con_ind'].str.strip()
    baseDados['con_ind'] = baseDados['con_ind'].astype(str)

    baseDados = baseDados.loc[:, ~baseDados.columns.isin(['ST_CONTA_FIXA', 'COLUNA_DF', 'GRUPO_DFP'])]
    baseDados = baseDados[baseDados['ORDEM_EXERC'] != 'PENÚLTIMO']

    os.chdir('..')

    baseDados.to_parquet('dados_empresas_cvm.parquet')

def lista_empresas(baseDados):

    return baseDados['DENOM_CIA'].unique()

def lista_demonstrativos(baseDados):

    return baseDados['tipo_dem'].unique()


if __name__ == '__main__':

    from pandas.core.base import PandasObject
    PandasObject.view = view

    caminhoDados = r'C:\Users\Caio\Documents\dev\github\coletando_dados_cvm\dados_cvm'

    # pegando_dados_cvm(caminhoDados)
    # criando_base_dados(caminhoDados)

    baseDados = pd.read_parquet('dados_empresas_cvm.parquet')
    
    listaEmpresas = lista_empresas(baseDados)
    listaDemonstrativos = lista_demonstrativos(baseDados)

    # print(* listaEmpresas)
    # print(* listaDemonstrativos)

    iraniDRE = baseDados[(baseDados['DENOM_CIA'] == 'IRANI PAPEL E EMBALAGEM S.A.') & 
                         (baseDados['tipo_dem'] == 'Demonstração do Resultado') & 
                         (baseDados['con_ind'] == 'DF Consolidado') & 
                         (baseDados['tipo_doc'] == 'dfp') & 
                         (baseDados['DS_CONTA'] == 'Resultado Bruto')]
    
    iraniDRE.view()


    