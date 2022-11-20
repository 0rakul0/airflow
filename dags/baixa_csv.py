import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np
import datetime


class cluster():

    def dataframe(self):
        url_1 = f"https://www.fundsexplorer.com.br/ranking"
        headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/ 97.0.4692.99 Safari/537.36"}
        site1 = requests.get(url_1, headers=headers)
        soup_fundo1 = bs(site1.content, 'html.parser')

        data = self.extrat(soup_fundo1)
        self.tratamento(data)

    def extrat(self, soup):
        # procura a tabela
        tabela = soup.find('table', {'id':'table-ranking'})
        # joga a tabela para uma string
        tabela_str = str(tabela)
        # transforma a tabela em um dataframe
        df = pd.read_html(tabela_str)[0]
        
        return df

    def tratamento(self, dados):
        #tratando valores vazios

        dados.rename(columns={'Códigodo fundo':'codigo_fundo','Setor':'setor','Preço Atual':'preco_atual','Liquidez Diária':'liquidez_diaria','Dividendo':'dividendo','DividendYield':'dy','DY (3M)Acumulado':'dy_3_acumulado',
                              'DY (6M)Acumulado':'dy_6_acumulado','DY (12M)Acumulado':'dy_12_acumulado','DY (3M)Média':'dy_3_media','DY (6M)Média':'dy_6_media','DY (12M)Média':'dy_12_media',
                              'DY Ano':'dy_ano','Variação Preço':'variacao_preco','Rentab.Período':'rentabilidade_periodo','Rentab.Acumulada':'rentabilidade_acumulada','PatrimônioLíq.':'patrimonio_liquido',
                              'VPA':'vpa','P/VPA':'p_vpa','VariaçãoPatrimonial':'variacao_patrimonial','DYPatrimonial':'dy_patrimonial', 'Rentab. Patr.no Período':'rentabilidade_patrimonial_periodo',
                              'Rentab. Patr.Acumulada':'rentabilidade_patrimonial_acumulada','VacânciaFísica':'vacancia_fisica','VacânciaFinanceira':'vacancia_financeira','QuantidadeAtivos':'quantidade_ativos'}
                     , inplace=True)
        #pega os dados para tratar
        dados_tratados = dados

        dados_tratados.to_csv('./csv/fundos_investimentos.csv', encoding='utf8', sep=';')

if __name__=="__main__":
    cl = cluster()
    cl.dataframe()