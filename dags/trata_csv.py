import pandas as pd
import datetime

class trata():
    def ler_df(self):
        df = pd.read_csv(f'../csv/fundos_investimentos.csv', sep=';')

        """
         1   codigo_fundo                         383 non-null    object 
         2   setor                                366 non-null    object 
         3   preco_atual                          382 non-null    object 
         4   liquidez_diaria                      382 non-null    float64
         5   dividendo                            383 non-null    object 
         6   dy                                   377 non-null    object 
         7   dy_3_acumulado                       377 non-null    object 
         8   dy_6_acumulado                       377 non-null    object 
         9   dy_12_acumulado                      377 non-null    object 
         10  dy_3_media                           377 non-null    object 
         11  dy_6_media                           377 non-null    object 
         12  dy_12_media                          377 non-null    object 
         13  dy_ano                               310 non-null    object 
         14  variacao_preco                       377 non-null    object 
         15  rentabilidade_periodo                377 non-null    object 
         16  rentabilidade_acumulada              377 non-null    object 
         17  patrimonio_liquido                   374 non-null    object 
         18  vpa                                  374 non-null    object 
         19  p_vpa                                373 non-null    float64
         20  dy_patrimonial                       299 non-null    object 
         21  variacao_patrimonial                 299 non-null    object 
         22  rentabilidade_patrimonial_periodo    299 non-null    object 
         23  rentabilidade_patrimonial_acumulada  299 non-null    object 
         24  vacancia_fisica                      123 non-null    object 
         25  vacancia_financeira                  30 non-null     object 
         26  quantidade_ativos                    383 non-null    int64  
        """

        for id, dados in df.iterrows():
            real_monetario = lambda x: x.replace('R$ ','').replace('.','').replace(',','.')
            porcent = lambda x:x.replace('%','').replace('.','').replace(',','.')
            try:
                codigo_fundo = dados['codigo_fundo']
                preco_atual = dados['preco_atual']
                preco_atual = float(real_monetario(preco_atual))
                liquidez_diaria = dados['liquidez_diaria']
                liquidez_diaria = int(liquidez_diaria)
                dividendo = dados['dividendo']
                dividendo = float(real_monetario(dividendo))
                dy = dados['dy']
                dy = float(porcent(dy))

                data = codigo_fundo, preco_atual, liquidez_diaria, dividendo, dy
                novo_tratamento = pd.DataFrame(data, index=id)

            except Exception as e:
                print(e)

if __name__ == "__main__":
    tr = trata()
    tr.ler_df()