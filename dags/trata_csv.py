import pandas as pd
import datetime

class trata():
    def ler_df(self):
        df = pd.read_csv('./csv/fundos_investimentos.csv', sep=';')

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
        dia = datetime.datetime.today()
        for id, dados in df.iterrows():
            vf = lambda x: str(x) if x != 'nan' else '0'
            real_monetario = lambda x: vf(x).replace('R$ ','').replace('.','').replace(',','.')
            porcent = lambda x: vf(x).replace('%','').replace('.','').replace(',','.')

            try:
                codigo_fundo = dados['codigo_fundo']
                preco_atual = dados['preco_atual']
                preco_atual = float(real_monetario(preco_atual))
                liquidez_diaria = dados['liquidez_diaria']
                liquidez_diaria = int(liquidez_diaria)
                dividendo = dados['dividendo']
                dividendo = float(real_monetario(dividendo))
                dy = dados['dy']
                dy = float(porcent(vf(dy)))
                dy_3_acumulado = dados['dy_3_acumulado']
                dy_3_acumulado = float(porcent(dy_3_acumulado))
                dy_6_acumulado = dados['dy_6_acumulado']
                dy_6_acumulado = float(porcent(dy_6_acumulado))
                dy_12_acumulado = dados['dy_12_acumulado']
                dy_12_acumulado = float(porcent(dy_12_acumulado))
                dy_3_media = dados['dy_3_media']
                dy_3_media = float(porcent(dy_3_media))
                dy_6_media = dados['dy_6_media']
                dy_6_media = float(porcent(dy_6_media))
                dy_12_media = dados['dy_12_media']
                dy_12_media = float(porcent(dy_12_media))
                dy_ano = dados['dy_ano']
                dy_ano = float(porcent(dy_ano))
                variacao_preco = dados['variacao_preco']
                variacao_preco = float(porcent(variacao_preco))
                rentabilidade_periodo = dados['rentabilidade_periodo']
                rentabilidade_periodo = float(porcent(rentabilidade_periodo))
                rentabilidade_acumulada = dados['rentabilidade_acumulada']
                rentabilidade_acumulada = float(porcent(rentabilidade_acumulada))
                vpa = dados['vpa']
                vpa = float(real_monetario(vpa))
                p_vpa = dados['p_vpa']
                p_vpa = int(vf(str(p_vpa).replace('.0','')))
                dy_patrimonial = dados['dy_patrimonial']
                dy_patrimonial = float(porcent(dy_patrimonial))
                variacao_patrimonial = dados['variacao_patrimonial']
                variacao_patrimonial = float(porcent(variacao_patrimonial))
                rentabilidade_patrimonial_periodo = dados['rentabilidade_patrimonial_periodo']
                rentabilidade_patrimonial_periodo = float(porcent(rentabilidade_patrimonial_periodo))
                rentabilidade_patrimonial_acumulada = dados['rentabilidade_patrimonial_acumulada']
                rentabilidade_patrimonial_acumulada = float(porcent(rentabilidade_patrimonial_acumulada))
                vacancia_fisica = dados['vacancia_fisica']
                vacancia_fisica = float(porcent(vacancia_fisica))
                vacancia_financeira = dados['vacancia_financeira']
                vacancia_financeira = float(porcent(vacancia_financeira))
                quantidade_ativos = dados['quantidade_ativos']
                quantidade_ativos = int(vf(quantidade_ativos))

                novo_tratamento = pd.DataFrame({'data_extracao':dia, 'codigo_fundo':codigo_fundo, 'preco_atual':preco_atual, 'liquidez_diaria':liquidez_diaria,
                        'dividendo':dividendo, 'dy':dy, 'dy_3_acumulado':dy_3_acumulado, 'dy_6_acumulado':dy_6_acumulado, 'dy_12_acumulado':dy_12_acumulado,
                        'dy_3_media':dy_3_media, 'dy_6_media': dy_6_media,'dy_12_media':dy_12_media, 'dy_ano':dy_ano, 'variacao_preco':variacao_preco,
                        'rentabilidade_periodo':rentabilidade_periodo,'rentabilidade_acumulada':rentabilidade_acumulada, 'vpa':vpa, 'p_vpa':p_vpa,
                        'dy_patrimonial':dy_patrimonial,'variacao_patrimonial':variacao_patrimonial,'rentabilidade_patrimonial_periodo':rentabilidade_patrimonial_periodo,
                        'rentabilidade_patrimonial_acumulada':rentabilidade_patrimonial_acumulada, 'vacancia_fisica':vacancia_fisica, 'vacancia_financeira':vacancia_financeira,
                        'quantidade_ativos':quantidade_ativos}, index=[id])

                novo_tratamento.to_csv('../csv/fundos_investimentos_tratado.csv', sep=';', mode='a', header=False, index=True)

            except Exception as e:
                print(e)

if __name__ == "__main__":
    tr = trata()
    tr.ler_df()