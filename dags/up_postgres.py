import datetime
import pandas as pd
import psycopg2

class up_postgres():
    def conecta(self):
        """
        Configuraçãoes do docker
        """
        host = 'host.docker.internal'
        user = 'airflow'
        password = 'airflow'
        database = 'teste_contas_airflow'
        port = '5432'

        try:
            conn_str = f'host={host} user={user} dbname={database} password={password} port={port}'
            conexao = psycopg2.connect(conn_str)
            print("conectado")
            cursor = conexao.cursor()
            return cursor, conexao
        except:
            print("falha ao conectar")

    def verificador(self, cursor, data_extracao):
        cursor.execute(f"SELECT data_extracao FROM fundo_fii.fundos_invetimentos WHERE data_extracao='{data_extracao}';")
        return cursor.fetchone()

    def insere_banco(self):
        dados = pd.read_csv('../csv/fundos_investimentos_tratado.csv', sep=';', header=0, error_bad_lines=False)
        cursor, conexao = self.conecta()
        data_verifica = datetime.date.today()
        data_if = str(data_verifica)
        data_extracao = data_verifica
        verificador = self.verificador(cursor, data_extracao)
        if verificador != None:
            verificador = str(verificador[0])
        else:
            verificador = verificador
        if verificador != data_if:
            for id, dados in dados.iterrows():
                codigo_fundo = dados['codigo_fundo']
                setor = dados['setor']
                preco_atual = dados['preco_atual']
                liquidez_diaria = dados['liquidez_diaria']
                dividendo = dados['dividendo']
                dy = dados['dy']
                dy_3_acumulado = dados['dy_3_acumulado']
                dy_6_acumulado = dados['dy_6_acumulado']
                dy_12_acumulado = dados['dy_12_acumulado']
                dy_3_media = dados['dy_3_media']
                dy_6_media = dados['dy_6_media']
                dy_12_media = dados['dy_12_media']
                dy_ano = dados['dy_ano']
                variacao_preco = dados['variacao_preco']
                rentabilidade_periodo = dados['rentabilidade_periodo']
                rentabilidade_acumulada = dados['rentabilidade_acumulada']
                patrimonio_liquido = dados['patrimonio_liquido']
                vpa = dados['vpa']
                p_vpa = dados['p_vpa']
                dy_patrimonial = dados['dy_patrimonial']
                variacao_patrimonial = dados['variacao_patrimonial']
                rentabilidade_patrimonial_periodo = dados['rentabilidade_patrimonial_periodo']
                rentabilidade_patrimonial_acumulada = dados['rentabilidade_patrimonial_acumulada']
                vacancia_fisica = dados['vacancia_fisica']
                vacancia_financeira = dados['vacancia_financeira']
                quantidade_ativos = dados['quantidade_ativos']
                cursor.execute("INSERT INTO fundo_fii.fundos_invetimentos (data_extracao, codigo_fundo, setor, preco_atual, liquidez_diaria, dividendo, dy, dy_3_acumulado, dy_6_acumulado, dy_12_acumulado, dy_3_media, dy_6_media, dy_12_media, dy_ano, variacao_preco, rentabilidade_periodo, rentabilidade_acumulada, patrimonio_liquido, vpa, p_vpa, dy_patrimonial, variacao_patrimonial, rentabilidade_patrimonial_periodo, rentabilidade_patrimonial_acumulada, vacancia_fisica, vacancia_financeira, quantidade_ativos) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(
                    data_extracao,codigo_fundo, setor, preco_atual, liquidez_diaria, dividendo, dy, dy_3_acumulado, dy_6_acumulado,
                    dy_12_acumulado, dy_3_media, dy_6_media, dy_12_media, dy_ano, variacao_preco, rentabilidade_periodo,
                    rentabilidade_acumulada, patrimonio_liquido, vpa, p_vpa, dy_patrimonial, variacao_patrimonial,
                    rentabilidade_patrimonial_periodo, rentabilidade_patrimonial_acumulada, vacancia_fisica,
                    vacancia_financeira, quantidade_ativos
                ))
                conexao.commit()
            cursor.close()
        else:
            print("dentro dos 7 dias")

if __name__ == "__main__":
    up = up_postgres()
    up.insere_banco()