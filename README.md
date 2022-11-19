# airflow
 airflow no docker

<h3> abra o cmd na pasta </h3>
<strong> > docker-compose up ariflow-init </strong>

<hr>

<h3> espere terminar e no mesmo terminal levante o docker novamente </h3>
<strong> > docker-compose up </strong>

<hr>

## python
dentro do 

## conectando os bancos

<h3> para a conexão do postgres</h3>
<p> é bem simples </p>

|         | configurações |
|---------|---------------|
| host    | localhost     |
| port    | 5432          |
| user    | airflow       |
| pws     | airflow       |
| banco   | airflow       |

## dentro do pgAdmin ou DBeaver
<hr>
<p> dentro do Banco airflow conectado, crie um Databases com o nome "teste_contas_airflow" </p>
<p> ele será necessrio para o desenvolvimento e conectividade dos bancos 
<hr>

## configuração na UI do aiflow
<hr> 
<p> dentro da ui do airlfow > localhost:8080 </p>
<p> vá em Admin > Connections > + </p>
<hr>

|                | configurações        |
|----------------|----------------------|
| Connection ID  | postgres-airflow     |
| Connectio Type | Postgres             |
| Host           | host.docker.internal |
| Schema         | teste_contas_airflow |
| Login          | airflow              |
| Password       | aiflow               |
| Port           | 5432                 |

<strong> dentro da pasta dags/tem um exemplo de como é feita a inserção dos dados </strong> 
