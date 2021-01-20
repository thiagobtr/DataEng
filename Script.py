# Carregando as libs necessarias
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import glob, os
# Importando as funcoes do arquivo functions.py
import functions 

# Define os locais de origem e destino dos arquivos
arq_ori=['./DataEngTest-main/raw_data/ads.zip','./DataEngTest-main/raw_data/buildings.zip']
arq_dest='./DataEngTest-main/raw_data/'
pos =0
loc_arquivos=[]

# Executa rotina para extrair os arquivos zipados 
for file in arq_ori:
    # chama funcao para extração dos arquivos
    path_extraido = functions.extrair_arquivo(file,arq_dest)
    pos+=1
    loc_arquivos.append(path_extraido)
    print(path_extraido)


# Define diretorio dos arquivos json 
    # path='./DataEngTest-main/raw_data/ads_json/ads'
# seleciona local dos arquivos json
loc_json= loc_arquivos[0]

path_json = os.path.join(loc_json,'*.json')
# faz a leitura dos diretorios
files= glob.glob(path_json)

# Rotina para fazer a leitura dos diretorios dos arquivos json e em seguida 
# grava todos os arquivos no dataframe df_ads

# define lista
ads=[]

for f in files:
    df_json =pd.read_json(f,lines=True, dtype={'street_number':str})
   # adiciona DataFrame de cada arquivo a lista "ads"
    ads.append(df_json)    
# Cria DataFrame
df_ads= pd.concat(ads, ignore_index=True)


# leitura arquivo buildings
# local do arquivo csv
loc_csv= loc_arquivos[1]

df_buildings = pd.read_csv(loc_csv+'buildings.csv',
                           dtype={'street_number':str},na_values="None",sep=',')

# Executa funcao para criação das tabelas
functions.create_tables()

# Vamos padronizar os registros "state" para sigla dos estados 
# Como exemplo, vamos atualizar somente a sigla "SP"
df_buildings.loc[(df_buildings['state']=='São Paulo','state')]='SP'

# Retirando os espaços adicionais
df_buildings['city']= df_buildings['city'].str.rstrip()
df_buildings['city']= df_buildings['city'].str.lstrip()

# atualliza o nome da cidade para "São Paulo"
df_buildings.loc[(df_buildings['city']=='Sao Paulo','city')]='São Paulo'

# chama a função para receber os parametros de conexao
params = functions.config()
# define os parametros do banco
string_conn = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
    user=params['user'], passwd=params['password'], host=params['host']
    , port=5432, db=params['database'])

alchemyEngine = create_engine(string_conn, pool_size = 50)

# Realiza a conexao
dbConn = alchemyEngine.connect();

# Carga das tabelas no BD
# com o comando "to_sql", 
# tabela "ads" -> recebe dados de "dfs_ads" 
df_ads.to_sql('ads',con=dbConn,if_exists='append',index_label='id')

# tabela "BUILDINGS" -> recebe dados de df_buildings
df_buildings.to_sql('buildings',con=dbConn,if_exists='append',index=False)

# faz a conexao 
conn = psycopg2.connect(**params)
cur = conn.cursor()
# Queries
# 1ª query -> atualiza o id_building na tabela ADs
# 2ª query -> atualiza o od_building restante
# 3ª query -> insere os registros com mais de um registro buildings encontrado na tabela ads_building
queries = ('''
update ADS set id_building = b.id 
from "buildings" b
where ads.street = b.address  
	and ads.street_number = b.address_number
    and ads.city_name = b.city
    and ads.state = b.state
    and ads.neighborhood =b.neighborhood 
''',
'''
-- atualizando utilizandos as inf de endereço sem "neighborhood"
-- para esses casos, como podemos encontrar mais de um registro de building para cada ad
-- vamos selecionar somente os registros com 1 building para 1 ad  

with cte as (select ads.id,count(b.id)as qtd
from ads , "buildings" b
where ads.street = b.address  
	and ads.street_number = b.address_number
    and ads.city_name = b.city
    and ads.state = b.state
    and ads.id_building is null -- Exclui os registros já encontrados
group by ads.id
having(count(b.id))= 1 --somente 1 registro por ad
order by ads.id)

-- Update
update ads set id_building= b.id
from buildings b
where ads.street = b.address  
	and ads.street_number = b.address_number
    and ads.city_name = b.city
    and ads.state = b.state
-- selecionando os casos retornados pelo CTE
and ads.id in (select id from cte)

''',
'''
-- Insere os registros com mais de 1 predio para um unico registro de ad na tabela ads_buildings
INSERT INTO ad_buildings (id_ad,id_build)
select a.id as id_ad, b.id as id_build
from ADS  a
inner join buildings b on a.street = b.address  
        and a.street_number = b.address_number
        and a.city_name = b.city
        and a.state = b.state                      
where id_building is null
'''        )

for query in queries:
    
    # Executa a query
    cur.execute(query)

# fecha conexao
cur.close()
# commit the changes
conn.commit()
if conn is not None:
    conn.close()
