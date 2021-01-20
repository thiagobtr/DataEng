# Libs
import psycopg2
from zipfile import ZipFile
import os
from configparser import ConfigParser

# Funcao para leitura do arquivo "database.ini", com os parametros de conexao
def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

# Funcao para criacao das tabelas

def create_tables():
    commands=(
    '''
    CREATE TABLE IF NOT EXISTS BUILDINGS (
    id int PRIMARY key,
    address VARCHAR,
    address_number varchar(80),
    neighborhood VARCHAR ,
    city VARCHAR(60),
    state VARCHAR(60),
    cep varchar(10),
    latitude NUMERIC,
    longitude NUMERIC
    )
    ''',
    '''
    CREATE TABLE IF NOT EXISTS ADS
    (id int PRIMARY key,
    bedrooms_min numeric NULL,
    bathrooms_min numeric NULL,
    built_area_min numeric NULL,
    city_name VARCHAR(100)NULL,
    idno VARCHAR(20) ,
    lat numeric NULL,
    lon numeric NULL,
    neighborhood VARCHAR NULL,
    parking_space_min numeric NULL,
    property_type VARCHAR(50)NULL,
    sale_price numeric NULL,
    state VARCHAR(20)NULL,
    street VARCHAR NULL,
    street_number varchar(10) NULL,
    id_building int references BUILDINGS (id)
    )

    ''',
    '''
    CREATE TABLE IF NOT EXISTS AD_BUILDINGS(
    id int GENERATED ALWAYS AS identity PRIMARY KEY,
    id_ad int REFERENCES ADS(id),
    id_build int REFERENCES BUILDINGS(id)
    )
    ''')
    try:
        # Chama funcao Config para leitura dos  parametros
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
# https://www.postgresqltutorial.com/postgresql-python/create-tables/

# Funcao para extracao arquivo de zip
def extrair_arquivo (path_ori,path_dest):
    with ZipFile(path_ori, 'r') as zipObj:
    # extraindo conteudo para o diretorio 
        zipObj.extractall(path_dest)
        path_extraido = zipObj.namelist() 
        zipObj.close()
        extracted_file = os.path.join(path_dest, path_extraido[0])
    # retorna caminho do arquivo extraido
    return extracted_file