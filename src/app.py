import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# load the .env file variables
load_dotenv()

# 1) Connect to the database here using the SQLAlchemy's create_engine function

# URL de conexion
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# creo un engine de SQLAlchemy
engine = create_engine(DATABASE_URL)

# creo la sesion para interactuar con la base de datos
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base() #declaro 'Base'

# Test de conexion
try:
    connection = engine.connect() #me conecto y consulto algo simple
    print("Conexión exitosa a la base de datos")
except SQLAlchemyError as e:
    print(f"Error al conectar a la base de datos: {e}")
finally:
    connection.close()  # cierro la conexión



# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function

# Crear la tabla
test_create_table_query = text("""
CREATE TABLE IF NOT EXISTS publishers(
    publisher_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY(publisher_id)
);
""")

with engine.connect() as connection:
    connection.execute(test_create_table_query)
    connection.commit()  # hago el commit para confirmar
    
create_authors_tables = text("""
CREATE TABLE IF NOT EXISTS authors(
    author_id INT NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    middle_name VARCHAR(50) NULL,
    last_name VARCHAR(100) NULL,
    PRIMARY KEY(author_id)
);
""")
with engine.connect() as connection:
    connection.execute(create_authors_tables)
    connection.commit()  # hago el commit para confirmar

create_books_table = text("""
CREATE TABLE IF NOT EXISTS books(
    book_id INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    total_pages INT NULL,
    rating DECIMAL(4, 2) NULL,
    isbn VARCHAR(13) NULL,
    published_date DATE,
    publisher_id INT NULL,
    PRIMARY KEY(book_id),
    CONSTRAINT fk_publisher FOREIGN KEY(publisher_id) REFERENCES publishers(publisher_id)
);
""")
create_book_authors_table = text("""
CREATE TABLE IF NOT EXISTS book_authors (
    book_id INT NOT NULL,
    author_id INT NOT NULL,
    PRIMARY KEY(book_id, author_id),
    CONSTRAINT fk_book FOREIGN KEY(book_id) REFERENCES books(book_id) ON DELETE CASCADE,
    CONSTRAINT fk_author FOREIGN KEY(author_id) REFERENCES authors(author_id) ON DELETE CASCADE
);
""")

with engine.connect() as connection:
    connection.execute(create_books_table)
    connection.execute(create_book_authors_table)
    connection.commit()  # hago el commit para confirmar



# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function

# otra forma que encontre para usar los codigos dentro de los archivos
sql_directory = 'src\sql'
file_to_use = 'src/sql/insert.sql' 
with open(file_to_use, 'r') as sql_file:
    sql_commands = sql_file.read()
    with engine.connect() as connection:
        transaction = connection.begin()  # Comienza ala transacción
        try: 
            for command in sql_commands.split(';'):
                command = command.strip()
                if command:
                    connection.execute(text(command))
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            print(f"Error: {e}")
        connection.commit()  # Hacer el commit para confirmar las ejecuciones





# 4) Use pandas to print one of the tables as dataframes using read_sql function

#consultas simples de tablas
table_of_query = 'books'
query = text(f'SELECT * FROM {table_of_query};')

with engine.connect() as connection:
    df_result = pd.read_sql(query,connection)
    if df_result.empty:
        print("No se encontraron resultados")
    else :
        print(df_result)