from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

# SQLALCHEMY_DATABASE_URL = 'sqlite:///./blog.db'

# engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={
#                        "check_same_thread": False})

engine = create_engine('postgresql+psycopg2://fmacadmin:fmac*2025@localhost:5432/fmac_db')

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

default_session  = scoped_session(SessionLocal)

Base = declarative_base()

def get_db():
    db = default_session()
    try:
        yield db
    finally:
        db.close()