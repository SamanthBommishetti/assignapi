from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase, scoped_session
from app.core.config_loader import settings

engine = create_engine(str(settings.SQLALCHEMY_DATABASE_URI))

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

default_session  = scoped_session(SessionLocal)

class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()