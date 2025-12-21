from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

class Database:
    def __init__(self, db_url: str, echo: bool = False):
        self.engine = create_engine(
            db_url, 
            echo=echo,
            pool_size=40,
            max_overflow=40,
            pool_recycle=1800,
            pool_pre_ping=True
        )
        
        self._session_factory = sessionmaker(
            autocommit=False, 
            autoflush=False, 
            bind=self.engine
        )

    @contextmanager
    def session(self):
        """
        Provide a Context Manager.
        Ensuring close Session and rollback.
        """
        session: Session = self._session_factory()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_tables(self, base):
        base.metadata.create_all(self.engine)