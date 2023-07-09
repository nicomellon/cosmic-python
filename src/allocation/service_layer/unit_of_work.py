from contextlib import AbstractContextManager
from abc import abstractmethod

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from allocation import config
from allocation.adapters import repository
from allocation.service_layer import messagebus


class AbstractUnitOfWork(AbstractContextManager):
    products: repository.AbstractProductRepository

    def commit(self):
            self._commit()
            self.publish_events()

    def publish_events(self):
        for product in self.products.seen:
            while product.events:
                event = product.events.pop(0)
                messagebus.handle(event)

    @abstractmethod
    def _commit(self):
        raise NotImplementedError

    @abstractmethod
    def rollback(self):
        raise NotImplementedError


DEFAULT_SESSION_FACTORY = sessionmaker(
    bind=create_engine(
        config.get_postgres_uri(),
        isolation_level="REPEATABLE READ",
    )
)


class SqlAlchemyUnitOfWork(AbstractUnitOfWork):
    def __init__(self, session_factory=DEFAULT_SESSION_FACTORY):
        self.session_factory = session_factory

    def __enter__(self):
        self.session = self.session_factory()
        self.products = repository.SqlAlchemyRepository(self.session)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.rollback()
        self.session.close()

    def rollback(self):
        self.session.rollback()

    def _commit(self):  #(1)
        self.session.commit()
