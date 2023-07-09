import abc

from allocation.domain import model


class AbstractProductRepository(abc.ABC):

    @abc.abstractmethod
    def add(self, product: model.Product) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, sku) -> model.Product:
        raise NotImplementedError


class SqlAlchemyRepository(AbstractProductRepository):

    def __init__(self, session) -> None:
        self.session = session

    def add(self, product: model.Product) -> None:
        self.session.add(product)

    def get(self, sku) -> model.Product:
        return (
            self.session.query(model.Product)
            .filter_by(sku=sku)
            .with_for_update()
            .first()
        )
