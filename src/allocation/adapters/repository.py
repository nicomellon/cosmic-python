import abc

from allocation.domain import model


class AbstractProductRepository(abc.ABC):

    def __init__(self):
        self.seen: set[model.Product] = set()

    def add(self, product: model.Product):
        self._add(product)
        self.seen.add(product)

    def get(self, sku) -> model.Product:
        product = self._get(sku)
        if product:
            self.seen.add(product)
        return product

    @abc.abstractmethod
    def _add(self, product: model.Product):
        raise NotImplementedError

    @abc.abstractmethod
    def _get(self, sku) -> model.Product:
        raise NotImplementedError


class SqlAlchemyRepository(AbstractProductRepository):

    def __init__(self, session) -> None:
        super().__init__()
        self.session = session

    def _add(self, product: model.Product) -> None:
        self.session.add(product)

    def _get(self, sku) -> model.Product:
        return (
            self.session.query(model.Product)
            .filter_by(sku=sku)
            .with_for_update()
            .first()
        )
