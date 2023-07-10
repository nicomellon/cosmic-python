from datetime import date

from allocation.adapters import repository
from allocation.domain import events
from allocation.service_layer import messagebus, unit_of_work


class FakeProductRepository(repository.AbstractProductRepository):
    def __init__(self, products):
        super().__init__()
        self._products = set(products)

    def _add(self, product):
        self._products.add(product)

    def _get(self, sku):
        return next((p for p in self._products if p.sku == sku), None)

    def _get_by_batchref(self, batchref):
        return next(
            (p for p in self._products for b in p.batches if b.reference == batchref),
            None,
        )


class FakeUnitOfWork(unit_of_work.AbstractUnitOfWork):
    def __init__(self):
        self.products = FakeProductRepository([])
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.rollback()

    def _commit(self):
        self.committed = True

    def rollback(self):
        pass


class TestAddBatch:
     def test_for_new_product(self):
         uow = FakeUnitOfWork()
         messagebus.handle(
             events.BatchCreated("b1", "CRUNCHY-ARMCHAIR", 100, None), uow
         )
         assert uow.products.get("CRUNCHY-ARMCHAIR") is not None
         assert uow.committed


class TestAllocate:
     def test_returns_allocation(self):
         uow = FakeUnitOfWork()
         messagebus.handle(
             events.BatchCreated("batch1", "COMPLICATED-LAMP", 100, None), uow
         )
         result = messagebus.handle(
             events.AllocationRequired("o1", "COMPLICATED-LAMP", 10), uow
         )
         assert result[0] == "batch1"


class TestChangeBatchQuantity:
    def test_changes_available_quantity(self):
        uow = FakeUnitOfWork()
        messagebus.handle(
            events.BatchCreated("batch1", "ADORABLE-SETTEE", 100, None), uow
        )
        [batch] = uow.products.get(sku="ADORABLE-SETTEE").batches
        assert batch.available_quantity == 100  #(1)

        messagebus.handle(events.BatchQuantityChanged("batch1", 50), uow)

        assert batch.available_quantity == 50  #(1)

    def test_reallocates_if_necessary(self):
        uow = FakeUnitOfWork()
        event_history = [
            events.BatchCreated("batch1", "INDIFFERENT-TABLE", 50, None),
            events.BatchCreated("batch2", "INDIFFERENT-TABLE", 50, date.today()),
            events.AllocationRequired("order1", "INDIFFERENT-TABLE", 20),
            events.AllocationRequired("order2", "INDIFFERENT-TABLE", 20),
        ]
        for e in event_history:
            messagebus.handle(e, uow)
        [batch1, batch2] = uow.products.get(sku="INDIFFERENT-TABLE").batches
        assert batch1.available_quantity == 10
        assert batch2.available_quantity == 50

        messagebus.handle(events.BatchQuantityChanged("batch1", 25), uow)

        # order1 or order2 will be deallocated, so we'll have 25 - 20
        assert batch1.available_quantity == 5  #(2)
        # and 20 will be reallocated to the next batch
        assert batch2.available_quantity == 30  #(2)