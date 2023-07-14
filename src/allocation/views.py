from sqlalchemy.sql import text

from allocation.service_layer import unit_of_work


def allocations(orderid: str, uow: unit_of_work.SqlAlchemyUnitOfWork):
    with uow:
        results = uow.session.execute(
            text("SELECT sku, batchref FROM allocations_view WHERE orderid = :orderid"),
            {"orderid": orderid}
        )
    return [{"sku": sku, "batchref": batchref} for sku, batchref in results]
