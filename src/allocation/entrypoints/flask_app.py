from datetime import datetime

from flask import Flask, request, jsonify

from allocation import bootstrap, views
from allocation.domain import commands
from allocation.service_layer import messagebus, unit_of_work
from allocation.service_layer.handlers import InvalidSku

app = Flask(__name__)
bus = bootstrap.bootstrap()


@app.route("/add_batch", methods=["POST"])
def add_batch():
    eta = request.json["eta"]
    if eta is not None:
        eta = datetime.fromisoformat(eta).date()
    cmd = commands.CreateBatch(
        request.json["ref"], request.json["sku"], request.json["qty"], eta
    )
    bus.handle(cmd)
    return "OK", 201


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    try:
        cmd = commands.Allocate(
            request.json["orderid"], request.json["sku"], request.json["qty"]
        )
        bus.handle(cmd)
    except InvalidSku as e:
        return jsonify({'message': str(e)}), 400

    return jsonify({'message': 'Allocated'}), 202


@app.route("/allocations/<orderid>", methods=["GET"])
def allocations_view_endpoint(orderid):
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    result = views.allocations(orderid, uow)
    if not result:
        return "not found", 404
    return jsonify(result), 200
