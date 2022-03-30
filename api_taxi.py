from contextlib import contextmanager
from typing import Any, Callable
from flask import Flask, request, Response
from jsonschema.exceptions import ValidationError
from jsonschema.validators import validate
from sqlalchemy import Boolean, Column, DateTime, Integer, ForeignKey, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, scoped_session
from sqlalchemy.sql import func

app = Flask(__name__)

# region БД
"""БД по описанию из onyx_taxi.dbml."""

engine = create_engine("postgresql://postgres:postgres@localhost/taxi")
Base = declarative_base()


class Client(Base):
    """Таблица БД с пассажирами сервиса."""
    __tablename__ = 'clients'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True, nullable=False)
    is_vip = Column(Boolean, nullable=False)

    def __repr__(self):
        return f'<Client {self.name}, is_vip = {self.is_vip} with id={self.id}>'


class Driver(Base):
    """Таблица БД с водителями сервиса."""
    __tablename__ = 'drivers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(125), unique=True, nullable=False)
    car = Column(String(25), nullable=False)

    def __repr__(self):
        return f'<Driver {self.name}, car ={self.car} with id={self.id}>'


class Order(Base):
    """Таблица БД с заказами сервиса."""
    __tablename__ = 'orders'

    id = Column(Integer, primary_key=True, autoincrement=True)
    address_from = Column(String(50), nullable=False)
    address_to = Column(String(50), nullable=False)
    client_id = Column(Integer, ForeignKey(Client.id))
    driver_id = Column(Integer, ForeignKey(Driver.id))
    date_created = Column(DateTime(), default=func.now(), nullable=False)
    status = Column(String(25), default="not_accepted", nullable=False)
    client = relationship("Client")
    driver = relationship("Driver")

    def __repr__(self):
        return f'<Order id = {self.id}' \
               f' customer id = {self.client_id},' \
               f' driver id =  {self.driver_id}, ' \
               f' address {self.address_from} - {self.address_to} ' \
               f' date {self.date_created},' \
               f' status \"{self.status}\" >'


Base.metadata.create_all(engine)
# endregion

# region Валидация данных
drivers_post_schema = {
    "type": "object",
    "required": ["name", "car"],
    "additionalProperties": False,
    "properties": {
        "name": {"type": "string", "minLength": 2},
        "car": {"type": "string", "minLength": 2},
    },
}

clients_post_schema = {
    "type": "object",
    "required": ["name", "is_vip"],
    "additionalProperties": False,
    "properties": {
        "name": {"type": "string", "minLength": 1},
        "is_vip": {"type": "boolean"},
    },
}

orders_post_schema = {
    "type": "object",
    "required": ["client_id", "driver_id", "date_created", "status", "address_from", "address_to"],
    "additionalProperties": False,
    "properties": {
        "client_id": {"type": "integer"},
        "driver_id": {"type": "integer"},
        "date_created": {"type": "string", "format": "date-time"},
        "status": {
            "type": "string",
            "enum": ["not_accepted", "in_progress", "done", "cancelled"],
        },
        "address_from": {"type": "string", "minLength": 1},
        "address_to": {"type": "string", "minLength": 1},
    },
}


def validate_json(schema_name: str) -> Callable:
    """Декоратор для валидации json из тела HTTP-запроса."""

    def decorator(function: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                validate(request.get_json(), schema_name)
            except ValidationError:
                print("Ошибка валидации данных")
                return Response("Bad Request", status=400)
            return function(*args, **kwargs)
        return wrapper
    return decorator

# endregion


Session = scoped_session(sessionmaker(expire_on_commit=False, bind=engine))


@contextmanager
def session_scope():
    session = Session()
    try:
        yield session
        session.commit()
    except :
        session.rollback()
        raise
    finally:
        session.close()

# region Пассажиры


@app.route("/clients/<int:client_id>", methods=["GET"])
def get_client(client_id):
    with session_scope() as s:
        cli = s.query(Client).get(client_id)
    return cli.__repr__()


@validate_json(clients_post_schema)
@app.route("/clients", methods=["POST"])
def create_client():
    content = request.get_json()
    with session_scope() as s:
        new_cli = Client(name=content["name"], is_vip=content["is_vip"])
        s.add(new_cli)
        cli_info = s.query(Client).order_by(Client.id.desc()).first()
        return cli_info.__repr__()


@app.route("/clients/<int:client_id>", methods=["DELETE"])
def delete_client(client_id):
    with session_scope() as s:
        cli = s.query(Client).get(client_id)
        if cli is not None:
            s.delete(cli)
            return f"Пассажир {client_id} удален"
        else:
            return Response("Пассажир не найден", 404)

# endregion

# region Водители


@app.route("/drivers/<int:driver_id>", methods=["GET"])
def get_driver(driver_id):
    with session_scope() as s:
        dr = s.query(Driver).get(driver_id)
        return dr.__repr__()


@validate_json(drivers_post_schema)
@app.route("/drivers", methods=["POST"])
def create_driver():
    content = request.get_json()
    with session_scope() as s:
        new_dr = Driver(name=content["name"], car=content["car"])
        s.add(new_dr)
        dr_info = s.query(Driver).order_by(Driver.id.desc()).first()
        return dr_info.__repr__()


@app.route("/drivers/<int:driver_id>", methods=["DELETE"])
def delete_driver(driver_id):
    with session_scope() as s:
        dr = s.query(Driver).get(driver_id)
        if dr is not None:
            s.delete(dr)
            return f"Водитель {driver_id} удален"
        else:
            return Response("Водитель не найден", 404)

# endregion

# region Заказы


@app.route("/orders/<int:order_id>", methods=["GET"])
def get_order(order_id):
    with session_scope() as s:
        ord = s.query(Order).get(order_id)
        return ord.__repr__()


@validate_json(orders_post_schema)
@app.route("/orders", methods=["POST"])
def create_order():
    content = request.get_json()
    with session_scope() as s:
        new_ord = Order(
            client_id=content["client_id"],
            driver_id=content["driver_id"],
            date_created=content["date_created"],
            status=content["status"],
            address_from=content["address_from"],
            address_to=content["address_to"]
        )
        s.add(new_ord)
        ord_info = s.query(Order).order_by(Order.id.desc()).first()
        return ord_info.__repr__()


@validate_json(orders_post_schema)
@app.route("/orders/<int:order_id>", methods=["PUT"])
def change_order(order_id):
    with session_scope() as s:
        content = request.get_json()
        ord = s.query(Order).get(order_id)
        if ord is not None:
            if ord.status == "done" or ord.status == "cancelled":
                return Response("Нельзя изменить завершенный заказ", status=400)
            elif ord.status == "in_progress" and ord.date_created != content["date_created"] \
                    and ord.client_id != content["client_id"] and ord.driver_id != content["driver_id"]:
                return Response("Заказ в обработке нельзя изменить", status=400)
            elif ord.status in status_chain:
                if content["status"] in status_chain[ord.status]:
                    ord.client_id = content["client_id"],
                    ord.driver_id = content["driver_id"],
                    ord.date_created = content["date_created"],
                    ord.status = content["status"],
                    ord.address_from = content["address_from"],
                    ord.address_to = content["address_to"]
                else:
                    return Response("Нельзя изменить статус заказа", status=400)
        else:
            return Response("Object not found", 404)
        return ord.__repr__()


status_chain = {"not_accepted": ["in_progress", "cancelled"],
                "in_progress": ["cancelled", "done"]}

# endregion


if __name__ == '__main__':
    app.run(debug=True)
