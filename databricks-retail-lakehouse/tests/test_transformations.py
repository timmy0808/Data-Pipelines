from dataclasses import dataclass


VALID_ORDER_STATUSES = {"PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"}


@dataclass
class Order:
    order_id: str | None
    customer_id: str | None
    status: str
    order_total: float


def is_valid_order(order: Order) -> bool:
    return (
        order.order_id is not None
        and order.customer_id is not None
        and order.status.upper() in VALID_ORDER_STATUSES
        and order.order_total >= 0
    )


def calculate_line_total(quantity: int, unit_price: float) -> float:
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    if unit_price < 0:
        raise ValueError("unit_price cannot be negative")
    return round(quantity * unit_price, 2)


def test_valid_order():
    order = Order("O1001", "C001", "shipped", 164.49)
    assert is_valid_order(order)


def test_invalid_negative_order():
    order = Order("O1002", "C001", "shipped", -1.00)
    assert not is_valid_order(order)


def test_invalid_status():
    order = Order("O1003", "C001", "unknown", 20.00)
    assert not is_valid_order(order)


def test_line_total():
    assert calculate_line_total(3, 12.25) == 36.75
