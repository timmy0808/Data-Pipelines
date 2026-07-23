# Data Dictionary

## customers

| Column | Type | Description |
|---|---|---|
| customer_id | string | Natural customer key |
| first_name | string | Customer first name |
| last_name | string | Customer last name |
| email | string | Customer email |
| city | string | Current city |
| state | string | Two-character state |
| region | string | US sales region |
| created_at | timestamp | Customer creation time |
| updated_at | timestamp | Source modification time |

## products

| Column | Type | Description |
|---|---|---|
| product_id | string | Natural product key |
| product_name | string | Product display name |
| category | string | Product category |
| unit_price | double | Current list price |
| active | boolean | Whether product is active |
| updated_at | timestamp | Source modification time |

## orders

| Column | Type | Description |
|---|---|---|
| order_id | string | Natural order key |
| customer_id | string | Customer reference |
| order_timestamp | timestamp | Order event time |
| status | string | Processing state |
| payment_method | string | Payment channel |
| order_total | double | Total order value |

## order_items

| Column | Type | Description |
|---|---|---|
| order_item_id | string | Natural line-item key |
| order_id | string | Order reference |
| product_id | string | Product reference |
| quantity | integer | Units purchased |
| unit_price | double | Sale price per unit |
| line_total | double | Extended line value |
