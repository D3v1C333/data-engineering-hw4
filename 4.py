import json
import sqlite3


def load_text_data(file_name):
    items = []
    with open(file_name, 'r', encoding="utf-8") as file:
        lines = file.readlines()
        item = dict()
        item['category'] = "no"
        for line in lines:
            if line == "=====\n":
                items.append(item)
                item = dict()
                item['category'] = "no"
            else:
                line = line.strip()
                splitted = line.split("::")
                if splitted[0] in ("quantity", "views"):
                    item[splitted[0]] = int(splitted[1])
                elif splitted[0] == "price":
                    item[splitted[0]] = float(splitted[1])
                elif splitted[0] == "isAvailable":
                    item[splitted[0]] = splitted[1] == "True"
                else:
                    item[splitted[0]] = splitted[1]
    return items


def connect_to_db():
    connection = sqlite3.connect("4.db")
    connection.row_factory = sqlite3.Row
    return connection


def insert_data(db, data):
    cursor = db.cursor()
    cursor.executemany(
        "INSERT INTO product (name, price, quantity, category, fromCity, isAvailable, views) VALUES(:name, :price, :quantity, :category, :fromCity, :isAvailable, :views)",
        data)
    db.commit()


def load_update_data(file_name):
    with open(file_name, 'r', encoding="utf-8") as json_file:
        return json.load(json_file)


def handle_method(cursor, name, method, param=None):
    if method == 'remove':
        cursor.execute("DELETE FROM product WHERE name = ?", [name])
    elif method == 'quantity_add':
        cursor.execute("UPDATE product SET quantity = quantity + ?, version = version + 1 WHERE name = ?",
                       [abs(param), name])
    elif method == 'quantity_sub':
        cursor.execute(
            "UPDATE product SET quantity = quantity - ?, version = version + 1 WHERE name = ? AND ((quantity - ?) > 0)",
            [abs(param), name, abs(param)])
    elif method == 'available':
        cursor.execute("UPDATE product SET isAvailable = ?, version = version + 1 WHERE name == ?",
                       [1 if param else 0, name])
    elif method == 'price_percent':
        cursor.execute("UPDATE product SET price = ROUND(price * (1 + ?), 2), version = version + 1 WHERE name = ?",
                       [param, name])
    elif method == 'price_abs':
        cursor.execute(
            "UPDATE product SET price = price + ?, version = version + 1 WHERE name = ? AND ((price + ?) > 0)",
            [param, name, param])


def handle_update(db, data):

    for update in data:
        handle_method(db, update['name'], update['method'], update['param'])
    db.commit()


def most_updatable(db):
    cursor = db.cursor()
    result = []
    get_top_by_updatable = cursor.execute(
        "SELECT * FROM product ORDER BY version  DESC LIMIT 10")
    for row in get_top_by_updatable.fetchall():
        result.append(dict(row))
    with open('./output/4_most_updatable.json', 'w', encoding='utf-8') as json_file:
        json.dump(result, json_file, ensure_ascii=False, indent=2)

    cursor.close()

def price_stats(db):
    cursor = db.cursor()
    result = []
    get_price_stats = cursor.execute("""SELECT category,
        SUM(price) as sum_price,
        MIN(price) as min_price,
        MAX(price) as max_price,
        AVG(price) as avg_price,
        SUM(quantity) as quantity
        FROM product
        GROUP BY category""")
    for row in get_price_stats.fetchall():
        result.append(dict(row))
    with open('./output/4_price_stats.json', 'w', encoding='utf-8') as json_file:
        json.dump(result, json_file, ensure_ascii=False, indent=2)

    cursor.close()

def quantity_stats(db):
    cursor = db.cursor()
    result = []
    get_quantity_stats = cursor.execute("""SELECT category,
        SUM(quantity) as sum_quantity,
        MIN(quantity) as min_quantity,
        MAX(quantity) as max_quantity,
        AVG(quantity) as avg_quantity 
        FROM product
        GROUP BY category""")
    for row in get_quantity_stats.fetchall():
        result.append(dict(row))
    with open('./output/4_quantity_stats.json', 'w', encoding='utf-8') as json_file:
        json.dump(result, json_file, ensure_ascii=False, indent=2)

    cursor.close()


def my_query(db, price, limit):
    cursor = db.cursor()
    result = []
    get_price_stats = cursor.execute("SELECT * FROM product WHERE price > ? ORDER BY price DESC LIMIT ?", [price, limit])
    for row in get_price_stats.fetchall():
        result.append(dict(row))
    with open('./output/4_my_query.json', 'w', encoding='utf-8') as json_file:
        json.dump(result, json_file, ensure_ascii=False, indent=2)

    cursor.close()


items = load_text_data('./input/4/task_4_var_11_product_data.text')
db = connect_to_db()
# insert_data(db, items)
# updatable = load_update_data('./input/4/task_4_var_11_update_data.json')
# handle_update(db, updatable)
most_updatable(db)
price_stats(db)
quantity_stats(db)
my_query(db, 100, 10)
