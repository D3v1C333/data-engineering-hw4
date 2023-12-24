import csv
import json
import sqlite3


def parse_data():
    with open('./input/3/task_3_var_11_part_1.json', 'r', encoding="utf-8") as json_file:
        items = json.load(json_file)

    with open('./input/3/task_3_var_11_part_2.csv', 'r', encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        for row in csv_reader:
            row['duration_ms'] = int(row['duration_ms'])
            row['year'] = int(row['year'])
            row['tempo'] = float(row['tempo'])
            items.append(dict(row))
    return items



def connect_to_db():
    connection = sqlite3.connect("2.db")
    connection.row_factory = sqlite3.Row
    return connection


def insert_data(db, data):
    cursor = db.cursor()
    cursor.executemany(
        "INSERT INTO music (artist, song, duration_ms, year, tempo, genre) VALUES(:artist, :song, :duration_ms, :year, :tempo, :genre)",
        data)
    db.commit()


def get_top_by_duration(db, limit):
    cursor = db.cursor()
    result = []
    get_top_by_duratin = cursor.execute(
        "SELECT * FROM music ORDER BY duration_ms  DESC LIMIT ?", [limit])
    for row in get_top_by_duratin.fetchall():
        result.append(dict(row))
    with open('./output/3_first_filtered_result.json', 'w', encoding='utf-8') as json_file:
        json.dump(result, json_file, ensure_ascii=False, indent=2)

    cursor.close()


def get_filtered_by_year(db, min_year, limit):
    cursor = db.cursor()
    result = []
    filtered_by_year_stat = cursor.execute(
        "SELECT * FROM music WHERE year > ? ORDER BY year LIMIT ?", [min_year, limit])
    for row in filtered_by_year_stat.fetchall():
        result.append(dict(row))
    with open('./output/3_second_filtered_result.json', 'w', encoding='utf-8') as json_file:
        json.dump(result, json_file, ensure_ascii=False, indent=2)


def get_stats(db):
    cursor = db.cursor()
    result_data = {}
    genre_res = []

    freq_by_genre = cursor.execute(
        "SELECT genre, COUNT(*) as count FROM music GROUP BY genre")
    for row in freq_by_genre.fetchall():
        genre_res.append(dict(row))
    result_data['freq_by_genre'] = genre_res

    year_stat = cursor.execute(
        "SELECT SUM(year) as sum, AVG(year) as avg, MIN(year) as min, MAX(year) as max FROM music")
    result_data['year_stat'] = dict(year_stat.fetchone())

    with open('./output/3_stats.json', 'w', encoding='utf-8') as json_file:
        json.dump(result_data, json_file, ensure_ascii=False, indent=2)


items = parse_data()
db = connect_to_db()
# insert_data(db, items)
get_top_by_duration(db, 21)
get_stats(db)
get_filtered_by_year(db, 1857, 26)
