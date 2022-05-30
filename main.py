import os
import re
import pandas

table_template = """
DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} ({table_schema});
"""

column_name = "列名"
data_type = "数据类型"

is_key = "主键或外键"
primary = "主键"
foreign = "外键"

constraint = "字段值约束"
not_null = "不为空"
unique = "唯一"

name = "对应中文属性名"

primary_key = ""

def convert(row_type):
    row_type = re.findall(r"[a-zA-Z]+", row_type)[0].upper()
    match (row_type):
        case "TEXT" | "CHAR" | "NVARCHAR" | "DATETIME":
            return "TEXT"
        case "NUMERIC":
            return "INTEGER"
        case "FLOAT":
            return "REAL"
        case _:
            return row_type


def resolve(row):
    global primary_key
    row_type = " " + convert(row[data_type])
    be_null = " NOT NULL" if type(row[constraint]) is str and not_null in row[constraint] else ""
    be_unique = " UNIQUE" if type(row[constraint]) is str and unique in row[constraint] else ""
    if row[is_key] == primary:
        autoincrement = ""
        if "ID" in row[column_name].upper():
            row_type = " INTEGER"
            autoincrement = " AUTOINCREMENT"

        primary_key = f", PRIMARY KEY ({row[name]}{autoincrement})"
    return f"{row[name]}{row_type}{be_unique}{be_null}"

def resolve_csv(csv_file):
    print("Resolving csv file: " + csv_file)
    global primary_key
    primary_key = ""
    table_name = csv_file.rstrip(".csv")
    schema = pandas.read_csv(csv_file, encoding="utf-8")
    table_schema = ", ".join(resolve(i[1]) for i in schema.iterrows()) + primary_key
    return table_template.format(table_name=table_name, table_schema=table_schema)

files = filter(lambda i: i.endswith(".csv"), os.listdir())
with open("hrms.sql", "w", encoding="utf-8_sig") as f:
    f.write("".join(resolve_csv(i) for i in files))