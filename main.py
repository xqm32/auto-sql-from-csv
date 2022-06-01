import os
import re
import pandas

table_template = """
DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} ({table_schema});
"""

csv_column_name = "列名"
csv_data_type = "数据类型"

csv_is_key = "主键或外键"
csv_value_primary = "主键"
csv_value_foreign = "外键"

csv_constraint = "字段值约束"
csv_value_not_null = "不为空"
csv_value_unique = "唯一"

csv_chinese_name = "对应中文属性名"
csv_default = "默认值"


def convert(data_type):
    data_type = re.findall(r"[a-zA-Z]+", data_type)[0].upper()
    match (data_type):
        case "TEXT" | "CHAR" | "NVARCHAR" | "DATETIME":
            return "TEXT"
        case "NUMERIC":
            return "INTEGER"
        case "FLOAT":
            return "REAL"
        case _:
            return data_type


def resolve_row(row):
    data_type = " " + convert(row[csv_data_type])
    not_null = " NOT NULL" if csv_value_not_null in row[csv_constraint] else ""
    unique = " UNIQUE" if csv_value_unique in row[csv_constraint] else ""
    default_value = " DEFAULT " + row[csv_default] if row[csv_default] else ""
    reference = ""
    primary_key = ""
    autoincrement = ""

    if "编号" in row[csv_chinese_name]:
        data_type = " INTEGER"

    if csv_value_primary in row[csv_is_key]:
        # 是主键且编号
        if "编号" in row[csv_chinese_name] and csv_value_foreign not in row[csv_is_key]:
            data_type = " INTEGER"
            autoincrement = " AUTOINCREMENT"
        primary_key = f" PRIMARY KEY"

    if csv_value_foreign in row[csv_is_key]:
        refer_to = row[csv_is_key].split(":")[1]
        reference = f" REFERENCES {refer_to}({row[csv_chinese_name]})"

    return f"{row[csv_chinese_name]}{data_type}{unique}{not_null}{reference}{primary_key}{autoincrement}{default_value}"


def resolve_csv(csv_file):
    print("Resolving csv file: " + csv_file)

    table_name = csv_file.rstrip(".csv")

    schema = pandas.read_csv(csv_file, encoding="utf-8")
    schema.fillna("", inplace=True)
    if csv_default not in schema.columns:
        schema[csv_default] = ""

    table_schema = ", ".join(resolve_row(i[1]) for i in schema.iterrows())
    if "PRIMARY KEY" not in table_schema:
        print("Warning: No primary key found in table: " + table_name)
    return table_template.format(table_name=table_name, table_schema=table_schema)


files = filter(lambda i: i.endswith(".csv"), os.listdir())
with open("hrms.sql", "w", encoding="utf-8_sig") as f:
    f.write("".join(resolve_csv(i) for i in files))
