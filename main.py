import re

INPUT_FILE_NAME = 'input/input.txt'
OUTPUT_FILE_NAME = 'output/insertAll.txt'

FILE_TYPE_DB_TYPE_MAP = {"Date": "TIMESTAMP",
                         "String": "VARCHAR",
                         "Long": "BIGINT",
                         "BigDecimal": "NUMERIC",
                         "JsonNode": "?"}


class QueryContent:
    def __init__(self, _pk_type, _other_types):
        self.pk_type = _pk_type
        self.other_types = _other_types


class TableColumnType:
    def __init__(self, _column_name, _field_name, _field_type):
        self.column_name = _column_name
        self.field_name = _field_name
        self.field_type = _field_type
        self.db_type = self.get_db_type_from_field_type(_field_type)

    def get_db_type_from_field_type(self, field_type):
        result = FILE_TYPE_DB_TYPE_MAP.get(field_type)
        if result is None:
            raise Exception("Неизвестный тип поля " + field_type)
        return result

    def print(self):
        print(self.column_name + " " + self.field_name + " " + self.field_type + " " + self.db_type + "\n")


def extract_class_from_file(filename):
    with open(filename, "r", encoding='utf-8') as f:
        return f.read()


def extract_table_name(string_value_of_class):
    pattern = re.compile(r' * This class corresponds to the database table (\w+)')
    match = pattern.search(string_value_of_class)

    if match:
        return match.group(1)
    else:
        raise Exception('Не найдено название таблицы в бд')


def extractClassContet(string_value_of_class):
    pattern = re.compile(r'\{([^}]*)\}')
    match = pattern.search(string_value_of_class)

    if match:
        return match.group(1)
    else:
        raise Exception("Не удалось извлечь содержание класса")


def extract_field_with_comment_block(class_content):
    pattern = re.compile(r"\/([\s\S]+?);")
    match = pattern.findall(class_content)
    return match


def extract_column_name(field_with_comment):
    pattern = re.compile(r'This field corresponds to the database column (\w+).(\w+)')
    match = pattern.search(field_with_comment)
    if match:
        return match.group(2)
    else:
        raise Exception('Не найдено имя поля в ' + field_with_comment)


def extract_field_type(field_with_comment):
    pattern = re.compile(r'private (\w+)')
    match = pattern.search(field_with_comment)
    if match:
        return match.group(0).split(" ")[1]
    else:
        raise Exception('Не удалось получить тип поля в ' + field_with_comment)


def extract_field_name(field_with_comment):
    pattern = re.compile(r'private (\w+) (\w+)')
    match = pattern.search(field_with_comment)
    if match:
        return match.group(2)
    else:
        raise Exception('Не удалось получить имя поля в ' + field_with_comment)


def create_table_column_types(fields_with_comments):
    result = []
    for field in fields_with_comments:
        field_name = extract_field_name(field)
        field_type = extract_field_type(field)
        column_name = extract_column_name(field)
        result.append(TableColumnType(column_name, field_name, field_type))
    return result


def get_seq_name(table_name):
    return table_name + "_seq"


def get_pk_column_name(table_name):
    split = table_name.split('dim_')
    return split[1] + "_id"


def get_pk_type(column_types, table_name):
    pk_column_name = get_pk_column_name(table_name)
    for column_type in column_types:
        if column_type.column_name == pk_column_name:
            return column_type
    raise Exception("Не найден первичный ключ в таблице " + table_name)


def get_other_types(column_types, table_name):
    pk_column_name = get_pk_column_name(table_name)
    return list(filter(lambda column_type: column_type.column_name != pk_column_name, column_types))


def create_insert_all_query(query_content, table_name):
    other_columns = query_content.other_types
    result = ('<insert id="insertAll" useGeneratedKeys="true" keyColumn="' + query_content.pk_type.column_name
              + '" keyProperty="' + query_content.pk_type.field_name + '">\n')
    result += '    <if test="entityListToSave != null and !entityListToSave.isEmpty()">\n'
    result += '        insert into ' + table_name + '\n'
    result += '        (\n'
    result += '        ' + query_content.pk_type.column_name + ',\n'
    for i in range(len(query_content.other_types)):
        if i != len(query_content.other_types) - 1:
            result += '        ' + query_content.other_types[i].column_name + ',\n'
        else:
            result += '        ' + query_content.other_types[i].column_name + '\n'
    result += '        )\n'
    result += '        values\n'
    result += '        <foreach collection="entityListToSave" item="entity" separator="), (" open="(" close=")">\n'
    result += "            nextval('" + get_seq_name(table_name) + "'),\n"
    for i in range(len(other_columns)):
        if i != len(other_columns) - 1:
            result += '            #{entity.' + other_columns[i].field_name + ',jdbcType=' + other_columns[
                i].db_type + '},\n'
        else:
            result += '            #{entity.' + other_columns[i].field_name + ',jdbcType=' + other_columns[
                i].db_type + '}\n'
    result += '        </foreach>\n'
    result += '    </if>\n'
    result += '</insert>\n'
    return result


def abbreviate_string(input_string):
    words = input_string.split("_")
    abbreviation = ""

    for word in words:
        abbreviation += word[0]

    return abbreviation


def create_update_all_query(query_content, table_name):
    other_columns = query_content.other_types
    pk_type = query_content.pk_type

    main_table_abr = abbreviate_string(table_name)
    new_values_abr = main_table_abr + "_new_values"

    result = '<update id="updateAll">\n'
    result += '    <if test="entityListToUpdate != null and !entityListToUpdate.isEmpty()">\n'
    result += '        update ' + table_name + ' ' + main_table_abr + '\n'
    result += '        <set>\n'
    for i in range(len(other_columns)):
        other_column = other_columns[i]
        if i != len(other_columns) - 1:
            result += '            ' + other_column.column_name + ' = ' + new_values_abr + '.' + other_column.field_name + ',\n'
        else:
            result += '            ' + other_column.column_name + ' = ' + new_values_abr + '.' + other_column.field_name + '\n'
    result += '        </set>\n'
    result += '        from (values\n'
    result += '        <foreach collection="entityListToUpdate" item="entity" separator=",">\n'
    result += '            (\n'
    result += '            ' + '#{entity.' + pk_type.field_name + ',jdbcType=' + pk_type.db_type + '},\n'
    for i in range(len(other_columns)):
        other_column = other_columns[i]
        if i != len(other_columns) - 1:
            result += '            ' + '#{entity.' + other_column.field_name + ',jdbcType=' + other_column.db_type + '},\n'
        else:
            result += '            ' + '#{entity.' + other_column.field_name + ',jdbcType=' + other_column.db_type + '}\n'
    result += '            )\n'
    result += '        </foreach>\n'
    result += '        ) as ' + new_values_abr + '(\n'
    result += '        ' + pk_type.field_name + ',\n'
    for i in range(len(other_columns)):
        other_column = other_columns[i]
        if i != len(other_columns) - 1:
            result += '        ' + other_column.field_name + ',\n'
        else:
            result += '        ' + other_column.field_name + '\n'
    result += '        )\n'
    result += '        where ' + main_table_abr + '.' + pk_type.column_name + ' = ' + new_values_abr + '.' + pk_type.field_name + '\n'
    result += '    </if>\n'
    result += '</update>\n'
    return result


def write_result_to_file(file_name, content):
    with open(file_name, "w", encoding='utf-8') as f:
        f.write(content)


string_value_of_class = extract_class_from_file(INPUT_FILE_NAME)
table_name = extract_table_name(string_value_of_class)
class_content = extractClassContet(string_value_of_class)

fields = extract_field_with_comment_block(class_content)
types = create_table_column_types(fields)

pk_type = get_pk_type(types, table_name)
other_columns = get_other_types(types, table_name)

queryContent = QueryContent(pk_type, other_columns)

insert_all_query = create_insert_all_query(queryContent, table_name)
update_all_query = create_update_all_query(queryContent, table_name)
write_result_to_file("output/insertAll.txt", insert_all_query)
write_result_to_file("output/updateAll.txt", update_all_query)
