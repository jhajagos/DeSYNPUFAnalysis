__author__ = 'janos'

"""
Generate queries against DE-SYNPUF database or another claim's database were
the coding is flat for understanding relationships between first time of diagnosis.
"""

import sqlalchemy as sa
import re
import csv


def find_columns_that_match(table_columns, regex_field_match):
    columns_that_match = []
    for column in table_columns:
        column_result = regex_field_match.search(column)
        if column_result is not None:
            columns_that_match += [column]

    return columns_that_match


def get_columns_from_table(table_name, meta_data):
    table_obj = meta_data.tables[table_name]
    return table_obj.columns


def get_metadata(connection_uri):

    engine = sa.create_engine(connection_uri)
    connection = engine.connect()

    meta_data = sa.MetaData(connection, reflect=True)

    return meta_data


def generate_ccs_dx_codes_to_search(ccs_codes, csv_file_name="./ccs/cleaned_dxref_2015.csv"):

    ccs_dict_with_values = {}
    with open(csv_file_name, "rb") as f:
        csv_dict_reader = csv.DictReader(f)
        for csv_dict in csv_dict_reader:
            ccs_code = csv_dict["CCS CATEGORY"]

            ccs_code_description = csv_dict["CCS CATEGORY DESCRIPTION"]
            code = csv_dict["ICD-9-CM CODE"]

            if ccs_code in ccs_codes:
                ccs_key = (ccs_code, ccs_code_description)
                if ccs_key in ccs_dict_with_values:
                    ccs_dict_with_values[ccs_key] += [code]
                else:
                    ccs_dict_with_values[ccs_key] = [code]

    return ccs_dict_with_values


def generate_ccs_dx_codes_to_search_across_range(ccs_codes,  csv_file_name="./ccs/cleaned_dxref_2015.csv"):
    with open(csv_file_name, "rb") as f:
        code_list = []
        csv_dict_reader = csv.DictReader(f)
        for csv_dict in csv_dict_reader:
            ccs_code = csv_dict["CCS CATEGORY"]

            ccs_code_description = csv_dict["CCS CATEGORY DESCRIPTION"]
            code = csv_dict["ICD-9-CM CODE"]

            if ccs_code in ccs_codes:
                ccs_key = (ccs_code, ccs_code_description)
                code_list += [code]
        return code_list

def generate_ccs_proc_codes_to_search():
    pass


def case_statement_search_multiple_fields(search_dict, field_escape_left='"', field_escape_right='"'):

    search_field_value_list_sql = "("
    search_field_value_list = search_dict["search_values"]

    case_false_true_value = search_dict["case_false_true_value"]

    for search_value in search_field_value_list:
        if search_value.__class__ in (u"".__class__, "".__class__):
            search_value_formatted = "'%s'" % search_value
        else:
            search_value_formatted = "%s" % search_value

        search_field_value_list_sql += search_value_formatted + ", "

    search_field_value_list_sql = search_field_value_list_sql[:-2] + ")"

    fields_to_search = search_dict["fields_to_search"]

    indicator_case_sql = "CASE\n"

    for field_to_search in fields_to_search:
        indicator_case_sql += "   WHEN "
        indicator_case_sql += field_escape_left + field_to_search + field_escape_right
        indicator_case_sql += " IN " + search_field_value_list_sql
        indicator_case_sql += " THEN " + str(case_false_true_value[1]) + '\n'
    indicator_case_sql += "   ELSE " + str(case_false_true_value[0]) + '\n'
    indicator_case_sql += "END AS " + search_dict["alias_field_name"]

    return indicator_case_sql


def generate_min_max_code(table_name, fields_to_search, field_to_max, identifier_to_group_on):
    pass


def clean_field_names(columns):
    return [c.name.split(".")[0] for c in columns]


def main():
    fields_to_match = "^ICD9_DGNS_CD_"
    db_connection_uri = "mysql+pyodbc://desynpuf"
    table_name = "DE1_0_2008_to_2010_Carrier_Claims_Sample_1A"
    table_name = table_name.lower()
    id_field = "`DESYNPUF_ID`"

    date_field = '`CLM_FROM_DT`'

    alias_larger_category = "dx_cancer_range"

    meta_data = get_metadata(db_connection_uri)
    column_names = get_columns_from_table(table_name, meta_data)

    column_names = clean_field_names(column_names)

    columns_to_search = find_columns_that_match(column_names, re.compile(fields_to_match))

    ccs_dict_with_codes = generate_ccs_dx_codes_to_search([str(i) for i in [14, 17, 19, 24]])

    ccs_codes_for_larger_range = generate_ccs_dx_codes_to_search_across_range([str(i) for i in range(11,46)])

    search_dict_larger_range = {"fields_to_search": columns_to_search, "search_values": ccs_codes_for_larger_range,
                       "alias_field_name": alias_larger_category, "case_false_true_value": ["NULL", date_field]}

    search_dict_larger_sql = case_statement_search_multiple_fields(search_dict_larger_range, field_escape_left="`", field_escape_right="`") + ", \n"


    search_sql_field = id_field + ", " + date_field + ", \n"
    search_sql_field += search_dict_larger_sql + ", "
    ccs_field_name_list = []

    for ccs_key in ccs_dict_with_codes:
        ccs_field_name = "_".join(re.split("/| |-", ccs_key[1])) + "_" + ccs_key[0]
        ccs_field_name_list += [ccs_field_name]

        codes_to_search = ccs_dict_with_codes[ccs_key]

        search_dict = {"fields_to_search": columns_to_search, "search_values": codes_to_search,
                       "alias_field_name": ccs_field_name, "case_false_true_value": ["NULL", date_field]}

        search_sql_field += case_statement_search_multiple_fields(search_dict, field_escape_left="`", field_escape_right="`") + ", \n"

    search_sql_field = search_sql_field[:-3]

    inner_sql = "SELECT "
    inner_sql += search_sql_field
    inner_sql += "\nFROM " + table_name

    outer_sql_field = id_field + ", "
    outer_sql_field += "COUNT(%s) as n_records, " % id_field
    outer_sql_field += "MIN(%s) as min_%s, " % (date_field, "claim_date")
    outer_sql_field += "MAX(%s) as max_%s, " % (date_field, "claim_date")
    outer_sql_field += "COUNT(distinct %s) as %s,\n" % (date_field, "claim_date_n_distinct")

    outer_sql_field += "MIN(%s) as min_%s, " % (alias_larger_category, alias_larger_category)
    outer_sql_field += "MAX(%s) as max_%s, " % (alias_larger_category, alias_larger_category)
    outer_sql_field += "COUNT(distinct %s) as n_distinct_%s,\n" % (alias_larger_category, alias_larger_category)


    for ccs_field in ccs_field_name_list:
        outer_sql_field += "MIN(%s) as min_%s, " % (ccs_field, ccs_field)
        outer_sql_field += "MAX(%s) as max_%s, " % (ccs_field, ccs_field)
        outer_sql_field += "COUNT(distinct %s) as %s,\n" % (ccs_field, "n_distinct_" + ccs_field)

    outer_sql_field = outer_sql_field[:-2]

    outer_sql = "SELECT %s from\n (%s) t group by %s" % (outer_sql_field, inner_sql, id_field)

    print(outer_sql)

if __name__ == "__main__":
    main()
