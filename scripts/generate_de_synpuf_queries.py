__author__ = 'janos'

import json

test_json = """
{"fields_to_search": ["dx1", "dx2", "dx3", "dx4"],
 "search_values": ["250", "2501", "2502"],
 "alias_field_name": "is_simple_diabetes",
 "case_false_true_value": [0, 1]
}
"""


def case_statement_search_multiple_fields(search_dict, field_escape_left='"', field_escape_right='"'):

    search_field_value_list_sql = "("
    search_field_value_list = search_dict["search_values"]

    case_false_true_value = search_dict["case_false_true_value"]

    for search_value in search_field_value_list:
        if search_value.__class__ == u"".__class__:
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

    print(indicator_case_sql)

def generate_dx_codes():
    pass


if __name__ == "__main__":
    search_dict = json.loads(test_json)
    case_statement_search_multiple_fields(search_dict)