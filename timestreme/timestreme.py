import sys
import traceback


"""
query and read timestreme query and reautn in dictionary format
"""


class TimeStreamRead:
    def __init__(self, boto_session):
        self.sess = boto_session
        self.client = self.sess.client("timestream-query")

    def message(self, msg, debug):
        if debug:
            print(msg)

    def run_query(self, query_string, max_items=10, debug: bool = True):

        try:
            paginator = self.client.get_paginator("query")
            page_iterator = paginator.paginate(
                QueryString=query_string,
                PaginationConfig={
                    "MaxItems": max_items,
                    #    'PageSize': 1,
                },
            )

            data: list = []
            for page in page_iterator:
                self.message(page, debug=debug)
                data.append(page)
            return data

        except Exception as err:
            print("Exception while running query:", err)
            traceback.print_exc(file=sys.stderr)

    def read_rows(self, data, filter_column=[], to_dimention=False, nullarg=None):
        def merge_dicts(l: list):
            f = {}
            for D in l:
                for key, value in D.items():
                    f[key] = value
            return f

        rows = []

        for columns in data["Rows"]:
            tmp = []
            c_info = data["ColumnInfo"]

            if len(filter_column) != 0:

                c_data = columns["Data"]
                for c in range(len(c_info)):
                    column = c_info[c]["Name"]
                    if column in filter_column:
                        # print(f"{c_info[c]['Name']}, {c_data[c]['ScalarValue']}")
                        if to_dimention == False:
                            tmp.append({c_info[c]["Name"]: c_data[c]["ScalarValue"]})
                        else:
                            tmp.append(
                                {
                                    "Name": c_info[c]["Name"],
                                    "Value": c_data[c]["ScalarValue"],
                                }
                            )

                if to_dimention == False:
                    f = merge_dicts(tmp)
                    rows.append(f)
                else:
                    rows.append(tmp)

            else:

                c_data = columns["Data"]
                for c in range(len(c_data)):
                    if "NullValue" in c_data[c].keys():
                        if to_dimention == False:
                            tmp.append({c_info[c]["Name"]: nullarg})
                        else:
                            tmp.append({"Name": c_info[c]["Name"], "Value": nullarg})
                    elif "ScalarValue" in c_data[c].keys():
                        if to_dimention == False:
                            tmp.append({c_info[c]["Name"]: c_data[c]["ScalarValue"]})
                        else:
                            tmp.append(
                                {
                                    "Name": c_info[c]["Name"],
                                    "Value": c_data[c]["ScalarValue"],
                                }
                            )

                if to_dimention == False:
                    f = merge_dicts(tmp)
                    rows.append(f)
                else:
                    rows.append(tmp)

        return rows


"""
TimeStream Write utils
"""


class TimeStreamWrite:
    def __init__(self, boto_session):
        self.sess = boto_session
        self.client = self.sess.client("timestream-write")

    def add_col(self, dimentions, columns: dict):
        f = []
        for row in dimentions:
            for name in columns:
                row[name] = columns[name]
            f.append(row)
        return f

    def to_dimention(self, d: dict):

        dim = []
        for i in d:
            dim.append({"Name": i, "Value": d[i]})
        return dim

    def nested_chunk(self, data_list: list, chunk_size: int):
        i = 0
        new_list = []
        while i < len(data_list):
            new_list.append(data_list[i : i + chunk_size])
            i += chunk_size

        return new_list

    def table(self, rows, r_time):
        records = []
        for row in rows:
            record = {
                "Time": r_time,
                "Dimensions": self.to_dimention(row),
                "MeasureName": "Machine_Tag",
                "MeasureValue": "Machine_Value",
                "MeasureValueType": "VARCHAR",
            }
            records.append(record)
        return records


"""
This class converts timestremewrite table into a dict
which can be visualised in jupyter-notebook by pandas dataframe of that dict
in order to see how data in going to write on aws timestreme db table
before pushing it over there

import pandas as pd
pd.set_option('display.max_rows', None)

t = ViewDictTable()
t_see = t.view_table(table_data)

pd.DataFrame(t_see)

"""


class ViewDictTable:
    
    def view_dimention(self, dimention):
        f = {}
        for dim in dimention:
            f[dim["Name"]] = dim["Value"]
        return f

    def view_table(self, t):

        _t = []
        for row in range(len(t)):
            t_row = t[row]
            _d = self.view_dimention(t_row["Dimensions"])
            _d["Time"] = t_row["Time"]

            MeasureName = t_row.get("MeasureName")
            MeasureValue = t_row.get("MeasureValue")
            MeasureValueType = t_row.get("MeasureValueType")

            if MeasureName:
                _d["MeasureName"] = MeasureName
            if MeasureValue:
                _d["MeasureValue"] = MeasureValue
            if MeasureValueType:
                _d["MeasureValueType"] = MeasureValueType

            _t.append(_d)

        return _t
