import sys
import traceback


class TimeStreamRead:
    """
    query and read timestreme query and reautn in dictionary format
    """

    def __init__(self, boto_session):
        self.sess = boto_session
        self.client = self.sess.client("timestream-query")

    def message(self, msg, debug:bool):
        if debug:
            print(msg)
        return

    def run_query(self, query_string: str, max_items: int=10, debug: bool = True, filter: bool=False, **kwargs):

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
                if filter == True:
                    row = self.read_rows(page, **kwargs)
                    if len(row) != 0:
                        data.append(row)
                else:
                    data.append(page)
            return data

        except Exception as err:
            print("Exception while running query:", err)
            traceback.print_exc(file=sys.stderr)

    def _merge_dicts(self, l: list):
        f = {}
        for D in l:
            for key, value in D.items():
                f[key] = value
        return f

    def read_rows(self, data:dict, filter_column:list=[], to_dimention: bool=False, nullarg=None):

        rows = []

        for columns in data["Rows"]:
            tmp = []
            c_info = data["ColumnInfo"]
            c_data = columns["Data"]

            if len(filter_column) != 0:

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

            else:
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
                f = self._merge_dicts(tmp)
                rows.append(f)
            else:
                rows.append(tmp)
        
        return rows

    def add_col(self, table, columns: dict):
        """
        add extra column to
        """

        f = []
        for row in table:
            for name in columns:
                row[name] = columns[name]
            f.append(row)
        return f

    def to_dimention(self, d: dict):
        """
        Dimention contains data of
        each row in timestreme database
        """

        dim = []
        for i in d:
            dim.append({"Name": i, "Value": d[i]})
        return dim

    def to_table(self, rows, epoc_time):
        """
        Table contains records or each dimention/rows
        """

        records = []
        for row in rows:
            record = {
                "Time": epoc_time,
                "Dimensions": self.to_dimention(row),
                "MeasureName": "Machine_Tag",
                "MeasureValue": "Machine_Value",
                "MeasureValueType": "VARCHAR",
            }
            records.append(record)
        return records


class TimeStreamWrite:
    """
    TimeStream Write utils
    """

    record_chunk: bool = False
    response: list = []

    def __init__(self, boto_session):
        self.sess = boto_session
        self.client = self.sess.client("timestream-write")

    def nested_chunk(self, data_list: list, chunk_size: int):
        i = 0
        new_list = []
        while i < len(data_list):
            new_list.append(data_list[i : i + chunk_size])
            i += chunk_size

        return new_list

    def write_records(self, db_name, table_name, records, chunk_size=None):
        """
        writes given records rows
        to defined `table_name` of database `db_name`
        on aws timestreme.
        """

        def _write(records):
            result = self.client.write_records(
                DatabaseName=db_name,
                TableName=table_name,
                Records=records,
                CommonAttributes={},
            )
            print(
                "WriteRecords Status: [%s]"
                % result["ResponseMetadata"]["HTTPStatusCode"]
            )
            return result

        if self.record_chunk == True:
            nested_rec = self.nested_chunk(records, chunk_size)
            for rec in nested_rec:
                resp = _write(rec)
                self.response.append(resp)
        else:
            resp = _write(records)
            self.response.append(resp)


class ViewDictTable:
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

            measure_name = t_row.get("MeasureName")
            measure_value = t_row.get("MeasureValue")
            measure_value_type = t_row.get("MeasureValueType")

            if measure_name:
                _d["MeasureName"] = measure_name
            if measure_value:
                _d["MeasureValue"] = measure_value
            if measure_value_type:
                _d["MeasureValueType"] = measure_value_type

            _t.append(_d)

        return _t
