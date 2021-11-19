import sys
import traceback


class TimeStreamRead:
    """
    query and read timestreme query and reautn in dictionary format
    """

    def __init__(self, boto_session):
        self.sess = boto_session
        self.client = self.sess.client("timestream-query")

    def message(self, msg, debug: bool):
        if debug:
            print(msg)
        return

    def _shrink(self, l: list):
        if len(l) == 1 and isinstance(l, list):
            if isinstance(l[0], list):
                return self._shrink(l[0])
        return l

    def run_query(
        self,
        query: str,
        max_items: int = 10,
        debug: bool = True,
        filter: bool = False,
        **kwargs,
    ) -> list:

        try:
            paginator = self.client.get_paginator("query")
            page_iterator = paginator.paginate(
                QueryString=query,
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

        except Exception as err:
            print("Exception while running query:", err)
            traceback.print_exc(file=sys.stderr)

        return self._shrink(data)

    def _merge_dicts(self, l: list):
        f = {}
        for D in l:
            for key, value in D.items():
                f[key] = value
        return f

    def read_rows(
        self,
        data: dict,
        to_dimention: bool = False,
        nullarg=None,
        add_cols=None,
    ):
        """
        Reads raw data to timestreme table, and converts it
        to dict or dimention
        """

        rows = []
        self.__is_dimention = to_dimention

        # print(data["Rows"])
        for columns in data["Rows"]:
            tmp = {}
            c_info = data["ColumnInfo"]
            c_data = columns["Data"]

            for c in range(len(c_data)):
                if "NullValue" in c_data[c].keys():
                    tmp[c_info[c]["Name"]] = nullarg
                elif "ScalarValue" in c_data[c].keys():
                    tmp[c_info[c]["Name"]] = c_data[c]["ScalarValue"]

            if add_cols != None and isinstance(add_cols, dict):
                for col in add_cols:
                    tmp[col] = add_cols[col]

            if not to_dimention:
                rows.append(tmp)
            else:
                rows.append(self.to_dimention(tmp))

        return rows

    def add_cols(self, table: list, columns: dict) -> list:
        """
        Add extra column to records timestreme table
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

        if not isinstance(d, dict):
            print(d)
            raise ValueError(f"Required datatype {type({})} got {type(d)}")

        return [{"Name": i, "Value": d[i]} for i in d]

    def to_table(self, rows: list, epoc_time) -> list:
        """
        Table contains records or each dimention/rows
        """

        records: list = []
        for row in rows:

            if self.__is_dimention == False:
                dim = self.to_dimention(row)
            else:
                dim = row

            record = {
                "Time": epoc_time,
                "Dimensions": dim,
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
