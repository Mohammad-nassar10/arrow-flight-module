#
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0
#
import pyarrow.flight as fl
import pyarrow as pa
import json
import pandas as pd
import pyarrow.ipc as ipc
import numpy
# taken from https://github.com/apache/arrow/blob/master/python/pyarrow/tests/test_flight.py#L450
class HttpBasicClientAuthHandler(fl.ClientAuthHandler):
    """An example implementation of HTTP basic authentication."""
    def __init__(self, username, password):
        super().__init__()
        self.basic_auth = fl.BasicAuth(username, password)
        self.token = None
    def authenticate(self, outgoing, incoming):
        auth = self.basic_auth.serialize()
        outgoing.write(auth)
        self.token = incoming.read()
    def get_token(self):
        return self.token
request_orig = {
    "asset": "new-dataset", 
}
request_new = {
    # "asset": "fybrik-notebook-sample/new-data-parquet",
    "asset": "new-dataset",
}
def main(port, username, password):
    client = fl.connect("grpc://localhost:{}".format(port))
    # client = fl.connect("grpc://my-notebook-fybrik-notebook-sample-arrow-flight-aef23.fybrik-blueprints:80")
    if username or password:
        client.authenticate(HttpBasicClientAuthHandler(username, password))
    # write the new dataset
    data = pa.Table.from_arrays([pa.array(range(0, 10 * 1024)), pa.array(range(1, 10 * 1024 + 1))], names=['a', 'b'])
    writer, _ = client.do_put(fl.FlightDescriptor.for_command(json.dumps(request_orig)),
                              data.schema)
    writer.write_table(data, 1024)
    writer.close()


    # Send request and fetch result as a pandas DataFrame
    info = client.get_flight_info(fl.FlightDescriptor.for_command(json.dumps(request_orig)))
    reader: fl.FlightStreamReader = client.do_get(info.endpoints[0].ticket)
    df: pd.DataFrame = reader.read_all().to_pandas()
    print(df)
 
    # df.pop("a")
    
    table = pa.Table.from_pandas(df)

    # batches = table.to_batches
    # new_col = (table.column(0).to_pylist() + table.column(1).to_pylist())
    new_col = numpy.add(table.column(0).to_pylist(), table.column(1).to_pylist())
    new_col = [n / 2 for n in new_col]
    print(new_col)
    table = table.append_column('avg', pa.array(new_col, pa.float32()))
    s_schema = table.schema
    
    # schema = pa.schema(info.schema)
    # s_schema = pa.ipc.read_schema(schema)
    # write the new dataset
    # data = pa.Table.from_arrays([pa.array(range(0, 10 * 1024))], names=['a'])
    # data = pa.Table.from_pandas(df)
    writer, _ = client.do_put(fl.FlightDescriptor.for_command(json.dumps(request_orig)),
                              s_schema)
    writer.write_table(table, 1024)
    writer.close()
    # now that the dataset is in place, let's try to read it
    info = client.get_flight_info(
        fl.FlightDescriptor.for_command(json.dumps(request_orig)))
    endpoint = info.endpoints[0]
    result: fl.FlightStreamReader = client.do_get(endpoint.ticket)
    print(result.read_all().to_pandas())

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='arrow-flight-module sample')
    parser.add_argument(
        '--port', type=int, default=8080, help='Listening port')
    parser.add_argument(
        '--username', type=str, default=None, help='Authentication username')
    parser.add_argument(
        '--password', type=str, default=None, help='Authentication password')
    args = parser.parse_args()
    main(args.port, args.username, args.password)












# #
# # Copyright 2020 IBM Corp.
# # SPDX-License-Identifier: Apache-2.0
# #
# import pyarrow.flight as fl
# import pyarrow as pa
# import json

# # taken from https://github.com/apache/arrow/blob/master/python/pyarrow/tests/test_flight.py#L450
# class HttpBasicClientAuthHandler(fl.ClientAuthHandler):
#     """An example implementation of HTTP basic authentication."""

#     def __init__(self, username, password):
#         super().__init__()
#         self.basic_auth = fl.BasicAuth(username, password)
#         self.token = None

#     def authenticate(self, outgoing, incoming):
#         auth = self.basic_auth.serialize()
#         outgoing.write(auth)
#         self.token = incoming.read()

#     def get_token(self):
#         return self.token

# request = {
#     "asset": "new-dataset", 
# }

# def main(port, username, password):
#     client = fl.connect("grpc://localhost:{}".format(port))
#     if username or password:
#         client.authenticate(HttpBasicClientAuthHandler(username, password))

#     # write the new dataset
#     data = pa.Table.from_arrays([pa.array(range(0, 10 * 1024))], names=['a'])
#     writer, _ = client.do_put(fl.FlightDescriptor.for_command(json.dumps(request)),
#                               data.schema)
#     writer.write_table(data, 1024)
#     writer.close()

#     # now that the dataset is in place, let's try to read it
#     info = client.get_flight_info(
#         fl.FlightDescriptor.for_command(json.dumps(request)))

#     endpoint = info.endpoints[0]
#     result: fl.FlightStreamReader = client.do_get(endpoint.ticket)
#     print(result.read_all().to_pandas())

# if __name__ == "__main__":
#     import argparse
#     parser = argparse.ArgumentParser(description='arrow-flight-module sample')
#     parser.add_argument(
#         '--port', type=int, default=8080, help='Listening port')
#     parser.add_argument(
#         '--username', type=str, default=None, help='Authentication username')
#     parser.add_argument(
#         '--password', type=str, default=None, help='Authentication password')
#     args = parser.parse_args()

#     main(args.port, args.username, args.password)
