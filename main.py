from nsetools import Nse

nse = Nse()
from nsepy import get_history
from datetime import date
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
from json import loads


def get_influx2_client():
    try:
        client = influxdb_client.InfluxDBClient(
            url="http://localhost:8086",
            token="kherI5PpUJb8fLhmzMXbxKKfCIUKQ2uJ9Iq8Dy45HQdZy3f4MamVGN0MtHTRPH8IxtLWoLlA6UwILXjm3Q1gIQ==",
            org="iam"
        )
        return client
    except Exception as e:
        raise e


if __name__ == '__main__':
    indexes = nse.get_stock_codes()
    # for code in indexes:
    del indexes["SYMBOL"]
    client = get_influx2_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    indexes = indexes.keys()

    from time import sleep

    # indexes = ["3PLAND"]

    for stock_code in indexes:
        if stock_code[0] in ["A", "B", "C","2","3","5"]:
            continue

        if stock_code[0:2] in ["DA","DB","DC","DE","DF","DH","DI","DL","DM","DN","DO","DP","DR","DS","DT","DU","DG"]:
            continue

        if stock_code == "DVL":
            continue
        print(stock_code)
        sleep(0.1)
        data = get_history(symbol=stock_code, start=date(2020, 1, 1), end=date(2021, 7, 10))
        data = loads(data.to_json())

        push_dict = {}

        for root_key in data.keys():
            for data_item in data[root_key]:
                try:
                    push_dict[data_item].update({
                        root_key: data[root_key][data_item]
                    })
                except KeyError:
                    push_dict.update(
                        {
                            data_item: {
                                root_key: data[root_key][data_item]
                            }
                        }
                    )
        push_list = []
        for item in push_dict:
            timestamp = datetime.fromtimestamp(int(item) / 1000).astimezone().isoformat()
            metric = {
                "measurement": "stocks",
                "tags": {
                    "stock_code": stock_code
                },
                "fields": {

                },
                "time": timestamp
            }
            for tag in push_dict[item]:
                value_ = push_dict[item][tag]
                if value_ is not None:
                    if tag == "Deliverable Volume":
                        value_ = int(value_)

                    metric["fields"].update({
                        tag: value_
                    })
            try:
                write_api.write(bucket="stocks", record=[metric])
            except Exception as e:
                pass
            pass
            push_list.append(metric)

        # try:
        #     write_api.write(bucket="stocks", record=push_list)
        # except Exception as e:
        #     pass
        # pass
