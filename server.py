from odata import ODataService
from flask import Flask, jsonify
import time, re, requests
from datetime import datetime, timedelta
app = Flask(__name__)


def get_date(time_string):
    epoch = datetime(1970, 1, 1)
    ticks, offset = re.match(r'/Date\((\d+)([+-]\d{4})?\)/$', time_string).groups()
    utc_dt = epoch + timedelta(milliseconds=int(ticks))
    return utc_dt


def find_vsp():
    a = {}
    url = 'https://iotp2000450966trial.hanatrial.ondemand.com/iot/device.xsodata/'
    Service = ODataService(url, reflect_entities=True)
    items = Service.entities['IOT_ITEM']
    query_items = Service.query(items)
    query_items = query_items.limit(1000000)
    query_items = query_items.order_by(items.DATE.asc())
    for item in query_items:
        a[item.ID_VSP] = 0
    return a


@app.route('/')
def main():
    date0 = datetime(1970, 1, 1)
    weight0 = 0
    while True:
        url = 'https://iotp2000450966trial.hanatrial.ondemand.com/iot/device.xsodata/'
        Service = None
        while Service is None:
            try:
                # connect
                Service = ODataService(url, reflect_entities=True)
            except:
                print("database is not found")
        dev_d = Service.entities['IOT_DEVICE_DATA']
        query1 = Service.query(dev_d)
        query1 = query1.limit(100000)
        query1 = query1.order_by(dev_d.G_CREATED.desc())
        item_d = query1.first()
        time_string = item_d.G_CREATED
        if get_date(time_string) > date0:
            print(item_d.C_WEIGHT)
            date0 = get_date(time_string)
            if float(item_d.C_WEIGHT) > weight0:
                items = Service.entities['IOT_ITEM']
                mapping = Service.entities['IOT_MAP']
                query_items = Service.query(items)
                query_items = query_items.limit(1000000)
                query_map = Service.query(mapping)
                query_map = query_map.limit(1000000)
                item = query_items.entity.__new__(cls=query_items.entity)
                item.ID_VSP = query_map.get(item_d.G_DEVICE).ID_VSP
                query_items = query_items.order_by(items.DATE.desc())
                last_qsum = query_items.first().QSUM
                if not int(last_qsum) < 0:
                    item.QUANTITY = -1
                    item.DATE = str(time_string)
                    # item.DATE = str(datetime.now())
                    item.ID_MATERIAL = 1
                    item.DEB_KRE = 'H'
                    print(item.ID_VSP, item.ID_MATERIAL, item.DATE, item.DEB_KRE, item.QUANTITY)
                    url = "https://iotp2000450966trial.hanatrial.ondemand.com/iot/device.xsodata/IOT_ITEM"
                    print(url)
                    payload = ({
                        "ID_VSP": str(item.ID_VSP),
                        "ID_MATERIAL": str(item.ID_MATERIAL),
                        "DATE": str(item.DATE),
                        "DEB_KRE": str(item.DEB_KRE),
                        "QUANTITY": str(item.QUANTITY),
                        "QSUM": str(0)
                    })
                    requests.post(url, json=payload)
            weight0 = float(item_d.C_WEIGHT)
        time.sleep(60)
    return jsonify(status=200)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
