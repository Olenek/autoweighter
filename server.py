from odata import ODataService
from flask import Flask, jsonify
import time, re, requests
from datetime import datetime, timedelta
from skpy import Skype
app = Flask(__name__)

sk = Skype("sberalerter@yandex.ru", "Antonio&2002")


def send_alert(id_vsp, name, warning, n):
    url = 'https://iotp2000450966trial.hanatrial.ondemand.com/iot/device.xsodata/'
    Service = ODataService(url, reflect_entities=True)
    users = Service.entities['IOT_SKYPE']
    query = Service.query(users)
    query = query.limit(1000000)
    query = query.filter(users.ID_VSP == id_vsp)
    if warning:
        for user in query:
            if user.ID_SKYPE != "veyukov.k.ge@sberbank.ru":
                sk.contacts[user.ID_SKYPE].chat.sendMsg("Внимание, в {}, заканчивается вода, осталось - {}бут. закажите поставку.".format(name))
                print("message send to {} for vsp_id {}".format(user.ID_SKYPE, user.ID_VSP, n))
    else:
        for user in query:
            if user.ID_SKYPE != "veyukov.k.ge@sberbank.ru":
                sk.contacts[user.ID_SKYPE].chat.sendMsg("Внимание, в {}, закончилась вода, срочно закажите поставку!".format(name))
                print("message send to {} for vsp_id {}".format(user.ID_SKYPE, user.ID_VSP))


def get_date(time_string):
    epoch = datetime(1970, 1, 1)
    ticks, offset = re.match(r'/Date\((\d+)([+-]\d{4})?\)/$', time_string).groups()
    utc_dt = epoch + timedelta(milliseconds=int(ticks))
    return utc_dt


def get_thresholds():
    a = {}
    url = 'https://iotp2000450966trial.hanatrial.ondemand.com/iot/device.xsodata/'
    Service = ODataService(url, reflect_entities=True)
    items = Service.entities['IOT_VSP']
    query_items = Service.query(items)
    query_items = query_items.limit(1000000)
    for item in query_items:
        a[item.ID_VSP] = int(item.WATER_MIN)
    return a


def find_vsp(i):
    a = {}
    url = 'https://iotp2000450966trial.hanatrial.ondemand.com/iot/device.xsodata/'
    Service = ODataService(url, reflect_entities=True)
    items = Service.entities['IOT_ITEM']
    query_items = Service.query(items)
    query_items = query_items.limit(1000000)
    query_items = query_items.order_by(items.DATE.asc())
    for item in query_items:
        a[item.ID_VSP] = i
    return a

def get_devices():
    a = {}
    url = 'https://iotp2000450966trial.hanatrial.ondemand.com/iot/device.xsodata/'
    Service = ODataService(url, reflect_entities=True)
    for key in find_vsp(10).keys():
        mapping = Service.entities['IOT_MAP']
        query_map = Service.query(mapping)
        query_map = query_map.limit(1000000)
        query_map = query_map.filter(mapping.ID_VSP == key)
        for item in query_map:
            a[item.G_DEVICE] = key
    return a

@app.route('/')
def main():
    thresholds = get_thresholds()
    date0 = {}
    weight0 = {}
    last_qsum = find_vsp(10)
    checksum = find_vsp(2)
    for key in last_qsum.keys():
        date0[key] = datetime(1970, 1, 1)
        weight0[key] = 0
    while True:
        url = 'https://iotp2000450966trial.hanatrial.ondemand.com/iot/device.xsodata/'
        Service = None
        while Service is None:
            try:
                # connect
                Service = ODataService(url, reflect_entities=True)
            except:
                print("database is not found")
        devices = get_devices()
        for device in get_devices().keys():
            id = devices.get(device)
            dev_d = Service.entities['IOT_DEVICE_DATA']
            query1 = Service.query(dev_d)
            query1 = query1.filter(dev_d.G_DEVICE == device)
            query1 = query1.limit(100000)
            query1 = query1.order_by(dev_d.G_CREATED.desc())
            item_d = query1.first()
            time_string = item_d.G_CREATED
            print(get_date(time_string), " ? ", date0[id])
            if get_date(time_string) > date0[id]:
                date0[id] = get_date(time_string)
                if float(item_d.C_WEIGHT) > weight0[id]:
                    items = Service.entities['IOT_ITEM']
                    mapping = Service.entities['IOT_MAP']
                    names  = Service.entities['IOT_VSP']

                    query_items = Service.query(items)
                    query_items = query_items.limit(1000000)

                    query_names = Service.query(names)
                    query_names = query_names.limit(1000000)

                    query_map = Service.query(mapping)
                    query_map = query_map.limit(1000000)

                    item = query_items.entity.__new__(cls=query_items.entity)
                    item.ID_VSP = query_map.get(item_d.G_DEVICE).ID_VSP
                    print(item.ID_VSP)
                    query_items = query_items.order_by(items.DATE.asc())
                    for item1 in query_items:
                        last_qsum[item1.ID_VSP] = int(item1.QSUM)
                    if last_qsum[item.ID_VSP] > thresholds[item.ID_VSP]:
                        checksum[item.ID_VSP] = 0
                    if last_qsum[item.ID_VSP] > 0:
                        item.QUANTITY = -1
                        item.DATE = str(time_string)
                        item.ID_MATERIAL = 1
                        item.DEB_KRE = 'H'
                        url = "https://iotp2000450966trial.hanatrial.ondemand.com/iot/device.xsodata/IOT_ITEM"
                        payload = ({
                            "ID_VSP": str(item.ID_VSP),
                            "ID_MATERIAL": str(item.ID_MATERIAL),
                            "DATE": str(item.DATE),
                            "DEB_KRE": str(item.DEB_KRE),
                            "QUANTITY": str(item.QUANTITY),
                            "QSUM": str(-1)
                        })
                        print("send")
                        requests.post(url, json=payload)
                    if thresholds[item.ID_VSP] > last_qsum[item.ID_VSP] > 0:
                        if checksum[item.ID_VSP] == 0:
                            name = query_names.get(item.ID_VSP).NAME_VSP
                            send_alert(item.ID_VSP, name, True, last_qsum[item.ID_VSP])
                        checksum[item.ID_VSP] = 1
                    elif last_qsum[item.ID_VSP] == 0:
                        if checksum[item.ID_VSP] == 1 or checksum[item.ID_VSP] == 0:
                            name = query_names.get(item.ID_VSP).NAME_VSP
                            send_alert(item.ID_VSP, name, False, last_qsum[item.ID_VSP])
                        checksum[item.ID_VSP] = 2
                        print("QSUM lower than 0")
                weight0[id] = float(item_d.C_WEIGHT)
        time.sleep(60)
    return jsonify(status=200)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080)
