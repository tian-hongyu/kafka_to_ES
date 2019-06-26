#!/usr/bin/python2
# coding=utf-8


import zmq
import json
import pprint


def main():
    ctx = zmq.Context()
    s = ctx.socket(zmq.SUB)
    s.connect("tcp://127.0.0.1:5556")
    s.setsockopt(zmq.SUBSCRIBE, b'flow')
    # s.setsockopt(zmq.SUBSCRIBE, b'flow')
    while True:
        # topic, msg = s.recv_multipart()
        # print('   Topic: %s, msg:%s' % (topic, msg))
        msg = s.recv()
        # print(msg)
        # ipdb.set_trace()
        # jsonitem = json.loads(msg.decode("utf-8"))
        # pprint.pprint(jsonitem)
        # print(msg)
        # print("#####################")
        print(msg)
        if msg[:2] == b'{"':
            try:
                jsonItem = json.loads(msg)
                # pprint.pprint(jsonItem)
                if jsonItem["57590"] == "152":
                    cts_teid = jsonItem["57694"]
                    stc_teid = jsonItem["57696"]
                    cip = jsonItem["57698"]
                    imsi = jsonItem["57699"]
                    msisdn = jsonItem["57700"]
                    imei = jsonItem["57701"]
                    print("c2s_teid:%s,s2c_teid:%s,imsi:%s,msisdn:%s,imei:%s,cip:%s" % (
                        cts_teid, stc_teid, imsi, msisdn, imei, cip))
            except:
                print(msg)


if __name__ == "__main__":
    main()
