import os
from sdqlpy.sdql_lib import *
import q3_test_all_compiled
import re
import time
import random

## The "sdqlpy_init" function must be defined here. 
## Its first parameter can be set to: 
## 0: to run in Python | 1: to run in compiled mode | 2: to run in compiled mode using the previously compiled version.
## Its second parameter show the number of threads. (current version does not support multi-threading for "run in Python" mode) 
## Note: when the first parameter is set to "1", the previous number of threads is used and the current parameter will be ignored. 

sdqlpy_init(1, 2)
#sdqlpy_init(2, 2)

preload = True

###### M3 Sources

LINEITEM_entry_type = { # TODO: change this to Python types and use fast_dict_generator.getCPPType to get the C++ types
    "orderkey": "long",
    "partkey": "long",
    "suppkey": "long",
    "linenumber": "long",
    "quantity": "double",
    "extendedprice": "double",
    "discount": "double",
    "tax": "double",
    "returnflag": "VarChar<1>",
    "linestatus": "VarChar<1>",
    "shipdate": "long",
    "commitdate": "long",
    "receiptdate": "long",
    "shipinstruct": "VarChar<63>",
    "shipmode": "VarChar<63>",
    "comment": "VarChar<63>"
}

ORDERS_entry_type = {
    "orderkey": "long",
    "custkey": "long",
    "orderstatus": "VarChar<1>",
    "totalprice": "double",
    "orderdate": "long",
    "orderpriority": "VarChar<63>",
    "clerk": "VarChar<63>",
    "shippriority": "long",
    "comment": "VarChar<63>"
}

CUSTOMER_entry_type = {
    "custkey": "long",
    "name": "VarChar<63>",
    "address": "VarChar<63>",
    "nationkey": "long",
    "phone": "VarChar<63>",
    "acctbal": "double",
    "mktsegment": "VarChar<63>",
    "comment": "VarChar<63>"
}

event_types = {
    "lineitem": LINEITEM_entry_type,
    "orders": ORDERS_entry_type,
    "customer": CUSTOMER_entry_type
}

###### M3 Maps

query3 = {record({"orders_orderkey": "long", "orders_orderdate": "long", "orders_shippriority": "long"}): "double"}
query3_mlineitem2 = {record({"query3_mlineitemlineitem_orderkey": "long",
                             "orders_orderdate": "long",
                             "orders_shippriority": "long"}): "long"}
query3_mlineitem2_mcustomer2 = {record({"query3_mlineitemlineitem_orderkey": "long",
                                       "query3_mlineitem2_mcustomercustomer_custkey": "long",
                                       "orders_orderdate": "long",
                                       "orders_shippriority": "long"}): "long"}
query3_morders2 = {record({"query3_mordersorders_custkey": "long"}): "long"}
query3_morders3 = {record({"query3_mordersorders_orderkey": "long"}): "double"}
query3_mcustomer2 = {record({"orders_orderkey": "long",
                             "query3_mcustomercustomer_custkey": "long",
                             "orders_orderdate": "long",
                             "orders_shippriority": "long"}): "double"}

map_types = [query3,
             query3_mlineitem2,
             query3_mlineitem2_mcustomer2,
             query3_morders2,
             query3_morders3,
             query3_mcustomer2]

###### Query Result

# The keys to the key record  must be exact matches to any maps
result_type = {record({"orders_orderkey": "long",
                       "orders_orderdate": "long",
                       "orders_shippriority": "long",
                       "query3": "double"}): bool}

######

@final_result(result_type)
def get_snapshot():
    return result_type

####### Triggers (Ensure these are in the same order as in the M3 file)

@agg_sum({"query3_mlineitem2": query3_mlineitem2, "query3": query3}, LINEITEM_entry_type, int)
def agg1():
    result = getMap(query3_mlineitem2).sum(lambda p:
                                           updateMap(query3, (getStream("lineitem_entry.orderkey"), p[0].orders_orderdate, p[0].orders_shippriority),
                                                     p[1] * (getStream("lineitem_entry.extendedprice") * (1 + ((-1) * getStream("lineitem_entry.discount")))))
                                           if
                                            getStream("lineitem_entry.orderkey") == p[0].query3_mlineitemlineitem_orderkey
                                           else
                                            0
                                           )
    return result

@m3_trigger("insert", LINEITEM_entry_type, query3)
def insert_lineitem_query3(lineitem_entry):
    result = agg1() if getStream("lineitem_entry.shipdate") > 19950315 else 0
    return result

@m3_trigger("insert", LINEITEM_entry_type, query3_morders3)
def insert_lineitem_query3_morders3(lineitem_entry):
    updateMap(query3_morders3, getStream("lineitem_entry.orderkey"), getStream("lineitem_entry.extendedprice") * (1 + ((-1) * getStream("lineitem_entry.discount")))) if getStream("lineitem_entry.shipdate") > 19950315 else 0

@agg_sum({"query3_mcustomer2": query3_mcustomer2,
          "query3_mlineitem2_mcustomer2": query3_mlineitem2_mcustomer2},
          LINEITEM_entry_type, int)
def agg2():
    result = getMap(query3_mlineitem2_mcustomer2).sum(lambda p:
                                                        updateMap(query3_mcustomer2,
                                                                  (getStream("lineitem_entry.orderkey"), p[0].query3_mlineitem2_mcustomercustomer_custkey, p[0].orders_orderdate, p[0].orders_shippriority),
                                                                  p[1] * (getStream("lineitem_entry.extendedprice") * (1 + ((-1) * getStream("lineitem_entry.discount"))))
                                                        )
                                                        if
                                                            getStream("lineitem_entry.orderkey") == p[0].query3_mlineitemlineitem_orderkey
                                                        else
                                                            0
                                                        )
    return result

@m3_trigger("insert", LINEITEM_entry_type, query3_mcustomer2)
def insert_lineitem_query3_mcustomer2(lineitem_entry):
    result = agg2() if getStream("lineitem_entry.shipdate") > 19950315 else 0
    return result

@m3_trigger("insert", ORDERS_entry_type, query3)
def insert_orders_query3(orders_entry):
    updateMap(query3, (getStream("orders_entry.orderkey"), getStream("orders_entry.orderdate"), getStream("orders_entry.shippriority")), getValueOrDefault(query3_morders2, getStream("orders_entry.custkey")) * getValueOrDefault(query3_morders3, getStream("orders_entry.orderkey"))) if getStream("orders_entry.orderdate") < 19950315 else 0

@m3_trigger("insert", ORDERS_entry_type, query3_mlineitem2)
def insert_orders_query3_mlineitem2(orders_entry):
    updateMap(query3_mlineitem2, (getStream("orders_entry.orderkey"), getStream("orders_entry.orderdate"), getStream("orders_entry.shippriority")), getValueOrDefault(query3_morders2, getStream("orders_entry.custkey"))) if getStream("lineitem_entry.orderdate") < 19950315 else 0

@m3_trigger("insert", ORDERS_entry_type, query3_mlineitem2_mcustomer2)
def insert_orders_query3_mlineitem2_mcustomer2(orders_entry):
    updateMap(query3_mlineitem2_mcustomer2, (getStream("orders_entry.orderkey"), getStream("orders_entry.custkey"), getStream("orders_entry.orderdate"), getStream("orders_entry.shippriority")), 1) if getStream("lineitem_entry.orderdate") < 19950315 else 0

@m3_trigger("insert", ORDERS_entry_type, query3_mcustomer2)
def insert_orders_query3_mcustomer2(orders_entry):
    updateMap(query3_mcustomer2, (getStream("orders_entry.orderkey"), getStream("orders_entry.custkey"), getStream("orders_entry.orderdate"), getStream("orders_entry.shippriority")), getValueOrDefault(query3_morders3, getStream("orders_entry.orderkey"))) if getStream("lineitem_entry.orderdate") < 19950315 else 0

@agg_sum({"query3": query3, "query3_mcustomer2": query3_mcustomer2}, CUSTOMER_entry_type, int)
def agg3():
    result = getMap(query3_mcustomer2).sum(lambda p:
                                           updateMap(query3, (p[0].orders_orderkey, p[0].orders_orderdate, p[0].orders_shippriority), p[1])
                                           if
                                            getStream("customer_entry.custkey") == p[0].query3_mcustomercustomer_custkey
                                           else
                                            0
                                           )
    return result

@m3_trigger("insert", CUSTOMER_entry_type, query3)
def insert_customer_query3(customer_entry):
    result = agg3() if getStream("customer_entry.mktsegment") == "BUILDING" else 0
    return result

@agg_sum({"query3_mlineitem2_mcustomer2": query3_mlineitem2_mcustomer2,
          "query3_mlineitem2": query3_mlineitem2}, CUSTOMER_entry_type, int)
def agg4():
    result = getMap(query3_mlineitem2_mcustomer2).sum(lambda p:
                                                      updateMap(query3_mlineitem2, (p[0].query3_mlineitemlineitem_orderkey, p[0].orders_orderdate, p[0].orders_shippriority), p[1])
                                                      if
                                                        getStream("customer_entry.custkey") == p[0].query3_mlineitem2_mcustomercustomer_custkey
                                                      else
                                                        0
                                                      )
    return result

@m3_trigger("insert", CUSTOMER_entry_type, query3_mlineitem2)
def insert_customer_query3_mlineitem2(customer_entry):
    result = agg4() if getStream("customer_entry.mktsegment") == "BUILDING" else 0
    return result

@m3_trigger("insert", CUSTOMER_entry_type, query3_morders2)
def insert_customer_query3_morders2():
    updateMap(query3_morders2, getStream("customer_entry.custkey"), 1) if getStream("customer_entry.mktsegment") == "BUILDING" else 0

####### IVM Logic

@event_processor({"event_types": event_types, "map_types": map_types})
def process_stream_event(ins_or_del, event_type, event_params):
    if ins_or_del == "i":
        results = "None1"
    else:
        results = "hello world"    
    return results  

######### Function Calls


def create_streams(event_type, event_stream):
    stream = []
    file_path = event_type + '.csv'
    preload = True
    if preload:
        with open(file_path, 'r') as file:
            reader = csv.reader(file, delimiter='|')

            for row in reader:
                stream.append(row)

        ins_or_del = "insert"

        for stream_event in stream:
            event_params = []
            for i in range(0, len(stream_event)):
                if list(event_types[event_type].values())[i] == "long":
                    event_params.append(int(stream_event[i].replace('-', '')))
                elif list(event_types[event_type].values())[i] == "double":
                    event_params.append(float(stream_event[i]))
                else:
                    event_params.append(stream_event[i][:60])
            args = [ins_or_del, event_type, event_params]
            event_stream.append(args)
    
    return event_stream

q3_test_all_compiled.on_system_ready_compiled()
ress = []
event_stream = []
for event_type in ["lineitem", "orders", "customer"]:
    event_stream = create_streams(event_type, event_stream)

# f = open("q3updateTimes.txt", 'w')

start_time = time.perf_counter()
counter = 0
process_time = 0.0
random.shuffle(event_stream)
for args in event_stream:
    process_start = time.perf_counter()
    thing = q3_test_all_compiled.process_stream_event_compiled(*args)
    # ress.append(q3_test_all_compiled.view_snapshot_compiled())
    process_end = time.perf_counter()
    # f.write(str(process_end - process_start) + "\n")
    process_time += process_end - process_start

# f.close()
res = q3_test_all_compiled.view_snapshot_compiled()
end_time = time.perf_counter()
print(thing)
print(res)
time_taken = end_time - start_time
print("Time taken: " + str(time_taken))
print("Process time: " + str(process_time))
print("Processed: " + str(len(event_stream)))
