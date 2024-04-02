import os
from sdqlpy.sdql_lib import *
import q1_test_all_compiled
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

############ Reading Dataset

## The following path must point to your dbgen dataset.
# dataset_path = os.getenv('TPCH_DATASET')

## Shows the number of returned results, average and stdev of run time, and the results (if the next parameter is also set to True)
verbose = True
show_results = False

## Number of iterations for benchmarking each query (must be >=2)
iterations = 2

############ Reading Dataset

lineitem_type = {record({"l_orderkey": int, "l_partkey": int, "l_suppkey": int, "l_linenumber": int, "l_quantity": float, "l_extendedprice": float, "l_discount": float, "l_tax": float, "l_returnflag": string(1), "l_linestatus": string(1), "l_shipdate": date, "l_commitdate": date, "l_receiptdate": date, "l_shipinstruct": string(25), "l_shipmode": string(10), "l_comment": string(44), "l_NA": string(1)}): bool}
customer_type = {record({"c_custkey": int, "c_name": string(25), "c_address": string(40), "c_nationkey": int, "c_phone": string(15), "c_acctbal": float, "c_mktsegment": string(10), "c_comment": string(117), "c_NA": string(1)}): bool}
order_type = {record({"o_orderkey": int, "o_custkey": int, "o_orderstatus": string(1), "o_totalprice": float, "o_orderdate": date, "o_orderpriority": string(15), "o_clerk": string(15), "o_shippriority": int, "o_comment": string(79), "o_NA": string(1)}): bool}
nation_type = {record({"n_nationkey": int, "n_name": string(25), "n_regionkey": int, "n_comment": string(152), "n_NA": string(1)}): bool}
region_type = {record({"r_regionkey": int, "r_name": string(25), "r_comment": string(152), "r_NA": string(1)}): bool}
part_type = {record({"p_partkey": int, "p_name": string(55), "p_mfgr": string(25), "p_brand": string(10), "p_type": string(25), "p_size": int, "p_container": string(10), "p_retailprice": float, "p_comment": string(23), "p_NA": string(1)}): bool}
partsupp_type = {record({"ps_partkey": int, "ps_suppkey": int, "ps_availqty": float, "ps_supplycost": float, "ps_comment": string(199), "ps_NA": string(1)}): bool}
supplier_type = {record({"s_suppkey": int, "s_name": string(25), "s_address": string(40), "s_nationkey": int, "s_phone": string(15), "s_acctbal": float, "s_comment": string(101), "s_NA": string(1)}): bool}

# lineitem = read_csv(dataset_path + "lineitem.tbl", lineitem_type, "li")
# customer = read_csv(dataset_path + "customer.tbl", customer_type, "cu")
# order = read_csv(dataset_path + "orders.tbl", order_type, "ord")
# nation = read_csv(dataset_path + "nation.tbl", nation_type, "na")
# region = read_csv(dataset_path + "region.tbl", region_type, "re")
# part = read_csv(dataset_path + "part.tbl", part_type, "pa")
# partsupp = read_csv(dataset_path + "partsupp.tbl", partsupp_type, "ps")
# supplier = read_csv(dataset_path + "supplier.tbl", supplier_type, "su")

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

event_types = {"lineitem": LINEITEM_entry_type}

###### M3 Maps

sum_qty = {record({"return_flag": "VarChar<1>", "linestatus": "VarChar<1>"}): "double"}
sum_base_price = {record({"return_flag": "VarChar<1>", "linestatus": "VarChar<1>"}): "double"}
sum_disc_price = {record({"return_flag": "VarChar<1>", "linestatus": "VarChar<1>"}): "double"}
sum_charge = {record({"return_flag": "VarChar<1>", "linestatus": "VarChar<1>"}): "double"}
avg_qty = {record({"return_flag": "VarChar<1>", "linestatus": "VarChar<1>"}): "double"}
avg_qty_mlineitem2_l1_1 = {record({"return_flag": "VarChar<1>", "linestatus": "VarChar<1>"}): "long"}
avg_qty_mlineitem3 = {record({"return_flag": "VarChar<1>", "linestatus": "VarChar<1>"}): "double"}
avg_price = {record({"return_flag": "VarChar<1>", "linestatus": "VarChar<1>"}): "double"}
avg_price_mlineitem3 = {record({"return_flag": "VarChar<1>", "linestatus": "VarChar<1>"}): "double"}
avg_disc = {record({"return_flag": "VarChar<1>", "linestatus": "VarChar<1>"}): "double"}
avg_disc_mlineitem3 = {record({"return_flag": "VarChar<1>", "linestatus": "VarChar<1>"}): "double"}
count_order = {record({"return_flag": "VarChar<1>", "linestatus": "VarChar<1>"}): "long"}

map_types = [sum_qty,
             sum_base_price,
             sum_disc_price,
             sum_charge,
             avg_qty,
             avg_qty_mlineitem2_l1_1,
             avg_qty_mlineitem3,
             avg_price,
             avg_price_mlineitem3,
             avg_disc,
             avg_disc_mlineitem3,
             count_order]

###### Query Result

result_type = {record({"lineitem_returnflag": "VarChar<1>",
                       "lineitem_linestatus": "VarChar<1>",
                       "sum_qty": "double",
                       "sum_base_price": "double",
                       "sum_disc_price": "double",
                       "sum_charge": "double",
                       "avg_qty": "double",
                       "avg_price": "double",
                       "avg_disc": "double",
                       "count_order": "long"}): bool}

######

@final_result(result_type)
def get_snapshot():
    return result_type


####### Triggers (Ensure these are in the same order as in the M3 file) (if poss make 1 function)

@m3_trigger("insert", LINEITEM_entry_type, sum_qty)
def insert_lineitem_sum_qty(lineitem_entry):
    updateMap(sum_qty, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")), getStream("lineitem_entry.quantity")) if (19970901 >= getStream("lineitem_entry.shipdate")) else None

@m3_trigger("insert", LINEITEM_entry_type, sum_base_price)
def insert_lineitem_sum_base_price(lineitem_entry):
    updateMap(sum_base_price, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")), getStream("lineitem_entry.extendedprice")) if (19970901 >= getStream("lineitem_entry.shipdate")) else None

@m3_trigger("insert", LINEITEM_entry_type, sum_disc_price)
def insert_lineitem_sum_disc_price(lineitem_entry):
    updateMap(sum_disc_price, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")), getStream("lineitem_entry.extendedprice") * (1 - (getStream("lineitem_entry.discount")))) if (19970901 >= getStream("lineitem_entry.shipdate")) else None

@m3_trigger("insert", LINEITEM_entry_type, sum_charge)
def insert_lineitem_sum_charge(lineitem_entry):
    updateMap(sum_charge, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")), getStream("lineitem_entry.extendedprice") * ((1 - (getStream("lineitem_entry.discount"))) * (1 + getStream("lineitem_entry.tax")))) if (19970901 >= getStream("lineitem_entry.shipdate")) else None

@m3_trigger("insert", LINEITEM_entry_type, avg_qty)
def insert_lineitem_avg_qty(lineitem_entry):
    agg1 = 0.0
    lift1 = getValueOrDefault(avg_qty_mlineitem2_l1_1, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus"))) + 1
    agg1 += 1.0 / max([1, lift1]) if (0 != lift1) else 0.0
    agg2 = 0.0
    lift2 = getValueOrDefault(avg_qty_mlineitem2_l1_1, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")))
    agg2 += 1.0 / max([1, lift2]) if (0 != lift2) else 0.0
    updateMap(avg_qty, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")), ((agg1 * (getValueOrDefault(avg_qty_mlineitem3, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus"))) + getStream("lineitem_entry.quantity"))) + (agg2 * (getValueOrDefault(avg_qty_mlineitem3, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus"))) * -1)))) if (19970901 >= getStream("lineitem_entry.shipdate")) else None

@m3_trigger("insert", LINEITEM_entry_type, avg_qty_mlineitem3)
def insert_lineitem_avg_qty_mlineitem3(lineitem_entry):
    updateMap(avg_qty_mlineitem3, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")), getStream("lineitem_entry.quantity")) if (19970901 >= getStream("lineitem_entry.shipdate")) else None

@m3_trigger("insert", LINEITEM_entry_type, avg_price)
def insert_lineitem_avg_price(lineitem_entry):
    agg3 = 0.0
    lift3 = getValueOrDefault(avg_qty_mlineitem2_l1_1, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus"))) + 1
    agg3 += 1.0 / max([1, lift3]) if (0 != lift3) else 0.0
    agg4 = 0.0
    lift4 = getValueOrDefault(avg_qty_mlineitem2_l1_1, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")))
    agg4 += 1.0 / max([1, lift4]) if (0 != lift4) else 0.0
    updateMap(avg_price, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")), ((agg3 * (getValueOrDefault(avg_price_mlineitem3, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus"))) + getStream("lineitem_entry.extendedprice"))) + (agg4 * (getValueOrDefault(avg_price_mlineitem3, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus"))) * -1)))) if (19970901 >= getStream("lineitem_entry.shipdate")) else None

@m3_trigger("insert", LINEITEM_entry_type, avg_price_mlineitem3)
def insert_lineitem_avg_price_mlineitem3(lineitem_entry):
    updateMap(avg_price_mlineitem3, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")), getStream("lineitem_entry.extendedprice")) if (19970901 >= getStream("lineitem_entry.shipdate")) else None

@m3_trigger("insert", LINEITEM_entry_type, avg_disc)
def insert_lineitem_avg_disc(lineitem_entry):
    agg5 = 0.0
    lift5 = getValueOrDefault(avg_qty_mlineitem2_l1_1, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus"))) + 1
    agg5 += 1.0 / max([1, lift5]) if (0 != lift5) else 0.0
    agg6 = 0.0
    lift6 = getValueOrDefault(avg_qty_mlineitem2_l1_1, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")))
    agg6 += 1.0 / max([1, lift6]) if (0 != lift6) else 0.0
    updateMap(avg_disc, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")), ((agg5 * (getValueOrDefault(avg_disc_mlineitem3, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus"))) + getStream("lineitem_entry.discount"))) + (agg6 * (getValueOrDefault(avg_disc_mlineitem3, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus"))) * -1)))) if (19970901 >= getStream("lineitem_entry.shipdate")) else None

@m3_trigger("insert", LINEITEM_entry_type, avg_qty_mlineitem2_l1_1)
def insert_lineitem_avg_qty_mlineitem2_l1_1(lineitem_entry):
    updateMap(avg_qty_mlineitem2_l1_1, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")), 1) if (19970901 >= getStream("lineitem_entry.shipdate")) else None

@m3_trigger("insert", LINEITEM_entry_type, avg_disc_mlineitem3)
def insert_lineitem_avg_disc_mlineitem3(lineitem_entry):
    updateMap(avg_disc_mlineitem3, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")), getStream("lineitem_entry.discount")) if (19970901 >= getStream("lineitem_entry.shipdate")) else None

@m3_trigger("insert", LINEITEM_entry_type, count_order)
def insert_lineitem_count_order(lineitem_entry):
    updateMap(count_order, (getStream("lineitem_entry.returnflag"), getStream("lineitem_entry.linestatus")), 1) if (19970901 >= getStream("lineitem_entry.shipdate")) else None

####### IVM Logic

@event_processor({"event_types": event_types, "map_types": map_types})  # this maybe should be a list
def process_stream_event(ins_or_del, event_type, event_params):
    if ins_or_del == "i":
        results = "None1"
    else:
        results = "hello world"    
    return results  

######### Function Calls

# file_path = "/home/james/UGProject/dbtoaster-experiments-data/tpch/standard/lineitem.csv"

# def convert_date_format(date_str):
#     """
#     Convert date from YYYY-MM-DD to YYYYMMDD format.
#     """
#     # Check if the date_str matches the YYYY-MM-DD format
#     if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
#         return date_str.replace('-', '')
#     return date_str

# def process_and_rewrite_csv(input_file_path, output_file_path):
#     with open(input_file_path, 'r') as input_file, open(output_file_path, 'w', newline='') as output_file:
#         reader = csv.reader(input_file, delimiter='|')
#         writer = csv.writer(output_file, delimiter='|')
        
#         for row in reader:
#             # Convert each element in the row if it matches the date format
#             processed_row = [convert_date_format(element) for element in row]
#             writer.writerow(processed_row)


# output_file_path = 'lineitem.csv'
# process_and_rewrite_csv(file_path, output_file_path)
#print("\n\n\n\n\n225\n\n\n\n")
lineitem_stream = []
file_path = 'lineitem.csv'
ress = []
if preload:
    with open(file_path, 'r') as file:
        reader = csv.reader(file, delimiter='|')

        for row in reader:
            lineitem_stream.append(row)

    ins_or_del = "insert"
    event_type = "lineitem"

    event_stream = []

    for stream_event in lineitem_stream:
        event_params = []
        for i in range(0, len(stream_event)):
            if list(LINEITEM_entry_type.values())[i] == "long":
                event_params.append(int(stream_event[i]))
            elif list(LINEITEM_entry_type.values())[i] == "double":
                event_params.append(float(stream_event[i]))
            else:
                event_params.append(stream_event[i])
        args = [ins_or_del, event_type, event_params]
        event_stream.append(args)
        #thing = test_all_compiled.process_stream_event_compiled(*args)

    f = open("q1updateTimes.txt", 'w')

    start_time = time.perf_counter()
    process_time = 0.0
    for args in event_stream:
        process_start = time.perf_counter()
        thing = q1_test_all_compiled.process_stream_event_compiled(*args)
        ress.append(q1_test_all_compiled.view_snapshot_compiled())
        process_end = time.perf_counter()
        f.write(str(process_end - process_start) + "\n")
        process_time += process_end - process_start
    
    f.close()

# for i in range(0, 5):
#     print(lineitem_stream[i])
thing2 = q1_test_all_compiled.view_snapshot_compiled()
end_time = time.perf_counter()
print(thing)
print(thing2)
time_taken = end_time - start_time
print("Time taken: " + str(time_taken))
print("Process time: " + str(process_time))
print("Processed: " + str(len(event_stream)))
