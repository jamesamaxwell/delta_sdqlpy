import os
from sdqlpy.sdql_lib import *
import q2_test_all_compiled
import re
import time
import random

## The "sdqlpy_init" function must be defined here. 
## Its first parameter can be set to: 
## 0: to run in Python | 1: to run in compiled mode | 2: to run in compiled mode using the previously compiled version.
## Its second parameter show the number of threads. (current version does not support multi-threading for "run in Python" mode) 
## Note: when the first parameter is set to "1", the previous number of threads is used and the current parameter will be ignored. 


sdqlpy_init(1, 2)
# sdqlpy_init(2, 2)

preload = True

###### M3 Sources

PART_entry_type = { # TODO: change this to Python types and use fast_dict_generator.getCPPType to get the C++ types
    "partkey": "long",
    "name": "VarChar<63>",  # SDQL's default is 25
    "mfgr": "VarChar<63>",
    "brand": "VarChar<63>",
    "type": "VarChar<63>",
    "size": "long",
    "container": "VarChar<63>",
    "retailprice": "double",
    "comment": "VarChar<63>"
}

SUPPLIER_entry_type = { # TODO: change this to Python types and use fast_dict_generator.getCPPType to get the C++ types
    "suppkey": "long",
    "name": "VarChar<63>",
    "address": "VarChar<63>",
    "nationkey": "long",
    "phone": "VarChar<63>",
    "acctbal": "double",
    "comment": "VarChar<63>"
}

PARTSUPP_entry_type = { # TODO: change this to Python types and use fast_dict_generator.getCPPType to get the C++ types
    "partkey": "long",
    "suppkey": "long",
    "availqty": "long",
    "supplycost": "double",
    "comment": "VarChar<63>"
}

# For now tables have to be treated like streams and simply manually entered using process_stream_event in this python file.
# There needs to be a trigger function created for the tables that simply loads each entry into the corresponding map
# For timing purposes a fair comparison would be to start timing after all the tables are fully loaded
# TODO: make special cases for tables so this^^^ isn't necessary

NATION_entry_type = { # M3 Table
    "nationkey": "long",
    "name": "VarChar<63>",
    "regionkey": "long",
    "comment": "VarChar<63>"
}

REGION_entry_type = { # M3 Table
    "regionkey": "long",
    "name": "VarChar<63>",
    "comment": "VarChar<63>"
}

event_types = {
    "part": PART_entry_type,
    "supplier": SUPPLIER_entry_type,
    "partsupp": PARTSUPP_entry_type,
    "nation": NATION_entry_type,
    "region": REGION_entry_type
}

###### M3 Maps

count = {record({"s_acctbal": "double", "s_name": "VarChar<63>", "n_name": "VarChar<63>", "p_partkey": "long", "p_mfgr": "VarChar<63>", "s_address": "VarChar<63>", "s_phone": "VarChar<63>", "s_comment": "VarChar<63>"}): "long"}
count_mpartsupp1_l2_3 = {record({"count_mpartsupppartsupp_suppkey": "long"}): "long"}
count_mpartsupp1_l2_3_msupplier1 = {record({"count_mpartsupp1_l2_3_msuppliersupplier_nationkey": "long"}): "long"}
count_mpartsupp2 = {record({"count_mpartsupppartsupp_partkey": "long", "p_mfgr": "VarChar<63>"}): "long"}
count_mpartsupp7 = {record({
    "count_mpartsupppartsupp_suppkey": "long",
    "s_name": "VarChar<63>",
    "s_address": "VarChar<63>",
    "s_phone": "VarChar<63>",
    "s_acctbal": "double",
    "s_comment": "VarChar<63>",
    "n_name": "VarChar<63>"
}): "long"}
count_mpartsupp7_msupplier1 = {record({"count_mpartsupp7_msuppliersupplier_nationkey": "long", "n_name": "VarChar<63>"}): "long"}
count_msupplier1 = {record({
    "p_partkey": "long",
    "p_mfgr": "VarChar<63>",
    "s_name": "VarChar<63>",
    "s_address": "VarChar<63>",
    "s_phone": "VarChar<63>",
    "s_acctbal":"double",
    "s_comment": "VarChar<63>",
    "ps_supplycost": "double",
    "n_name": "VarChar<63>"
}): "long"}
count_msupplier1_msupplier2 = {record({"p_partkey": "long", "p_mfgr": "VarChar<63>", "count_msupplier1_msuppliersupplier_suppkey": "long", "ps_supplycost": "double"}): "long"}
count_msupplier1_msupplier2_mpart3 = {record({"count_msupplier1_msupplier2_mpartpart_partkey": "long", "count_msupplier1_msuppliersupplier_suppkey": "long", "ps_supplycost": "double"}): "long"}
count_mpart3 = {record({
    's_name': 'VarChar<63>',
    's_address': 'VarChar<63>',
    's_phone': 'VarChar<63>',
    's_acctbal': 'double',
    's_comment': 'VarChar<63>',
    'count_mpartpart_partkey': 'long',
    'ps_supplycost': 'double',
    'n_name': 'VarChar<63>'
}): "long"}
count_mpart3_l2_1 = {record({
    'count_mpartpart_partkey': 'long',
    'ps2_supplycost': 'double'
}): "long"}

nation = {record({"nationkey": "long", "name": "VarChar<63>", "regionkey": "long", "comment": "VarChar<63>"}): "long"}
region = {record({"regionkey": "long", "name": "VarChar<63>", "comment": "VarChar<63>"}): "long"}

map_types = [count,
             count_mpartsupp1_l2_3,
             count_mpartsupp1_l2_3_msupplier1,
             count_mpartsupp2,
             count_mpartsupp7,
             count_mpartsupp7_msupplier1,
             count_msupplier1,
             count_msupplier1_msupplier2,
             count_msupplier1_msupplier2_mpart3,
             count_mpart3,
             count_mpart3_l2_1,
             nation,
             region]

###### Query Result

# The keys to the key record here probably shouldn't be strings
result_type = {record({"S_ACCTBAL": "double",
                       "S_NAME": "VarChar<63>",
                       "N_NAME": "VarChar<63>",
                       "P_PARTKEY": "long",
                       "P_MFGR": "VarChar<63>",
                       "S_ADDRESS": "VarChar<63>",
                       "S_PHONE": "VarChar<63>",
                       "S_COMMENT": "VarChar<63>",
                       "count": "long"}): bool}

######

@final_result(result_type)
def get_snapshot():
    return result_type

####### Triggers (Ensure these are in the same order as in the M3 file) (if poss make 1 function)

@m3_trigger("insert", NATION_entry_type, nation)
def insert_nation_nation(nation_entry):
    updateMap(nation, (getStream("nation_entry.nationkey"), getStream("nation_entry.name"), getStream("nation_entry.regionkey"), getStream("nation_entry.comment")), 1)

@m3_trigger("insert", REGION_entry_type, region)
def insert_region_region(region_entry):
    updateMap(region, (getStream("region_entry.regionkey"), getStream("region_entry.name"), getStream("nation_entry.comment")), 1)

@agg_sum({"count_mpart3_l2_1": count_mpart3_l2_1, "p": (record({
    's_name': 'VarChar<63>',
    's_address': 'VarChar<63>',
    's_phone': 'VarChar<63>',
    's_acctbal': 'double',
    's_comment': 'VarChar<63>',
    'count_mpartpart_partkey': 'long',
    'ps_supplycost': 'double',
    'n_name': 'VarChar<63>'
}), "long")}, PART_entry_type, int)
def aggsum2(p):

    thing = getMap(count_mpart3_l2_1).sum(lambda q:
                        q[1]
                        if
                            q[0].count_mpartpart_partkey == getStream("part_entry.partkey") and p[0].ps_supplycost > q[0].ps2_supplycost
                        else
                            0
                        )
    
    results = thing
    
    return results

@agg_sum({"count_mpart3": count_mpart3, "count_mpart3_l2_1": count_mpart3_l2_1, "count": count},
         PART_entry_type, int)
def aggsum1():

    agg1 = getMap(count_mpart3).sum(lambda p:
                            updateMap(count, (p[0].s_acctbal,
                                              p[0].s_name,
                                              p[0].n_name,
                                              getStream("part_entry.partkey"),
                                              getStream("part_entry.mfgr"),
                                              p[0].s_address,
                                              p[0].s_phone,
                                              p[0].s_comment),
                                      p[1])
                            if
                                aggsum2(p) == 0 and p[0].count_mpartpart_partkey == getStream("part_entry.partkey")
                            else
                                0
                            )
    results = agg1
    return results

@m3_trigger("insert", PART_entry_type, count)
def insert_part_count(part_entry):
    agg1 = aggsum1()
    return agg1

@m3_trigger("insert", PART_entry_type, count_mpartsupp2)
def insert_part_count_mpartsupp2(part_entry):
    
    updateMap(count_mpartsupp2, (getStream("part_entry.partkey"), getStream("part_entry.mfgr")), 1) if (regex_match('^.*BRASS$', getStream("part_entry.type")) and getStream("part_entry.size") == 15) else 0

@agg_sum({"count_mpart3": count_mpart3,
          "count_msupplier1": count_msupplier1},
          PART_entry_type, int)
def agg1_5():
    result = getMap(count_mpart3).sum(lambda p:
                          updateMap(count_msupplier1, (getStream("part_entry.partkey"),
                                                       getStream("part_entry.mfgr"),
                                                       p[0].s_name,
                                                       p[0].s_address,
                                                       p[0].s_phone,
                                                       p[0].s_acctbal,
                                                       p[0].s_comment,
                                                       p[0].ps_supplycost,
                                                       p[0].n_name), p[1])
                          if
                            p[0].count_mpartpart_partkey == getStream("part_entry.partkey")
                          else
                            0
                          )
    return result

@m3_trigger("insert", PART_entry_type, count_msupplier1)
def insert_part_count_msupplier1(part_entry):
    s1 = agg1_5() if (regex_match('^.*BRASS$', getStream("part_entry.type")) and getStream("part_entry.size") == 15) else 0
    return s1

@agg_sum({"count_msupplier1_msupplier2_mpart3": count_msupplier1_msupplier2_mpart3,
          "count_msupplier1_msupplier2": count_msupplier1_msupplier2},
          PART_entry_type, int)
def agg1_7():
    result = getMap(count_msupplier1_msupplier2_mpart3).sum(lambda p:
                                updateMap(count_msupplier1_msupplier2, (getStream("part_entry.partkey"),
                                                                        getStream("part_entry.mfgr"),
                                                                        p[0].count_msupplier1_msuppliersupplier_suppkey,
                                                                        p[0].ps_supplycost),
                                            p[1])
                                if
                                    p[0].count_msupplier1_msupplier2_mpartpart_partkey == getStream("part_entry.partkey")
                                else
                                    0
                                )
    return result

@m3_trigger("insert", PART_entry_type, count_msupplier1_msupplier2)
def insert_part_count_msupplier1_msupplier2(part_entry):
    s1 = agg1_7() if (regex_match('^.*BRASS$', getStream("part_entry.type")) and getStream("part_entry.size") == 15) else 0
    return s1

@m3_trigger("insert", SUPPLIER_entry_type, count_mpartsupp1_l2_3)
def insert_supplier_count_mpartsupp1_l2_3(supplier_entry):
    
    updateMap(count_mpartsupp1_l2_3, getStream("supplier_entry.suppkey"), getValueOrDefault(count_mpartsupp1_l2_3_msupplier1, (getStream("supplier_entry.suppkey"))))

@agg_sum({"count_mpartsupp7_msupplier1": count_mpartsupp7_msupplier1, "count_mpartsupp7": count_mpartsupp7},
         SUPPLIER_entry_type, int)
def agg1_8():
    result = getMap(count_mpartsupp7_msupplier1).sum(lambda p:
                                         updateMap(count_mpartsupp7, (getStream("supplier_entry.suppkey"),
                                                                           getStream("supplier_entry.name"),
                                                                           getStream("supplier_entry.address"),
                                                                           getStream("supplier_entry.phone"),
                                                                           getStream("supplier_entry.acctbal"),
                                                                           getStream("supplier_entry.comment"),
                                                                           p[0].n_name),
                                                   p[1])
                                         if
                                            p[0].count_mpartsupp7_msuppliersupplier_nationkey == getStream("supplier_entry.nationkey")
                                         else
                                            0
                                         )
    return result

@m3_trigger("insert", SUPPLIER_entry_type, count_mpartsupp7)
def insert_supplier_count_mpartsupp7(supplier_entry):
    s1 = agg1_8()
    return s1

@agg_sum({"count_msupplier1_msupplier2": count_msupplier1_msupplier2, "count_msupplier1": count_msupplier1, "supplier_entry_type": SUPPLIER_entry_type, "p": (record({"count_mpartsupp7_msuppliersupplier_nationkey": "long", "n_name": "VarChar<63>"}), "long")},
         SUPPLIER_entry_type, int)
def agg2_5(p):
    result = getMap(count_msupplier1_msupplier2).sum(lambda q:
                                                  updateMap(count_msupplier1, (q[0].p_partkey,
                                                                                    q[0].p_mfgr,
                                                                                    getStream("supplier_entry.name"),
                                                                                    getStream("supplier_entry.address"),
                                                                                    getStream("supplier_entry.phone"),
                                                                                    getStream("supplier_entry.acctbal"),
                                                                                    getStream("supplier_entry.comment"),
                                                                                    q[0].ps_supplycost,
                                                                                    p[0].n_name), p[1] * q[1])
                                                if
                                                    getStream("supplier_entry.suppkey") == q[0].count_msupplier1_msuppliersupplier_suppkey
                                                else
                                                    0
                                                )
    return result

@agg_sum({"count_mpartsupp7_msupplier1": count_mpartsupp7_msupplier1}, SUPPLIER_entry_type, int)
def agg2_6():
    result = getMap(count_mpartsupp7_msupplier1).sum(lambda p:
                                          agg2_5(p)
                                          if
                                            p[0].count_mpartsupp7_msuppliersupplier_nationkey == getStream("supplier_entry.nationkey")
                                          else
                                            0
                                          )
    return result

@m3_trigger("insert", SUPPLIER_entry_type, count_msupplier1)
def insert_supplier_count_msupplier1(supplier_entry):
    result = agg2_6()
    return result

@agg_sum({"count_msupplier1_msupplier2_mpart3": count_msupplier1_msupplier2_mpart3, "count_mpart3": count_mpart3, "count_mpartsupp7_msupplier1_entry": (record({
    'count_mpartsupp7_msuppliersupplier_nationkey': 'long',
    'n_name': 'VarChar<63>'
}), "long")}, SUPPLIER_entry_type, int)
def agg3_5(count_mpartsupp7_msupplier1_entry):
    result = getMap(count_msupplier1_msupplier2_mpart3).sum(lambda q:
                                                 updateMap(count_mpart3, (getStream("supplier_entry.name"),
                                                                          getStream("supplier_entry.address"),
                                                                          getStream("supplier_entry.phone"),
                                                                          getStream("supplier_entry.acctbal"),
                                                                          getStream("supplier_entry.comment"),
                                                                          q[0].count_msupplier1_msupplier2_mpartpart_partkey,
                                                                          q[0].ps_supplycost,
                                                                          count_mpartsupp7_msupplier1_entry[0].n_name), count_mpartsupp7_msupplier1_entry[1] * q[1])
                                                 if
                                                    getStream("supplier_entry.suppkey") == q[0].count_msupplier1_msuppliersupplier_suppkey    # count_msupplier1_msuppliersupplier_suppkey
                                                 else
                                                    0
                                                 )
    return result

@agg_sum({"count_mpartsupp7_msupplier1": count_mpartsupp7_msupplier1}, SUPPLIER_entry_type, int)
def agg3_6():
    result = getMap(count_mpartsupp7_msupplier1).sum(lambda p:
                                          agg3_5(p)
                                          if
                                            p[0].count_mpartsupp7_msuppliersupplier_nationkey == getStream("supplier_entry.nationkey")
                                          else
                                            0
                                          )
    return result

@m3_trigger("insert", SUPPLIER_entry_type, count_mpart3)
def insert_supplier_count_mpart3(supplier_entry):
    result = agg3_6()
    return result

@agg_sum({"count_msupplier1_msupplier2_mpart3": count_msupplier1_msupplier2_mpart3,
          "count_mpart3_l2_1": count_mpart3_l2_1}, SUPPLIER_entry_type, int)
def agg3_7():
    result = getMap(count_msupplier1_msupplier2_mpart3).sum(lambda p:
                                               updateMap(count_mpart3_l2_1, (p[0].count_msupplier1_msupplier2_mpartpart_partkey, p[0].ps_supplycost), p[1] * getValueOrDefault(count_mpartsupp1_l2_3_msupplier1, getStream("supplier_entry.nationkey")))
                                               if
                                                getStream("supplier_entry.suppkey") == p[0].count_msupplier1_msuppliersupplier_suppkey
                                               else
                                                0
                                               )
    return result

@m3_trigger("insert", SUPPLIER_entry_type, count_mpart3_l2_1)
def insert_supplier_count_mpart3_l2_1(supplier_entry):
    a = agg3_7()
    return a

@agg_sum({"count_mpart3_l2_1": count_mpart3_l2_1,
          "count_msupplier1_entry": (record({"p_partkey": "long",
                                             "p_mfgr": "VarChar<63>",
                                             "s_name": "VarChar<63>",
                                             "s_address": "VarChar<63>",
                                             "s_phone": "VarChar<63>",
                                             "s_acctbal": "double",
                                             "s_comment": "VarChar<63>",
                                             "ps_supplycost": "double",
                                             "n_name": "VarChar<63>"}), "long")},
            SUPPLIER_entry_type, int)
def agg4(count_msupplier1_entry):
    result = getMap(count_mpart3_l2_1).sum(lambda r:
                                r[1]
                                if
                                    r[0].count_mpartpart_partkey == count_msupplier1_entry[0].p_partkey and count_msupplier1_entry[0].ps_supplycost < r[0].ps2_supplycost
                                else
                                    0
                                )
    return result

@agg_sum({"count_msupplier1": count_msupplier1, "count": count}, SUPPLIER_entry_type, int)
def agg4_1():
    result =  getMap(count_msupplier1).sum(lambda p:
                               updateMap(count, (p[0].s_acctbal, p[0].s_name, p[0].n_name, p[0].p_partkey, p[0].p_mfgr, p[0].s_address, p[0].s_phone, p[0].s_comment), p[1])
                               if
                                agg4(p) == 0
                               else
                                0
                               )
    return result

@m3_trigger("insert", SUPPLIER_entry_type, count)
def insert_supplier_count(supplier_entry):
    s1 = reset_map(count)
    result = agg4_1()

    return result

@agg_sum({"count_mpart3_l2_1": count_mpart3_l2_1},
    PARTSUPP_entry_type, float)
def agg10():
    result = getMap(count_mpart3_l2_1).sum(lambda r:
                                r[1]
                                if
                                    getStream("partsupp_entry.partkey") > r[0].ps2_supplycost
                                    and getStream("partsupp_entry.partkey") == r[0].count_mpartpart_partkey
                                else
                                    0
                               )
    return result

@agg_sum({"count_mpart3": count_mpart3,
          "q": (record({
            's_name': 'VarChar<63>',
            's_address': 'VarChar<63>',
            's_phone': 'VarChar<63>',
            's_acctbal': 'double',
            's_comment': 'VarChar<63>',
            'count_mpartpart_partkey': 'long',
            'ps_supplycost': 'double',
            'n_name': 'VarChar<63>'
        }), "long")},
    PARTSUPP_entry_type, int)
def agg7(q):
    result = 1 if agg10() == 0 else 0
    return result

@agg_sum({"count_mpart3_l2_1": count_mpart3_l2_1,
          "count_mpart3_entry": (record({
            's_name': 'VarChar<63>',
            's_address': 'VarChar<63>',
            's_phone': 'VarChar<63>',
            's_acctbal': 'double',
            's_comment': 'VarChar<63>',
            'count_mpartpart_partkey': 'long',
            'ps_supplycost': 'double',
            'n_name': 'VarChar<63>'
        }), "long")},
    PARTSUPP_entry_type, float)
def agg14(count_mpart3_entry):
    result = getMap(count_mpart3_l2_1).sum(lambda r:
                                r[1]
                                if
                                    count_mpart3_entry[0].ps_supplycost > r[0].ps2_supplycost
                                    and getStream("partsupp_entry.partkey") == r[0].count_mpartpart_partkey
                                else
                                    0
                                )
    return result

@agg_sum({"count_mpart3": count_mpart3,
          "count_mpartsupp7": count_mpartsupp7,
          "count_mpart3_entry": (record({
            's_name': 'VarChar<63>',
            's_address': 'VarChar<63>',
            's_phone': 'VarChar<63>',
            's_acctbal': 'double',
            's_comment': 'VarChar<63>',
            'count_mpartpart_partkey': 'long',
            'ps_supplycost': 'double',
            'n_name': 'VarChar<63>'
        }), "long"),
        "p": (record({"count_mpartsupppartsupp_partkey": "long",
                                             "p_mfgr": "VarChar<63>"}), "long")},
    PARTSUPP_entry_type, float)
def agg12(p, count_mpart3_entry):
    result1 = getMap(count_mpart3).sum(lambda r:
                            r[1]
                            if
                                count_mpart3_entry[0] == r[0] and agg14(count_mpart3_entry) == 0
                            else
                                0
                            )

    result2 = getValueOrDefault(count_mpartsupp7, (getStream("partsupp_entry.suppkey"),
                                                count_mpart3_entry[0].s_name,
                                                count_mpart3_entry[0].s_address,
                                                count_mpart3_entry[0].s_phone,
                                                count_mpart3_entry[0].s_acctbal,
                                                count_mpart3_entry[0].s_comment,
                                                count_mpart3_entry[0].n_name)) * agg7(count_mpart3_entry)

    return (result1 * (-1)) + result2

@agg_sum({"count_mpart3_l2_1": count_mpart3_l2_1,
          "count_mpart3_entry": (record({
            's_name': 'VarChar<63>',
            's_address': 'VarChar<63>',
            's_phone': 'VarChar<63>',
            's_acctbal': 'double',
            's_comment': 'VarChar<63>',
            'count_mpartpart_partkey': 'long',
            'ps_supplycost': 'double',
            'n_name': 'VarChar<63>'
        }), "long")},
    PARTSUPP_entry_type, float)
def agg13(count_mpart3_entry):
    result = getMap(count_mpart3_l2_1).sum(lambda r:
                                r[1]
                                if
                                    getStream("partsupp_entry.partkey") == r[0].count_mpartpart_partkey
                                    and count_mpart3_entry[0].ps_supplycost > r[0].ps2_supplycost
                                else
                                    0
                                )
    return result

@agg_sum({"count_mpart3": count_mpart3,
          "count": count,
          "count_mpartsupp2_entry": (record({"count_mpartsupppartsupp_partkey": "long",
                                             "p_mfgr": "VarChar<63>"}), "long")},
    PARTSUPP_entry_type, float)
def agg11(count_mpartsupp2_entry):
    result = getMap(count_mpart3).sum(lambda q:
                           updateMap(count,
                                     (q[0].s_acctbal, q[0].s_name, q[0].n_name, getStream("partsupp_entry.partkey"), count_mpartsupp2_entry[0].p_mfgr, q[0].s_address, q[0].s_phone, q[0].s_comment),
                                     count_mpartsupp2_entry[1] * (q[1] + agg12(count_mpartsupp2_entry, q)))
                           if
                            (agg13(q) + (getValueOrDefault(count_mpartsupp1_l2_3, getStream("partsupp_entry.suppkey")) if q[0].ps_supplycost > getStream("partsupp_entry.supplycost") else 0)) == 0
                           else
                           updateMap(count,
                                     (q[0].s_acctbal, q[0].s_name, q[0].n_name, getStream("partsupp_entry.partkey"), count_mpartsupp2_entry[0].p_mfgr, q[0].s_address, q[0].s_phone, q[0].s_comment),
                                     count_mpartsupp2_entry[1] * agg12(count_mpartsupp2_entry, q))
                           )
    return result

@agg_sum({"count_mpartsupp2": count_mpartsupp2}, PARTSUPP_entry_type, float)
def agg11_1():
    result = getMap(count_mpartsupp2).sum(lambda p:
                               agg11(p)
                               if
                                getStream("partsupp_entry.suppkey") == p[0].count_mpartsupppartsupp_partkey
                               else
                                0
                               )
    return result

@m3_trigger("insert", PARTSUPP_entry_type, count)
def insert_partsupp_count(partsupp_entry):
    result = agg11_1()
    return result

@agg_sum({"count_mpartsupp2": count_mpartsupp2,
          "count_msupplier1": count_msupplier1,
          "count_msupplier7_entry": (record({
        "count_mpartsupppartsupp_suppkey": "long",
        "s_name": "VarChar<63>",
        "s_address": "VarChar<63>",
        "s_phone": "VarChar<63>",
        "s_acctbal": "double",
        "s_comment": "VarChar<63>",
        "n_name": "VarChar<63>"
    }), "long")},
    PARTSUPP_entry_type, float)
def agg15(count_msupplier7_entry):
    result = getMap(count_mpartsupp2).sum(lambda q:
                               updateMap(count_msupplier1, (getStream("partsupp_entry.partkey"),
                                                            q[0].p_mfgr,
                                                            count_msupplier7_entry[0].s_name,
                                                            count_msupplier7_entry[0].s_address,
                                                            count_msupplier7_entry[0].s_phone,
                                                            count_msupplier7_entry[0].s_acctbal,
                                                            count_msupplier7_entry[0].s_comment,
                                                            getStream("partsupp_entry.supplycost"),
                                                            count_msupplier7_entry[0].n_name), count_msupplier7_entry[1] * q[1])
                               if
                                getStream("partsupp_entry.suppkey") == q[0].count_mpartsupppartsupp_partkey
                               else
                                0
                               )
    return result

@agg_sum({"count_mpartsupp7": count_mpartsupp7},
    PARTSUPP_entry_type, float)
def agg15_1():
    result = getMap(count_mpartsupp7).sum(lambda p:
                               agg15(p)
                               if
                                getStream("partsupp_entry.suppkey") == p[0].count_mpartsupppartsupp_suppkey
                               else
                                0
                               )
    return result

@m3_trigger("insert", PARTSUPP_entry_type, count_msupplier1)
def insert_partsupp_count_msupplier1(partsupp_entry):
    result = agg15_1()
    return result

@agg_sum({"count_mpartsupp2": count_mpartsupp2,
          "count_msupplier1_msupplier2": count_msupplier1_msupplier2},
    PARTSUPP_entry_type, float)
def agg15_2():
    result = getMap(count_mpartsupp2).sum(lambda p:
                               updateMap(count_msupplier1_msupplier2, (getStream("partsupp_entry.partkey"),
                                                                       p[0].p_mfgr,
                                                                       getStream("partsupp_entry.suppkey"),
                                                                       getStream("partsupp_entry.supplycost")), p[1])
                               if
                                getStream("partsupp_entry.partkey") == p[0].count_mpartsupppartsupp_partkey
                               else
                                0
                               )
    return result

@m3_trigger("insert", PARTSUPP_entry_type, count_msupplier1_msupplier2)
def insert_partsupp_count_msupplier1_msupplier2(partsupp_entry):
    result = agg15_2()
    return result

@m3_trigger("insert", PARTSUPP_entry_type, count_msupplier1_msupplier2_mpart3)
def insert_partsupp_count_msupplier1_msupplier2_mpart3(partsupp_entry):
    updateMap(count_msupplier1_msupplier2_mpart3, (getStream("partsupp_entry.partkey"), getStream("partsupp_entry.suppkey"), getStream("partsupp_entry.supplycost")), 1)

@agg_sum({"count_mpartsupp7": count_mpartsupp7,
          "count_mpart3": count_mpart3},
    PARTSUPP_entry_type, float)
def agg15_3():
    result = getMap(count_mpartsupp7).sum(lambda p:
                               updateMap(count_mpart3, (p[0].s_name,
                                                        p[0].s_address,
                                                        p[0].s_phone,
                                                        p[0].s_acctbal,
                                                        p[0].s_comment,
                                                        getStream("partsupp_entry.partkey"),
                                                        getStream("partsupp_entry.supplycost"),
                                                        p[0].n_name), p[1])
                               if
                                getStream("partsupp_entry.suppkey") == p[0].count_mpartsupppartsupp_suppkey
                               else
                                0
                               )
    return result

@m3_trigger("insert", PARTSUPP_entry_type, count_mpart3)
def insert_partsupp_count_mpart3(partsupp_entry):
    result = agg15_3()
    return result

@m3_trigger("insert", PARTSUPP_entry_type, count_mpart3_l2_1)
def insert_partsupp_count_mpart3_l2_1(partsupp_entry):
    updateMap(count_mpart3_l2_1, (getStream("partsupp_entry.partkey"), getStream("partsupp_entry.supplycost")), getValueOrDefault(count_mpartsupp1_l2_3, (getStream("partsupp_entry.suppkey"))))

@agg_sum({"region": region,
          "nation_entry": (record({"nationkey": "long",
                                   "name": "VarChar<63>",
                                   "regionkey": "long",
                                   "comment": "VarChar<63>"}), "long")}, None, float)
def agg16(nation_entry):
    result = getMap(region).sum(lambda q:
                     nation_entry[1] * q[1]
                     if
                        q[0].regionkey == nation_entry[0].regionkey
                        and q[0].name == "EUROPE"
                     else
                        0
                     )
    return result

@agg_sum({"nation": nation, "count_mpartsupp1_l2_3_msupplier1": count_mpartsupp1_l2_3_msupplier1}, None, float)
def agg16_5():
    result = getMap(nation).sum(lambda p:
                     updateMap(count_mpartsupp1_l2_3_msupplier1, p[0].nationkey, agg16(p))
                     )
    return result

@m3_trigger("system_ready", None, count_mpartsupp1_l2_3_msupplier1)
def system_ready_count_mpartsupp1_l2_3_msupplier1(partsupp_entry):
    result = agg16_5()
    return result

@agg_sum({"region": region,
          "count_mpartsupp7_msupplier1": count_mpartsupp7_msupplier1,
          "nation_entry": (record({"nationkey": "long",
                                   "name": "VarChar<63>",
                                   "regionkey": "long",
                                   "comment": "VarChar<63>"}), "long")}, None, float)
def agg17(nation_entry):
    result = getMap(region).sum(lambda q:
                     updateMap(count_mpartsupp7_msupplier1, (nation_entry[0].nationkey, nation_entry[0].name), nation_entry[1] * q[1])
                     if
                        nation_entry[0].regionkey == q[0].regionkey
                        and q[0].name == "EUROPE"
                     else
                        0
                     )
    return result

@agg_sum({"nation": nation}, None, float)
def agg17_5():
    result = getMap(nation).sum(lambda p:
                     agg17(p)
                     )
    return result

@m3_trigger("system_ready", None, count_mpartsupp7_msupplier1)
def system_ready_count_mpartsupp7_msupplier1(partsupp_entry):
    result = agg17_5()
    return result

####### IVM Logic

@event_processor({"event_types": event_types, "map_types": map_types})  # turn this into regular args
def process_stream_event(ins_or_del, event_type, event_params):
    if ins_or_del == "i":
        results = "None1"
    else:
        results = "hello world"    
    return results  

######### Function Calls

ress = []
def create_streams(event_type, event_stream):
    stream = []
    file_path = event_type[0] + '.csv'
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
                if list(event_type[1].values())[i] == "long":
                    event_params.append(int(stream_event[i]))
                elif list(event_type[1].values())[i] == "double":
                    event_params.append(float(stream_event[i]))
                else:
                    event_params.append(stream_event[i][:60])
            args = [ins_or_del, event_type[0], event_params]
            event_stream.append(args)
    
    return event_stream

# Calculate tables first
event_stream = []
for event_type in [("nation", NATION_entry_type), ("region", REGION_entry_type)]:
    event_stream = create_streams(event_type, event_stream)

for args in event_stream:
    q2_test_all_compiled.process_stream_event_compiled(*args)

q2_test_all_compiled.on_system_ready_compiled()

event_stream = []
for event_type in [("part", PART_entry_type), ("supplier", SUPPLIER_entry_type), ("partsupp", PARTSUPP_entry_type)]:
    event_stream = create_streams(event_type, event_stream)

f = open("q2updateTimes.txt", 'w')
random.shuffle(event_stream)
start_time = time.perf_counter()
process_time = 0.0
for args in event_stream:
    process_start = time.perf_counter()
    thing = q2_test_all_compiled.process_stream_event_compiled(*args)
    ress.append(q2_test_all_compiled.view_snapshot_compiled())
    process_end = time.perf_counter()
    f.write(str(process_end - process_start) + "\n")
    process_time += process_end - process_start

f.close()
thing2 = q2_test_all_compiled.view_snapshot_compiled()
end_time = time.perf_counter()
# print(random.choice(ress))
print(thing)
print(thing2)
time_taken = end_time - start_time
print("Time taken: " + str(time_taken))
print("Process time: " + str(process_time))
print("Processed: " + str(len(event_stream)))
