# Delta SDQL.py

This project is an implementation of a delta query processor in Python

Use the following procedure to install <b>delta_sdqlpy</b> and run its test suite:

### Required Linux packages:
```
sudo su
apt-get install libssl-dev openssl  
apt install libtbb-dev
exit
```

### Python dependencies installation:
```
python3 -m pip install pip  
pip3 install numpy==1.22.0  
```
### Installation of delta sdqlpy:
```
git clone https://github.com/jamesamaxwell/delta_sdqlpy
cd delta_sdqlpy/src  
python3 setup.py build
sudo su
python3 setup.py install  
```
### Testing TPCH queries:
```
cd ../test  
```
At this point, the tests can be run with:
```
python3 q1_test_all.py  
```

These tests use sample TPC-H data taken from the following DBToaster Test Data repository: https://github.com/dbtoaster/dbtoaster-experiments-data/tree/master/tpch/standard

The tests read this data and add individual entries one at a time to test the speed it takes Delta SDQL.py to process and return the correct query. Results are compared to the results given by the same queries and test data on the DBToaster system.
