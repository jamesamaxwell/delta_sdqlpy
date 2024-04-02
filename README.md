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
### Installation of sdqlpy:
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
