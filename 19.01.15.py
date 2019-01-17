# -*- coding: utf-8 -*-
"""
Created on Tue Jan 15 10:14:56 2019

@author: hyokyeong
"""


# conda install dask
# conda install -c conda-forge dask-tensorflow
# conda install distributed -c conda-forge



# =============================================================================
#
from dask import delayed, compute
import dask

@delayed # 장식자
def square(num):
    print("square fn:", num)
    print()
    return num*num

@delayed # 장식자
def sum_list(args):
    print("sum_list fn:", args)
    return sum(args)

items = [1,2,3]

# 여기서 함수를 호출했으니까 실행이 되야 하는데 장식자를 달아서 실행 안되게 함.
computation_graph = sum_list([square(i) for i in items])

computation_graph.visualize()
print("Result", computation_graph.compute()) # session과 같은 역할


# dask-scheduler 해서 ip주소 확인해주고
# 프롬프트 원하는 개수만큼 열어서 dask-worker 192.168.0.73:8786 이런식으로 쳐주기


## 
from dask.distributed import Client
client = Client('192.168.0.73:8786')
def square(x):
    return x**2
def neg(x):
    return -x
A = client.map(square, range(10))
B = client.map(neg, A)
total = client.submit(sum, B)
total.result()
total.gather()
client.restart()


# 대용량 데이터 처리(배열처리) : 3.6버전에서 실행할 것 environment 환경에서 실행
import dask.array as da
import numpy as np
arr = np.random.randint(1, 1000, (10000, 10000))
darr = da.from_array(arr, chunks=(1000,1000))

print(darr.shape)

result=darr.compute()
result


# 데이터 프레임 처리 # 블록사이즈만큼만 읽어들여서 처리해준다.
import dask.dataframe as dd
df = dd.read_csv("wine-quality.csv", blocksize=50e6)
agg = df.groupby(['fixed acidity']).aggregate(["sum", "mean", "max", "min"])


agg.compute().head()

# =============================================================================







