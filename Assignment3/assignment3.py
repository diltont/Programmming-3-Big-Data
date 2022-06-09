import matplotlib
import pandas as pd
import matplotlib.pyplot as plt
path='~/Programming3/Assignment3/output/'
file='timings.txt'

timings=pd.read_csv(path+file,sep=' ',header=None)


import matplotlib.pyplot as plt

plt.figure()
plt.plot(timings[0])
plt.xlabel('Core number')
plt.ylabel('Time taken')
plt.show()
plt.savefig("output/timings.png")