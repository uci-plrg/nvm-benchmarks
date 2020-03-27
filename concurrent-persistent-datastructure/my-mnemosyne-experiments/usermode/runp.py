import os
import time
for x in range(50):
    print('run --'.format(x))
    os.system("./build/examples/fine-grained-plist/coarse-grained-plist")
    time.sleep(1)
