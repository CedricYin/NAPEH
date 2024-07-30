<h1 align="center">Viper: An Efficient Hybrid PMem-DRAM Key-Value Store</h1>
<p align="center">This repository contains the code to our <a href="https://hpi.de/fileadmin/user_upload/fachgebiete/rabl/publications/2021/viper_vldb21.pdf"> VLDB '21 paper<a/>.<p/>


### Using Napeh
Napeh is an embedded header-only key-value store for persistent memory.
You can download it and include it in your application(check out the [Downloading Napeh](#downloading-Napeh) and [Dependencies](#dependencies) sections below).





### Downloading Napeh
You can download Napeh code from https://github.com/ncic-hpdp/napeh.git
As Napeh is header-only, you only need to download the header files and include them in your code as shown above.
Just make sure you have the [dependencies](#dependencies) installed.
There is a simple performce test example in the sub-directory "build_test" , you can run this test follow the step below:
We will provide complete benchmark code soon.

# mount the NVM data pool in the pararent directory /mnt 
mount  -o dax /dev/pmem0.0 /mnt/pm0
mount  -o dax /dev/pmem1.0 /mnt/pm0
.....

#build the test code
cd build_test
make clean && make
./ test_NAPEH  test_KV_count    thread_count  need_load  load_file

The command parameter parsed below:

test_KV_count:  The KV item count operated in this test.
thread_count:  The count of concurrent work threads for this test 
need_load :  If set to 0 , the KV item will be genarated automatically. Otherwise  the test data will be input from a load file
load_file_name : The file which contain all test data for this test.      




  
### Dependencies
Napeh depends on [concurrentqueue 1.0.3](https://github.com/cameron314/concurrentqueue).

### Acknowlegement

Thanks for the work of Viper (https://hpi.de/fileadmin/user_upload/fachgebiete/rabl/publications/2021/viper_vldb21.pdf),
which enable our work implement based on the Viper storage framework and achieves IO throughput performance optimization 
by leveraging  the  NUMA_AWARE  NVM data allocation strategy  and the method of merging small-scale metadata operations.
﻿

### Cite Our Work
If you use Napeh  in your work, please cite us.

```
