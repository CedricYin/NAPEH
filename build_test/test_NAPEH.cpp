#include <iostream>
#include <thread>
#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <pthread.h>
#include <unistd.h>
#include <sched.h>


#include <numa.h>
#include "benchmark.hpp"
#include "napeh.hpp"
#define MAX_V_CLIENT_CNT 100
using namespace std;

namespace viper{
namespace kv_bm{

int need_load = 0;
int rw_sign = 5;
static uint64_t seed_[50];
static int aff_mask [50][50];

inline int set_cpu_to_thread(int i)  
{  
    cpu_set_t mask;  
    CPU_ZERO(&mask);  
  
    CPU_SET(i,&mask);  
  
    // this function is not useful for high performance !!
    printf("thread %u, i = %d\n", pthread_self(), i);  
    if(-1 == pthread_setaffinity_np(pthread_self() ,sizeof(mask),&mask))  
    {  
        fprintf(stderr, "pthread_setaffinity_np erro\n");  
        return -1;  
    }  
    return 0;  
}

inline void bind_thread_to_numa(int i)
{
	int node_id = 1-((i % 40)&0x01); // odd thread bind to node 0, even thread bind to node 1
	struct bitmask *bm = numa_allocate_cpumask();
    	numa_bitmask_setbit(bm, node_id);
    	numa_bind(bm);
    	numa_free_cpumask(bm);
	return ;
}

 inline int get_cpu_aff_thread(int i)  
{  
    cpu_set_t mask;  
    cpu_set_t this_mask;  
    CPU_ZERO(&this_mask);  
    CPU_ZERO(&mask);  
	int cpu_id = i;
	int nnn = i%40;
    //return 0;
	//if(i >= 40)
	//	return 0;
	if(nnn & 0x01)
		cpu_id = nnn -1;
	else
		cpu_id = nnn +1;
    CPU_SET(cpu_id,&mask);
	
	long nproc = sysconf(_SC_NPROCESSORS_ONLN);  
  
    // this function is not useful for high performance !!
    //printf("thread %u, i = %d\n", pthread_self(), i);

	if(-1 == sched_setaffinity(0 ,sizeof(mask),&mask))  
    {  
        fprintf(stderr, "pthread_setaffinity_ erro\n");  
        return -1;  
    }  
    if(-1 == sched_getaffinity(0 ,sizeof(this_mask),&this_mask))  
    {  
        fprintf(stderr, "pthread_setaffinity_np erro\n");  
        return -1;  
    } 
	//printf("thread %d , nproc %d\n", i ,nproc);  
	for(int n = 0 ; n<nproc; n++)
		aff_mask[i][n] = CPU_ISSET(n, &mask);
	//printf("*****\n");  
    return 0;  
}  

uint64_t get_rnd(int index)
{
	seed_[index] = (uint64_t)(seed_[index]*24681357ul + 268157)&0x0ffffffff;
	return seed_[index];
}

uint64_t get_random(uint64_t seed)
{
	return ((seed*24681357ul + 268157)&0x0ffffffff);
}


//}
//}	


void test_insert_and_search_new(uint64_t count,uint64_t thread_cnt , size_t init_size, pid_t wait_pid, int order, char * load_file, int need_load)
{

    // new , use create method , create a new viper db for test. 
    //auto viper_db = viper::Viper<uint64_t, SignedValue224>::create("/mytestpmem/viper", init_size);
    //auto viper_db = viper::Viper<uint64_t, SignedValue8>::create("/mnt/pm1/viper", init_size);
    auto viper_db = viper::Viper<uint64_t, SignedValue16>::create("/mnt/pm1/viper", init_size);
    uint64_t insert_start_times[thread_cnt];
    //auto v_client = viper_db->get_client();
    int ins_cnt = 0;
    struct timeval tv, tv1;
    uint64_t start, end;
    int block_cnt = 0;

    uint64_t * key_array ,*rand_array ;
    uint64_t m = 0;
    vector<thread> insertingThreads;
    vector<thread> searchingThreads;

    vector<thread> updatingThreads;
    vector<thread> rereadingThreads;
    key_array = (uint64_t*)malloc(count * sizeof(uint64_t));
    rand_array = (uint64_t*)malloc(count * sizeof(uint64_t));
    if(key_array == NULL){
	    printf("Cannot Alloc key_array\n");
	    return ;
    }
    gettimeofday(&tv,NULL);
    start = (uint64_t)tv.tv_sec*1000000+tv.tv_usec;
    if(!need_load){
	printf("generate random data ....\n");
    	for (uint64_t  m = 0; m<count ; m++){
	    key_array[m] = (uint64_t)random();
    	}
    }else{
	    char buf[128];
	    
	    uint64_t cur_key;
	    char * pckey;
	    size_t len;
	    FILE * fp = NULL;
	    fp = fopen(load_file, "r");
	    if(fp == NULL){
		    printf("can not open datafile %s \n",load_file);
		    exit(-2);
	    }
            
	    printf("laad data from file ....\n");
	    for (m = 0; m<count ; m++){
		    //TODO : get key from data file
		    memset(buf,0,128);
		    if(fgets(buf,128,fp)!= NULL){
		     memcpy((char*)(&cur_key),buf+7, 8);
		     key_array[m] = cur_key;
		     //if(m<20)
			     //printf("key[%d] = %llx\n", m, key_array[m]);
		    }else{
		      printf("can not read data file from line %lu\n",m);
		      exit(-3);
		    }
	    	   rand_array[m] = (uint64_t)random();
	     }

    }
    gettimeofday(&tv,NULL);
    end = (uint64_t)tv.tv_sec*1000000+tv.tv_usec-start;
    printf("generate %lu random item , use time %lu us, pid %d\n", m, end, wait_pid );
    block_cnt = count/thread_cnt;
	for(int num = 0; num<thread_cnt; num++){
	    //v_client_array[num] = viper_db->get_client();

    	}	
    auto put = [&viper_db, &ins_cnt, &insert_start_times, &key_array, &need_load](int from, int to, int thread_num){
	    struct timeval tv1, tv2;
	    uint64_t last_insert = 0; 
	    uint64_t get_cnt = 0;
	    uint64_t seed = (uint64_t)from; 
	    //set_cpu_to_thread((thread_num % 20));
    	    auto v_insert_client = viper_db->get_run_client(thread_num,0);
	    //v_insert_client.client_id = thread_num;
	    // changed by mayl
	    bind_thread_to_numa(thread_num);

	    //if(rw_sign >0)
		//get_cpu_aff_thread(thread_num);
	    gettimeofday(&tv1,NULL);
	    insert_start_times[thread_num] = ((uint64_t)(tv1.tv_sec))*1000000 + tv1.tv_usec;
	    //printf("inserting thread %d start %llu.%llu\n",tv1.tv_sec, tv1.tv_usec );
	    for(uint64_t n = from; n<to ; n++){
        	 //uint64_t value = (n<<32) + n+10;
        	 const SignedValue16 value((n<<32) + n+10) ;
		 SignedValue16 value1;
			 //value.set_fvalue((uint64_t)(n<<32) + n+10);
			

		    //const std::string value(200,'y');
		 bool ret ;
		 uint64_t r_key = from + (key_array[n] % (to-from));
		 r_key = (r_key<<32)+r_key;
		 //const KeyType8  p_key  {(n<<32)+n};
		  uint64_t  p_key  = (n<<32)+n;
		 if(need_load)
			 p_key = key_array[n];

		 //const uint64_t  s_key  = (r_key<<32)+r_key;
		 //const SignedValue224 value {(n<<32)+n+10};
		 //TODO: new v_client for each thread
		 //printf("ins_cnt = %d, pkey = %lx\n", ins_cnt, p_key);
		if(1){
				
			//if(n-from <10 )
			//	printf("p_key %lx\n", p_key);
			// for workloadd
			//if(rw_sign > 0){
			//if(get_rnd(thread_num %40)%100<rw_sign){
			//seed = get_random(seed);
			//seed = 10;
			//seed = get_rnd(thread_num %40);
			if((seed%100)<rw_sign){
				//if(n<from+10)
				//	printf("play insert %lu item, key %llx\n", n,p_key);
        			ret = v_insert_client->put(p_key, value);
				//last_insert = p_key;
				last_insert = 0;
			}
			else{
				//if(last_insert != 0)
        			//	ret = v_insert_client.get(last_insert, &value1);
				//else

        			        //ret = v_insert_client.get(r_key, &value1);
        			        ret = v_insert_client->get(p_key, &value1);
				//last_insert = 0;
				//get_cnt ++;

			}

		}
		else {
        		ret = v_insert_client->put(r_key, value);
		}
		 //printf("Ins/update %x, key %llx , ret %d \n", ins_cnt,(n<<32)+n,ret );
        	//v_client_array[thread_num].put(p_key, value);
		//ins_cnt++;
	    }
		
	    gettimeofday(&tv2,NULL);
		uint64_t utime = (tv2.tv_sec *1000000 + tv2.tv_usec)-tv1.tv_sec * 1000000 - tv1.tv_usec;
		//printf("insert time  %lu usec\n\n", utime);
	    //printf("workload , getcnt %lu, client_id %d \n", get_cnt,  v_insert_client.client_id);
		


     };

	auto remove = [&viper_db, &ins_cnt, &insert_start_times, &key_array](int from, int to, int thread_num){
	    struct timeval tv1, tv2;
	    //set_cpu_to_thread((thread_num % 20));
    	auto v_insert_client = viper_db->get_run_client(thread_num,0);
	    v_insert_client->client_id = thread_num;
	    
	    gettimeofday(&tv1,NULL);
		get_cpu_aff_thread(thread_num);
	    insert_start_times[thread_num] = ((uint64_t)(tv1.tv_sec))*1000000 + tv1.tv_usec;
	    //printf("inserting thread %d start %llu.%llu\n",tv1.tv_sec, tv1.tv_usec );
	    for(uint64_t n = from; n<to ; n++){
        	 //uint64_t value = (n<<32) + n+10;
        	 const SignedValue16 value((n<<32) + n+10) ;
			 //value.set_fvalue((uint64_t)(n<<32) + n+10);
			

		    //const std::string value(200,'y');
		 bool ret ;
		 uint64_t r_key = from + (key_array[n] % (to-from));
		 //const KeyType8  p_key  {(n<<32)+n};
		 const uint64_t  p_key  = (n<<32)+n;
		 const uint64_t  s_key  = (r_key<<32)+r_key;
		 //const SignedValue224 value {(n<<32)+n+10};
		 //TODO: new v_client for each thread
		if(ins_cnt != -1)
        		ret = v_insert_client->remove(p_key);
		else
        		ret = v_insert_client->remove(s_key);
		if(!ret)
			printf("remove failed at key %lu, %lu\n", p_key, s_key);
		 //printf("Ins/update %x, key %llx , ret %d \n", ins_cnt,(n<<32)+n,ret );
        	//v_client_array[thread_num].put(p_key, value);
		//ins_cnt++;
	    }
		
	    //gettimeofday(&tv2,NULL);
		//uint64_t utime = (tv2.tv_sec *1000000 + tv2.tv_usec)-tv1.tv_sec * 1000000 - tv1.tv_usec;
		//printf("insert time  %lu usec\n\n", utime);


     };

	
    rw_sign = 100;
    gettimeofday(&tv,NULL);

    start = (uint64_t)tv.tv_sec*1000000+tv.tv_usec;
    for(int j = 0; j < thread_cnt; j++){
	insertingThreads.emplace_back(thread(put, block_cnt*j, block_cnt*(j+1),j));
    }
    for(auto& th: insertingThreads) th.join();
    gettimeofday(&tv,NULL);
    end = (uint64_t)tv.tv_sec*1000000 +tv.tv_usec-start;
    printf("Insert %lu item , use time %lu us, pid %d, inscnt %d, %lu Kops\n", count, end, wait_pid, ins_cnt , (uint64_t)count*1000/(uint64_t)end);

     rw_sign = 0;
     sleep(3);
     //exit(0);
     printf("goto readlast workloadc\n");
     for(int jj = 0 ; jj<40  ; jj++){
	     seed_[jj] = 11335577ul;
     }
     	
			
	//}
	//exit(0); // mayl for workloadd
    
   usleep(1000); 
	 //return ;
    //auto v_snap_client = viper_db->get_client();
   
    //size_t snap_cnt =v_snap_client.snap();
    if(wait_pid){
	    usleep(1000*1000*3);
    }

    //return ;
    auto get = [&viper_db, &ins_cnt, &insert_start_times, &key_array, &need_load](int from, int to, int thread_num){
	    struct timeval tv1;
	    //set_cpu_to_thread((thread_num % 20));
    	    auto v_search_client = viper_db->get_run_client(thread_num, 0 );

	     //get_cpu_aff_thread(thread_num);
	     bind_thread_to_numa(thread_num);
	     v_search_client->client_id = thread_num ;
	    gettimeofday(&tv1,NULL);
	    int get_cnt = 0;
	    uint64_t seed_g = 11335577ul;
	    uint64_t last_key = -1;
	    rw_sign = 0;
            const SignedValue16  value_c((uint64_t)10);
	    //insert_start_times[thread_num] = ((uint64_t)(tv1.tv_sec))*1000000 + tv1.tv_usec;
	    //printf("searching thread %d start %llu.%llu, %lu->%lu\n",thread_num, tv1.tv_sec, tv1.tv_usec, from, to );
	    for(uint64_t n = from; n<to ; n++){
         	SignedValue16  value;

		 //uint64_t value;
		 //uint64_t a_key = from + ((random())%(to -from));
		 //uint64_t a_key = (n<<32)+n;
		 uint64_t a_key = from + (key_array[n] % (to-from));
		 a_key = (a_key<<32)+a_key;
		 if(need_load){
			 //uint64_t index = rand_array[n] %(to - from);
			 //uint64_t index = n;
			 a_key = key_array[n];
		 }
		 //a_key = (a_key<<32)+a_key;
		 //uint64_t r_key = n;
		 //s_key =  (s_key<<32)+s_key;
		 //const KeyType8  s_key {(r_key<<32)+r_key};
		 //const uint64_t  s_key {(r_key<<32)+r_key};
		 //const SignedValue8 value1 {(r_key<<32)+r_key+10};
		 //TODO: new v_client for each thread
			//if(n-from <10 )
			//	printf("g_key %lx\n", a_key);
		//if((get_rnd(thread_num %40)%100) >  rw_sign){
		seed_g = (seed_g*24681357ul + 268157)&0x0ffffffff;
		if(seed_g %100 >=rw_sign){
			//if(last_key != ((uint64_t)-1))
        		//	v_search_client.get(last_key, &value);
			//else
        		auto ret1 = v_search_client->get(a_key, &value);
			//last_key = (uint64_t)-1;

			 
			
			if(ret1 && (value != value_c) )
				get_cnt++;
			//last_key = 0;
		}
		else{
        		v_search_client->put(a_key, value);
			//last_key = a_key;
		}

		 //printf("get a_key %lx, value %lx\n", a_key, value.get_fvalue());
		//if (value != value1)
		//	printf("data mismatch when %x : %x :%x, key : %llx data: %llx\n", n, r_key, key_array[n], (r_key<<32)+r_key, value.get_fvalue());
        	//v_client_array[thread_num].put(p_key, value);
		//ins_cnt++;

	    }
	    printf("Gget value cnt %lu\n", get_cnt);
    };

    printf("wait_for shuffle key array\n");
    uint64_t *temp_array = (uint64_t*)malloc(sizeof(uint64_t)*count); 
    for(int j = 0 ; j< thread_cnt; j++){
	    for(int n = block_cnt*j ; n <block_cnt*(j+1); n++){
		    uint64_t index = block_cnt*j +(random()%block_cnt);
		    temp_array[n] = key_array[index];
	    }
     }
     for(int j = 0 ; j< thread_cnt; j++){
	    for(int n = block_cnt*j ; n <block_cnt*(j+1); n++){
		    //uint64_t index = n+(random()%block_cnt);
		    key_array[n] = temp_array[n];
	    }
     }

    printf(" shuffle key array complete \n");


    gettimeofday(&tv,NULL);
    start = (uint64_t)tv.tv_sec*1000000+tv.tv_usec;
    for(int j = 0; j < thread_cnt; j++){
	rereadingThreads.emplace_back(thread(get, block_cnt*j, block_cnt*(j+1),j));
    }
    for(auto& th: rereadingThreads) th.join();
    gettimeofday(&tv,NULL);
    end = (uint64_t)tv.tv_sec*1000000 +tv.tv_usec-start;
    printf("Get  %lu item , use time %lu us, pid %d, inscnt %d, %lu Kops\n", count, end, wait_pid, ins_cnt , (uint64_t)count*1000/(uint64_t)end);
    


    /*
	printf("aff mask \n");
	for (int nn = 0 ; nn<40 ; nn++){
		for(int xx = 0 ; xx <40; xx++ ){
			printf("%d ", aff_mask[nn][xx]);
		}
		printf("\n");
	}
    */
    return ;
    //exit(0);


#if 0
    gettimeofday(&tv,NULL);
    start = (uint64_t)tv.tv_sec*1000000+tv.tv_usec;
    for(int j = 0; j < thread_cnt; j++){
	searchingThreads.emplace_back(thread(get, block_cnt*j, block_cnt*(j+1),j));
    }
    for(auto& th: searchingThreads) th.join();
    gettimeofday(&tv,NULL);
    end = (uint64_t)tv.tv_sec*1000000+tv.tv_usec-start;
    printf("search %lu item , use time %lu us, pid %d , throuput %lu Kops \n", count, end, wait_pid , (uint64_t)count*1000/(uint64_t)end);
    if(wait_pid){
	    usleep(1000*1000*3);
    }
    return ;


    printf("start update out of order ..\n");
    if(!order)
    	ins_cnt = -1;
    gettimeofday(&tv,NULL);
    start = (uint64_t)tv.tv_sec*1000000+tv.tv_usec;
    for(int j = 0; j < thread_cnt; j++){
	updatingThreads.emplace_back(thread(put, block_cnt*j, block_cnt*(j+1),j));
    }
    for(auto& th: updatingThreads) th.join();
    gettimeofday(&tv,NULL);
    end = (uint64_t)tv.tv_sec*1000000 +tv.tv_usec-start;
    //printf("update disorder %lu item , use time %lu us, pid %d, inscnt %d\n", count, end, wait_pid, ins_cnt );
    
    printf("insert %lu item , use time %lu us, pid %d, inscnt %d, %lu Kops\n", count, end, wait_pid, ins_cnt , (uint64_t)count*1000/(uint64_t)end);
    if(wait_pid){
	    usleep(1000*1000*3);
    }
#endif

	printf("start get/lookup ..\n");
    if(!order)
    	ins_cnt = -1;
    gettimeofday(&tv,NULL);
    start = (uint64_t)tv.tv_sec*1000000+tv.tv_usec;
    for(int j = 0; j < thread_cnt; j++){
	updatingThreads.emplace_back(thread(get, block_cnt*j, block_cnt*(j+1),j));
    }
    for(auto& th: updatingThreads) th.join();
    gettimeofday(&tv,NULL);
    end = (uint64_t)tv.tv_sec*1000000 +tv.tv_usec-start;
    //printf("update disorder %lu item , use time %lu us, pid %d, inscnt %d\n", count, end, wait_pid, ins_cnt );
    
    printf("lookup  %lu item , use time %lu us, pid %d, inscnt %d, %lu Kops\n", count, end, wait_pid, ins_cnt , (uint64_t)count*1000/(uint64_t)end);
    if(wait_pid){
	    usleep(1000*1000*3);
    }
    printf("aff mask \n");
	for (int nn = 0 ; nn<40 ; nn++){
		for(int xx = 0 ; xx <40; xx++ ){
			printf("%d ", aff_mask[nn][xx]);
		}
		printf("\n");
	}


    //std::cout << "total No record found key count: " << not_found << std::endl;


}

}
}	

/*
 *  argv[1] count
 *  argv[2] order update
 *  argv[3] thread_cnt
 *  argv[4] is_old 
 * */
void usage_info()
{
	printf("usage: test_NAPEH  test_count    thread_count  need_load load_file_name\n ");
}

int main(int argc, char** argv) {
    unsigned long count = 0;
    int not_found = 0;
    pid_t wait_pid = 0;
    uint64_t r_key;
    int thread_cnt = 1;
    int order = 0;
    int is_old = 0;
    int need_load = 0;
    size_t init_size = 1024ul*1024ul*1024ul*100ul;


    //printf("size of size_t is %d\n", sizeof(size_t));
    //return 0;

    if(argc <2){
	    usage_info();
	    return 0;
    }

    count = atoi(argv[1]);
    if(argc >2 ){
	    wait_pid = getpid();
	    order = 1;
	    thread_cnt = atoi(argv[2])
    }
    
   
    if(argc > 3){
	    
		    need_load = atoi(argv[3]); 
		    
    }

   
    

    printf("An new built simplely NAPEH persistet KV test start..., %d\n", thread_cnt);
        	viper::kv_bm::test_insert_and_search_new(count, thread_cnt, init_size, wait_pid, order, argv[4], need_load);
    

    return 0;
}

