#ifndef IMAGINE_MAPREDUCE_REDUCER_H
#define IMAGINE_MAPREDUCE_REDUCER_H

#include<fcntl.h>
#include<atomic>
// #include<list>

#include<Imagine_Rpc/Imagine_Rpc/RpcServer.h>
#include<Imagine_Rpc/Imagine_Rpc/RpcClient.h>

#include"MapReduceUtil.h"
#include"Callbacks.h"

namespace Imagine_MapReduce{

template<typename key,typename value>
// Reducer节点
class Reducer
{

public:
    // 主节点
    class MasterNode{
        public:
            MasterNode():receive_all(false),memory_merge(true),disk_merge(true),memory_file_size(0)
            {

            }
            // 内存文件列表 进行合并，并将合并结果（合并文件名）写入磁盘文件
            void MemoryMerge()
            {
                pthread_mutex_lock(memory_list_lock);

                std::vector<std::string> merge_list;  // 合并列表
                int merge_num=memory_file_list.size();// 合并数量（键值对）
                // 存储split文件内容的链表
                for(auto it=memory_file_list.begin();it!=memory_file_list.end();it++){
                    merge_list.push_back(std::move(*it));// 将内存文件列表中的内容移动到合并列表中
                }
                memory_file_list.clear();

                std::string merge_name="memory_merge_"+MapReduceUtil::IntToString(memory_merge_id++)+".txt";// 合并文件名  
                int fd=open(&merge_name[0],O_CREAT|O_RDWR,0777); // 合并文件名对应的合并文件描述符fd==磁盘文件
                int* idxs=new int[merge_num];// 每个合并文件名的索引组

                std::priority_queue<KVReader*,std::vector<KVReader*>,KVReaderCmp> heap_;
                for(int i=0;i<merge_num;i++){
                    std::string key_;
                    std::string value_;
                    if(MapReduceUtil::ReadKVReaderFromMemory(merge_list[i],idxs[i],key_,value_)){
                        heap_.push(new KVReader(key_,value_,i));
                    }
                    // 将合并列表中的文件内容读取为键值对，并添加到优先级队列中
                }
                while(heap_.size()){
                    KVReader* next_kv=heap_.top();// 获取优先级队列中最小的键值对
                    heap_.pop();
                    std::string key_;
                    std::string value_;
                    // 继续读取下一个键值对并添加到优先级队列中
                    if(MapReduceUtil::ReadKVReaderFromMemory(merge_list[next_kv->reader_idx],idxs[next_kv->reader_idx],key_,value_))heap_.push(new KVReader(key_,value_,next_kv->reader_idx));
                    // 将堆的所有键值对 写入 磁盘文件fd==合并文件描述符中
                    MapReduceUtil::WriteKVReaderToDisk(fd,next_kv); 
                    delete next_kv;
                }

                delete [] idxs;
                close(fd);
                
                pthread_mutex_unlock(memory_list_lock);

                pthread_mutex_lock(disk_list_lock);
                disk_file_list.push_back(merge_name);// 将合并后文件名添加到磁盘文件列表中
                pthread_mutex_unlock(disk_list_lock);
            }

            void DiskMerge()
            {
                pthread_mutex_lock(disk_list_lock);

                std::vector<std::string> merge_list;
                int merge_num=disk_file_list.size();
                for(auto it=disk_file_list.begin();it!=disk_file_list.end();it++){
                    merge_list.push_back(std::move(*it));
                }
                disk_file_list.clear();

                int* fds=new int[merge_num];
                // 合并的文件组中每个文件的文件描述符
                for(int i=0;i<merge_num;i++){
                    fds[i]=open(&merge_list[i][0],O_RDWR);//打开第 i 个文件，并返回该文件的文件描述符
                }

                std::string merge_name="disk_merge_"+MapReduceUtil::IntToString(disk_merge_id++)+".txt";
                MapReduceUtil::MergeKVReaderFromDisk(fds,merge_num,merge_name);//将多个文件合并成一个文件

                disk_file_list.push_back(merge_name);// 将合并后的文件名添加到磁盘文件列表中

                for(int i=0;i<merge_num;i++){
                    close(fds[i]);// 关闭合并的文件
                    remove(&merge_list[i][0]);// 删除合并前的文件
                }
                delete [] fds;
        
                pthread_mutex_unlock(disk_list_lock);
            }

        public:
            int count=0;//计数,用于判断是否可以开始执行Reduce
            int file_num;// 源文件的数量
            std::unordered_map<std::string,std::unordered_map<std::string,int>> files;// <源文件名,splits哈希表>的文件哈希

            std::atomic<bool> receive_all;//标识所有文件是否全部接收完毕
            std::atomic<bool> memory_merge;//标识memory是否退出merge
            std::atomic<bool> disk_merge;//标识disk是否退出merge
            int memory_merge_id;
            int disk_merge_id;
            pthread_t* memory_thread;
            pthread_t* disk_thread;
            pthread_mutex_t* memory_list_lock;// 内存文件列表 的互斥锁
            pthread_mutex_t* disk_list_lock;  // 磁盘文件列表 的互斥锁
            std::atomic<int> memory_file_size;// 存储内存中储存的文件的总大小
            // int memory_file_size;          // 存储内存中储存的文件的总大小
            std::list<std::string> memory_file_list;//存储split文件内容的链表
            std::list<std::string> disk_file_list;  //存储磁盘中储存的文件的文件名的链表
    };

public:

    Reducer(const std::string& ip_, const std::string& port_, const std::string& keeper_ip_="", const std::string& keeper_port_="", ReduceCallback reduce_=nullptr);

    ~Reducer();

    void loop();//启动rpc_server的循环处理
    // 注册Reducer节点。接受输入参数input，返回注册结果
    std::vector<std::string> Register(const std::vector<std::string>& input);
    // Reduce操作
    std::vector<std::string> Reduce(const std::vector<std::string>& input);
    // 启动合并线程函数，用于启动合并线程进行Reduce结果的合并操作
    void StartMergeThread(MasterNode* master_node);
    // 写入磁盘函数，将文件内容写入磁盘
    bool WriteToDisk(const std::string& file_name, const std::string& file_content);
    // 设置默认Reduce函数的函数
    bool SetDefaultReduceFunction();

private:

    const std::string ip; //reduce节点的ip
    const std::string port;

    const std::string keeper_ip;
    const std::string keeper_port;

    ReduceCallback reduce;//Reduce回调函数

    RpcServer* rpc_server;//处理RPC请求和响应的服务器对象

    pthread_mutex_t* map_lock;//保护master_map的访问

    std::unordered_map<std::pair<std::string,std::string>,MasterNode*,HashPair,EqualPair> master_map;// 注册新的主节点
    // 每个键对应的 MasterNode主节点对象  <两个键，主节点>
    // 存储键值对的哈希表，用于管理 MasterNode 对象的集合==键的哈希和比较操作

};

template<typename key,typename value>
Reducer<key,value>::Reducer(const std::string& ip_,const std::string& port_, const std::string& keeper_ip_, const std::string& keeper_port_, ReduceCallback reduce_):
    ip(ip_),port(port_),keeper_ip(keeper_ip_),keeper_port(keeper_port_)
{   
    if(reduce_)reduce=reduce_;
    else SetDefaultReduceFunction();

    map_lock=new pthread_mutex_t;
    if(pthread_mutex_init(map_lock,nullptr)!=0){
        throw std::exception();
    }

    rpc_server=new RpcServer(ip,port,keeper_ip,keeper_port);
    rpc_server->Callee("Reduce",std::bind(&Reducer::Reduce,this,std::placeholders::_1));//注册Reduce函数作为RPC方法的回调函数
    rpc_server->Callee("Register",std::bind(&Reducer::Register,this,std::placeholders::_1));
}

template<typename key,typename value>
Reducer<key,value>::~Reducer()
{
    delete rpc_server;
    delete map_lock;
}

template<typename key,typename value>
void Reducer<key,value>::loop()
{
    rpc_server->loop();
}

template<typename key,typename value>
// 注册新的主节点到master_map，并进行相应的初始化操作，包括文件映射、线程创建和锁的初始化
std::vector<std::string> Reducer<key,value>::Register(const std::vector<std::string>& input)
{
    /*
    信息格式
        -1.源文件总数
        -2.文件名列表
        -3.master_ip
        -4.master_port
    */
    printf("This is Register Method !\n");
    // for(int i=0;i<input.size();i++)printf("%s\n",&input[i][0]);
    int new_master_file_num=MapReduceUtil::StringToInt(input[0]);// 新主节点的源文件名数量（每个源文件名对应很多分块splits文件）
    // new_master_pair == <新主节点ip,新主节点port>
    std::pair<std::string,std::string> new_master_pair=std::make_pair(input[new_master_file_num+1],input[new_master_file_num+2]);
    pthread_mutex_lock(map_lock);
    if(master_map.find(new_master_pair)!=master_map.end()){
        //重复注册 主节点
        throw std::exception();
    }
    MasterNode* new_master=new MasterNode;
    new_master->file_num=new_master_file_num; // 源文件数量  ==>每个源文件名对应很多分块splits文件(spilts哈希表)
    for(int i=0;i<new_master_file_num;i++){
        std::unordered_map<std::string,int> temp_map;
        // <splits名，数量>
        // <源文件名,splits哈希表>的文件哈希
        new_master->files.insert(std::make_pair(input[i+1],temp_map));
    }
    // 创建线程对象用于内存合并和磁盘合并
    new_master->memory_thread=new pthread_t;
    new_master->disk_thread=new pthread_t;
    new_master->memory_list_lock=new pthread_mutex_t;
    new_master->disk_list_lock=new pthread_mutex_t;
    if(pthread_mutex_init(new_master->memory_list_lock,nullptr)!=0){
        throw std::exception();
    }
    if(pthread_mutex_init(new_master->disk_list_lock,nullptr)!=0){
        throw std::exception();
    }

    StartMergeThread(new_master);//开启merge线程
    // <两个键，主节点>  <==> new_master_pair== <主节点ip,主节点port>  相当于在map哈希注册主节点
    master_map.insert(std::make_pair(new_master_pair,new_master));
    //rpc_server->SetTimer();
    pthread_mutex_unlock(map_lock);
    std::vector<std::string> output;
    output.push_back("Receive!");

    return output;
}

template<typename key,typename value>
std::vector<std::string> Reducer<key,value>::Reduce(const std::vector<std::string>& input)
{
    /*
    参数说明
        -1.split_num:文件被分成的split数
        -2.file_name:split前的文件名==源文件名
        -3.split_file_name:当前要获取的split文件名
        -4.mapper_ip:文件所在的mapper的ip
        -5.mapper_port:文件所在的mapper的port
        -6.master_ip:master的ip
        -7.master_port:master的port
    */
    printf("this is Reduce Method !\n");
    // for(int i=0;i<input.size();i++)printf("%s\n",&input[i][0]);
    int split_num=MapReduceUtil::StringToInt(input[0]);
    std::string file_name=input[1]; // 源文件名
    std::string split_name=input[2];// 分区文件名
    std::string mapper_ip=input[3];
    std::string mapper_port=input[4];
    std::pair<std::string,std::string> master_pair=std::make_pair(input[5],input[6]);

    pthread_mutex_lock(map_lock);
    typename std::unordered_map<std::pair<std::string,std::string>,MasterNode*,HashPair,EqualPair>::iterator master_it=master_map.find(master_pair);
   
    if(master_it==master_map.end()){
        //该master没有register
        throw std::exception();
    }
    // <两个键，主节点> ==> 找到MasterNode*
    MasterNode* master_node=master_it->second;
    // <源文件名,splits哈希表>的文件哈希   ==> 对应的spilt文件
    std::unordered_map<std::string,std::unordered_map<std::string,int>>::iterator file_it=master_node->files.find(file_name);
    if(file_it==master_it->second->files.end()){
        //错误的文件名
        throw std::exception();
        // std::unordered_map<std::string,int> temp_map;
        // auto temp_pair=it->second->files.insert(std::make_pair(file_name,temp_map));
        // if(temp_pair->second)file_it=temp_pair->first;
        // else throw std::exception();
    }
    // splits哈希<std::string,int>
    std::unordered_map<std::string,int>::iterator split_it=file_it->second.find(split_name);
    if(split_it!=file_it->second.end()){
        //重复接收同一个split文件
        throw std::exception();
    }
    pthread_mutex_unlock(map_lock);

    //从mapper获取 当前split文件==其中一个
    std::string method_name="GetFile";
    std::vector<std::string> parameters;
    parameters.push_back(split_name);
    // 取Mapper节点的split文件，加到MasterNode主节点的memory_file_list，同时更新memory_file_size记录内存文件总大小
    pthread_mutex_lock(master_node->memory_list_lock);// 加锁以访问 master_node 的 memory_file_list 和 memory_file_size
    // 远程调用mapper_ip和mapper_port的method_name函数（parameters参数），获取split文件，加到memory_file_list的前部
    master_node->memory_file_list.push_front(RpcClient::Call(method_name,parameters,mapper_ip,mapper_port)[0]);
    // split文件存在内容 ,输出split分区文件的内容
    if((*master_node->memory_file_list.begin()).size())printf("split file %s content : \n%s\n",&split_name[0],&(*master_node->memory_file_list.begin())[0]);
    else printf("split file %s content : NoContent!\n",&split_name[0]);
    master_node->memory_file_size+=master_node->memory_file_list.front().size();// 更新memory_file_size+加的split文件的大小
    pthread_mutex_unlock(master_node->memory_list_lock);

    pthread_mutex_lock(map_lock);
    split_it=file_it->second.find(split_name);
    if(split_it!=file_it->second.end()){
        //重复接收同一个split文件
        throw std::exception();
    }
    //   splits哈希<std::string,int> == file_it->second == 该文件的各个split文件
    file_it->second.insert(std::make_pair(split_name,1));// split_name 插到 splits哈希<std::string,int>中，并设置计数为 1
    master_it=master_map.find(master_pair);              // 主节点迭代器  == <两个键，主节点>主节点哈希
    // master_it->second == 主节点.files.find(<源文件名,splits哈希表>的文件哈希)===>splits哈希表unordered_map<std::string,int> 
    // ==> <splits文件名,数量>
    file_it=master_it->second->files.find(file_name);  // 根据源文件名file_name找splits哈希表 =======/ <源文件名,splits哈希表>的文件哈希
    if(file_it->second.size()==split_num)master_it->second->count++;// splits文件名数量 ==  分割数量 ==>主节点数++
    // master_it->second->count（已接收完所有文件的数量）是否等于 master_it->second->files.size()（文件总数）
    if(master_it->second->count==master_it->second->files.size()){
        //所有文件接收完毕,可以开始执行
        master_node->receive_all.store(true);
        while(master_node->disk_merge.load());//自旋等待,磁盘合并操作是否完成
        printf("TaskOver!\n");//任务已经完成
    }
    pthread_mutex_unlock(map_lock);

    std::vector<std::string> output;
    output.push_back("Receive!\n");

    return output;
}
// 启动内存合并和磁盘合并的线程
template<typename key,typename value>
void Reducer<key,value>::StartMergeThread(MasterNode* master_node)
{   
    // 此函数调用时机在Register的map_lock中,故不需要加锁
    // std::list<std::string>& memory_list=master_node->memory_file_list;
    // std::list<std::string>& disk_list=master_node->disk_file_list;
    // 创一个内存合并线程,并指定线程的执行函数lambda函数
    pthread_create(master_node->memory_thread,nullptr,[](void* argv)->void*{

        MasterNode* master_node=(MasterNode*)argv;
        while(!(master_node->receive_all.load())){ //尚未接收完所有文件，就继续循环
            if(master_node->memory_file_size>=DEFAULT_MEMORY_MERGE_SIZE){ // 内存中文件的总大小是否达到了默认的内存合并大小
                master_node->MemoryMerge(); // 内存合并
            }
        }
        master_node->MemoryMerge();             // 最后一次内存合并
        master_node->memory_merge.store(false); // 内存合并已经完成

        return nullptr;
    },master_node);

    pthread_detach(*(master_node->memory_thread));

    pthread_create(master_node->disk_thread,nullptr,[](void* argv)->void*{

        MasterNode* master_node=(MasterNode*)argv;
        while(master_node->memory_merge.load()){ // 内存合并是否还在进行中
            if(master_node->disk_file_list.size()>=DEFAULT_DISK_MERGE_NUM){
                master_node->DiskMerge();
            }
        }
        master_node->DiskMerge();
        master_node->disk_merge.store(false);

        return nullptr;
    },master_node);
    pthread_detach(*(master_node->disk_thread));

}
// 文件内容file_content写入磁盘file_name
template<typename key,typename value>
bool Reducer<key,value>::WriteToDisk(const std::string& file_name, const std::string& file_content)
{
    int fd=open(&file_name[0],O_CREAT|O_RDWR,0777);
    write(fd,&file_content[0],file_content.size());

    return true;
}

template<typename key,typename value>
bool Reducer<key,value>::SetDefaultReduceFunction(){
    reduce=MapReduceUtil::DefaultReduceFunction;
    return true;
}


}


#endif