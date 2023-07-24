#ifndef IMAGINE_MAPREDUCE_MAPRUNNER_H
#define IMAGINE_MAPREDUCE_MAPRUNNER_H

#include<string.h>
#include<queue>
#include <memory>
#include"RecordReader.h"
#include"Mapper.h"
#include"OutputFormat.h"
#include"KVBuffer.h"
#include"Callbacks.h"
#include"Partitioner.h"

namespace Imagine_MapReduce{

template<typename reader_key,typename reader_value,typename key,typename value>
class Mapper;

template<typename reader_key,typename reader_value,typename key,typename value>
class MapRunner
{

public:
    class SpillReader{//溢写文件的读取器
        
        public:
            SpillReader(std::string key_, std::string value_, int fd_):spill_key(key_),spill_value(value_),fd(fd_){}

        public:
            std::string spill_key;  //溢写文件的键值
            std::string spill_value;//溢写文件的值
            int fd;                 //溢写文件的文件描述符
    };

    class SpillReaderCmp{// 比较两个 SpillReader 对象的键值大小。它重载了 operator() 运算符，根据键值的字典序进行比较，返回比较结果
        public:
            bool operator()(SpillReader* a, SpillReader* b)
            {
                return strcmp(&a->spill_key[0],&b->spill_key[0])<0?true:false;
            }
    };

public:

    // MapRunner(int split_id_, int split_num_, const std::string file_name_, int partition_num_=DEFAULT_PARTITION_NUM):split_id(split_id_),split_num(split_num_),file_name(file_name_),partition_num(partition_num_)
    // {
    //     buffer=new KVBuffer(partition_num,split_id_,spill_files);
    //     Init();
    // }

    MapRunner(int split_id_, int split_num_, const std::string file_name_, const std::string& mapper_ip_, const std::string& mapper_port_, const std::string& master_ip_, const std::string& master_port_, MAP map_, Partitioner<key>* partitioner_, OutputFormat<key,value>* output_format_, RpcServer* rpc_server_, int partition_num_=DEFAULT_PARTITION_NUM)
        :split_id(split_id_),split_num(split_num_),file_name(file_name_),mapper_ip(mapper_ip_),mapper_port(mapper_port_),master_ip(master_ip_),master_port(master_port_),map(map_),partitioner(partitioner_),output_format(output_format_),rpc_server(rpc_server_),partition_num(partition_num_)
    {
        buffer=new KVBuffer(partition_num,split_id_,spill_files);//初始化 MapRunner 对象中的 buffer 成员变量
        Init();//当前只是创建了一个 spilling_thread 线程
    }

    ~MapRunner()
    {
    }

    void Init()
    {        
        spilling_thread=new pthread_t;
        if(!spilling_thread){
            throw std::exception();
        }
    }
    //  mapper对象的 master_ip
    bool SetIp(const std::string& ip_){
        master_ip = ip_;
        return true;
    }

    bool SetPort(const std::string& port_){master_port=port_;return true;}

    bool SetRecordReader(std::shared_ptr<RecordReader<reader_key,reader_value>> record_reader_){record_reader=record_reader_;return true;}

    bool SetMapFunction(MAP map_){map=map_;return true;}

    bool SetTimerCallback(MAPTIMER timer_callback_){timer_callback=timer_callback_;return true;}

    bool SetRpcServer(RpcServer* rpc_server_){rpc_server=rpc_server_;return true;}

    int GetSplitNum(){return split_num;}

    std::string GetFileName(){return file_name;}

    std::string GetMapperIp(){return mapper_ip;}

    std::string GetMapperPort(){return mapper_port;}

    std::string GetMasterIp(){return master_ip;}

    std::string GetMasterPort(){return master_port;}

    std::shared_ptr<RecordReader<reader_key,reader_value>> GetRecordReader(){return record_reader;}

    MAP GetMap(){return map;}

    MAPTIMER GetTimerCallback(){return timer_callback;}

    RpcServer* GetRpcServer(){return rpc_server;}

    OutputFormat<key,value>* GetOutPutFormat(){return output_format;}

    int GetId(){return split_id;}
    // 将键值对转换为字符串格式，并将其写入到缓冲区中的相应分区Partition
    bool WriteToBuffer(const std::pair<key,value>& content)
    {
        std::pair<char*,char*> pair_=output_format->ToString(content);// 其中 first 是键的字符串表示，second 是值的字符串表示
        bool flag=buffer->WriteToBuffer(pair_,partitioner->Partition(content.first));// 根据键的值计算其所属的分区编号
        delete [] pair_.first;
        delete [] pair_.second;
        return flag;
    }
    // 启动数据溢写线程
    bool StartSpillingThread()
    {   // 创建一个新的线程，传递了参数 buffer，表示需要进行溢写的缓冲区对象
        pthread_create(spilling_thread,nullptr,[](void* argv)->void*{
            KVBuffer* buffer=(KVBuffer*)argv;
            buffer->Spilling(); //数据溢写操作

            delete buffer;

            return nullptr;
        },buffer);
        pthread_detach(*spilling_thread);

        return true;
    }

    bool CompleteMapping()
    {
        //溢写缓冲区全部内容
        buffer->SpillBuffer();//保证全部spill完毕
        // 调用缓冲区对象的 SpillBuffer 方法，将缓冲区中的内容全部进行数据溢写
        Combine();//调用 Combine 方法进行合并操作
    }

    bool Combine()//合并spill文件到shuffle文件
    {
        /*
        Spill过程、Shuffle过程以及Merge过程中,对于用户要写的每一对key,value:
        -" "作为key和value的分隔符
        -"\r\n"作为每一对数据的分隔符
        */
        printf("Start Combining!\n");
        const int spill_num=spill_files[0].size();// 溢写文件有几个，就有几个溢出文件描述符组
        // 每个分区(partition)的多个溢出文件 合并为 每个 shuffle（string)
        // ！！！！先将第一个分区的多个溢出文件 写入到 一个shuffle_file文件==文件描述符shuffle_fd
        // 再第二个分区（partition）继续
        // 循环下来，多个一维shuffle文件组成数组shuffle_files
        for(int i=0;i<partition_num;i++){        
             //  shuffle文件名
            std::string shuffle_file="split"+MapReduceUtil::IntToString(split_id)+"_shuffle_"+MapReduceUtil::IntToString(i+1)+".txt";
            shuffle_files.push_back(shuffle_file);// 将 一个string shuffle 文件名加到 一维shuffle_files 中
            int shuffle_fd=open(&shuffle_file[0],O_CREAT|O_RDWR,0777);// / 取shuffle_file文件对应的shuffle_fd
            int fds[spill_num];// 溢写文件描述符数组fds，大小为spill_num
            std::priority_queue<SpillReader*,std::vector<SpillReader*>,SpillReaderCmp> heap_;
            // 创建优先队列 heap_，元素类型为 SpillReader*，使用自定义的比较函数 SpillReaderCmp 进行排序
            // printf("spill_num is %d\n",spill_num);
            // 不同溢出文件 合并到 shuffle文件
            for(int j=0;j<spill_num;j++){
                fds[j]=open(&spill_files[i][j][0],O_RDWR);//取文件名字符串  string文件名对应溢出文件描述符，string==一对键值对
                std::string key_;
                std::string value_;
                if(SpillRead(fds[j],key_,value_)) heap_.push(new SpillReader(key_,value_,fds[j]));
            }
            // 优先队列 heap_ 进行合并：从优先队列中取出最小的 SpillReader* 对象 next_。
            // 如果成功读取 next_ 中的键值对，则将键写入 shuffle 文件，然后写入空格符 ' '，接着写入值，最后写入回车符 "\r\n"。删除 next_ 对象。
            // shufflefd 里面 只包含一个键值对
            while(heap_.size()){
                SpillReader* next_=heap_.top();
                heap_.pop();
                std::string key_;
                std::string value_;
                if(SpillRead(next_->fd,key_,value_)) heap_.push(new SpillReader(key_,value_,next_->fd));//保证队列不变
                write(shuffle_fd,&next_->spill_key[0],next_->spill_key.size());
                char c=' ';
                write(shuffle_fd,&c,1);
                write(shuffle_fd,&next_->spill_value[0],next_->spill_value.size());
                char cc[]="\r\n";
                write(shuffle_fd,cc,2);

                delete next_;
            }
            // 一个个关闭溢写文件描述符和删除溢写文件。关闭 shuffle 文件。
            for(int j=0;j<spill_num;j++){
                close(fds[j]);
                remove(&spill_files[i][j][0]);// 指向该文件名字符串的指针
            }
            close(shuffle_fd);
        }

        return true;
    }
    // 读取溢写文件的一对键值对 和获取生成的 shuffle 文件的信息
    bool SpillRead(int fd, std::string& key_, std::string& value_)//从spill文件读取kv对
    {
        //假设空格是key和value的分隔符,且每个kv以\r\n结尾,且kv中不包含这三个字符
        bool flag=false;
        char c;
        int ret=read(fd,&c,1);
        if(!ret)return false;//文件读空
        
        key_.push_back(c);
        while(1){
            // printf("here\n");
            read(fd,&c,1);
            if(c==' ')break;
            key_.push_back(c);
        }
        while(1){
            read(fd,&c,1);
            value_.push_back(c);
            if(c=='\n'&&flag){
                value_.pop_back();
                value_.pop_back();
                return true;
            }
            if(c=='\r')flag=true;
            else flag=false;
        }
    }
    // 取生成的 shuffle 文件的文件名。这个函数主要用于向 MapReduce 中心报告任务完成时所需的参数
    std::vector<std::string>& GetShuffleFile(){return shuffle_files;}

private:

    const int split_id;//split的id 分片的ID  当前 MapRunner 处理的是哪个分片
    const int split_num;//分片的总数
    const int partition_num;//分区数(shuffle数目)
    const std::string file_name;//源文件名 == 目标文件的名称
    // 当前 Mapper（映射器）的 IP 地址。在分布式计算中，可能有多个 Mapper 并行执行，
    // 每个 Mapper 都在不同的计算节点上运行，因此需要知道每个 Mapper 的 IP 地址以进行通信和协调
    const std::string master_ip;//master的ip
    const std::string master_port;//master的port   MapperIp 是指当前 Mapper 的 IP 地址。MasterIp 是指 MapReduce 框架中的 Master 的 IP 地址。
    const std::string mapper_ip;//mapper的ip
    const std::string mapper_port;//mapper的port
    // MapReduce 框架中的 Master（主节点）的 IP 地址。Master 负责整个 MapReduce 作业的协调和调度。
    // 所有的 Mapper 和 Reducer（归约器）都与 Master 进行通信，向其报告状态、获取任务等

    std::shared_ptr<RecordReader<reader_key,reader_value>> record_reader;//共享指针，用于读取记录
    OutputFormat<key,value>* output_format;//输出键值对的格式化
    Partitioner<key>* partitioner;//用于将键值对分发到不同的分区
    MAP map;// Map 函数，用于对输入的键值对进行映射操作
    MAPTIMER timer_callback;//时器回调函数，在定时器触发时执行特定的操作

    RpcServer* rpc_server;//进行远程过程调用

    //缓冲区
    KVBuffer* buffer;
    pthread_t* spilling_thread;//执行数据溢写操作的线程

    std::vector<std::vector<std::string>> spill_files;//二维字符串向量，用于存储溢写的文件名（多个shuffle文件名）
    std::vector<std::string> shuffle_files;           //字符串向量，用于存储生成的 一个shuffle文件名
    
};

}





#endif