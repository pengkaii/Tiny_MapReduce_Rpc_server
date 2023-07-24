#ifndef IMAGINE_MAPREDUCE_MAPPER_H
#define IMAGINE_MAPREDUCE_MAPPER_H

#include<memory.h>

#include"Imagine_Rpc/Imagine_Rpc/RpcServer.h"
#include"MapReduceUtil.h"
#include"RecordReader.h"
#include"LineRecordReader.h"
#include"MapRunner.h"
#include"Imagine_Rpc/Imagine_Rpc/RpcClient.h"
#include"OutputFormat.h"
#include"TextOutputFormat.h"
#include"Callbacks.h"
#include"Partitioner.h"
#include"StringPartitioner.h"
// Map 阶段的相关功能和数据成员
namespace Imagine_MapReduce{

template<typename reader_key,typename reader_value,typename key,typename value>
class Mapper
{

public:

    Mapper(const std::string& ip_, const std::string& port_, RecordReader<reader_key,reader_value>* record_reader_=nullptr, MAP map_=nullptr, Partitioner<key>* partitioner_=nullptr, OutputFormat<key,value>* output_format_=nullptr, MAPTIMER timer_callback_=nullptr, const std::string& keeper_ip_="", const std::string& keeper_port_="");

    ~Mapper();

    //Rpc通信调用
    std::vector<std::string> Map(const std::vector<std::string>& input);

    std::vector<std::string> GetFile(const std::vector<std::string>& input);

    bool SetDefaultRecordReader();// 默认的输入记录读取器（分块，分块id)

    bool SetDefaultMapFunction(); // 默认的 Map 函数

    bool SetDefaultTimerCallback();

    bool SetDefaultOutputFormat();// 默认的输出格式

    bool SetDefaultPartitioner(); // 默认的分区器
                                  // 默认的计时器回调函数
    static void DefaultTimerCallback(int sockfd, std::shared_ptr<RecordReader<reader_key,reader_value>> reader);

    void loop();

private:

    const std::string ip;  // 服务端
    const std::string port;

    const std::string keeper_ip;
    const std::string keeper_port;

    MAP map_;//提供给用户自定义的 map函数
    MAPTIMER timer_callback;//定时回调函数 
    
    //MapReduceUtil::RecordReader record_reader;//提供给用户自定义的recordread函数
    RecordReader<reader_key,reader_value>* record_reader_class;//split迭代器类型==记录读取器(分块，分块id)
    OutputFormat<key,value>* output_format;// 输出格式
    std::vector<InputSplit*> splits;       // 输入总分块

    pthread_t* map_threads;                // map线程数组


    RpcServer* rpc_server;                 // rpc服务器
    pthread_t* rpc_server_thread;          // rpc服务器线程

    Partitioner<key>* partitioner;         // 使用分区器，将键值对数据按照一定规则分配到不同的分区中
};
// 实例化 Mapper 类
template<typename reader_key,typename reader_value,typename key,typename value>
Mapper<reader_key,reader_value,key,value>::Mapper(const std::string& ip_, const std::string& port_, RecordReader<reader_key,reader_value>* record_reader_, MAP map_, Partitioner<key>* partitioner_, OutputFormat<key,value>* output_format_, MAPTIMER timer_callback_, const std::string& keeper_ip_, const std::string& keeper_port_):
    ip(ip_),port(port_),keeper_ip(keeper_ip_),keeper_port(keeper_port_)
{
    rpc_server_thread=new pthread_t;// 管理 RPC 服务器线程
    if(!rpc_server_thread){
        throw std::exception();
    }

    if(record_reader_)record_reader_class=record_reader_;// 记录读取器（分块，分块id）
    else record_reader_class=new LineRecordReader();     // 行记录读取器（分块，分块id）

    if(map_)map_=map_;                                   // map函数
    else SetDefaultMapFunction();                        // 否则设置 map函数

    if(output_format_)output_format=output_format_;
    else SetDefaultOutputFormat();

    if(timer_callback_)timer_callback=timer_callback_;
    else SetDefaultTimerCallback();

    if(partitioner_)partitioner=partitioner_;
    else SetDefaultPartitioner();

    rpc_server=new RpcServer(ip,port,keeper_ip,keeper_port);
    // 远程调用请求到达 RpcServer 时，对应的回调函数 Map 和 GetFile 将被调用，并传递请求作为参数
    // 将 Mapper 类中的 Map 和 GetFile 函数暴露给远程客户端，通过远程调用来执行相应的功能
    rpc_server->Callee("Map",std::bind(&Mapper::Map,this,std::placeholders::_1));// placeholders作为参数占位符，表示在实际调用时会将调用者传递进来
    rpc_server->Callee("GetFile",std::bind(&Mapper::GetFile,this,std::placeholders::_1));
}

template<typename reader_key,typename reader_value,typename key,typename value>
Mapper<reader_key,reader_value,key,value>::~Mapper()
{
    delete rpc_server;
    delete rpc_server_thread;
    delete [] map_threads;

    delete record_reader_class;
    delete output_format;
    delete partitioner;
}

template<typename reader_key,typename reader_value,typename key,typename value>

// 实现并行的 Map 操作。它为每个分片创建一个线程，并在每个线程中执行相应的 Map 操作。
// 每个线程独立地读取记录、执行 Map 函数并写入缓冲区，最后通过数据溢写线程完成最终的处理和输出
std::vector<std::string> Mapper<reader_key,reader_value,key,value>::Map(const std::vector<std::string>& input)
{
    /*
    数据格式：
        1.目标文件名(路径)
        2.split大小
        3.MapReduceMaster的ip
        4.MapReduceMaster的port
        5.目标文件ip(若在本地则省略)
        6.目标文件port(若在本地则省略)
    注:对于split的跨行问题:到达split时多读一行(多读一个完整的\r\n进来),并且让除第一块意外的每个Mapper都跳过第一行数据(split起始位置为0不跳行,反之跳行)
    */

    // 获取split数据
    // 传入目标文件名和split大小
    // 使用输入参数获取目标文件的分片数据、将目标文件名和分片大小作为参数，并返回一个包含 InputSplit 对象的向量
    std::vector<InputSplit*> splits=MapReduceUtil::DefaultReadSplitFunction(input[0],MapReduceUtil::StringToInt(input[1]));//获取目标文件的总分片数据
    // map(MapReduceUtil::DefaultReadSplitFunction(input[2]));
    // 将数据按要求转换成kv数据
    // std::unordered_map<std::string,std::string> kv_map=record_reader(splits);
    map_threads=new pthread_t[splits.size()];
    for(int i=0;i<splits.size();i++){//它使用一个循环来处理每个分片
        // 创建一个 RecordReader 对象 new_record_reader，用于读取当前分片的记录
        //  表示当前分片的输入数据，i+1：表示当前分片的 ID。
        std::shared_ptr<RecordReader<reader_key,reader_value>> new_record_reader=record_reader_class->CreateRecordReader(splits[i],i+1);
        // new_record_reader->SetOutputFileName("split"+MapReduceUtil::IntToString(i+1)+".txt");
        // 创建一个 MapRunner管理者对象（runner），并初始化它与当前分片和其他相关信息
        MapRunner<reader_key,reader_value,key,value>* runner=new MapRunner<reader_key,reader_value,key,value>(i+1,splits.size(),input[0],ip,port,input[2],input[3],map_,partitioner,output_format,rpc_server);
        // i+1: 当前分片的 ID  splits.size(): 分片的总数  input[0]: 目标文件名/路径
        // ip: 当前 Mapper 的 IP 地址 、port: 当前 Mapper 的端口号
        // input[2]: MapReduceMaster 主节点的 IP 地址，作为 MapRunner 构造函数的第六个参数。
        // input[3]: MapReduceMaster 主节点 的端口号，作为 MapRunner 构造函数的第七个参数。
        // map_: 提供给用户自定义的 map 函数，作为 MapRunner 构造函数的第八个参数。
        // partitioner: 分区器对象，作为 MapRunner 构造函数的第九个参数。
        // output_format: 输出格式化对象，作为 MapRunner 构造函数的第十个参数。
        // rpc_server: RpcServer 对象，作为 MapRunner 构造函数的第十一个参数。
        runner->SetRecordReader(new_record_reader);//将 new_record_reader 设置为 runner 的记录读取器
        runner->SetTimerCallback(timer_callback);  //将 timer_callback 设置为 runner 的定时器回调函数
        // 创建一个新的线程，并将上述步骤创建的 runner 对象作为参数argv传递给线程函数
        pthread_create(map_threads+i,nullptr,[](void* argv)->void*{

            MapRunner<reader_key,reader_value,key,value>* runner=(MapRunner<reader_key,reader_value,key,value>*)argv;
            MAP map_=runner->GetMap(); // 获取 runner 中的 MAP 函数
            std::shared_ptr<RecordReader<reader_key,reader_value>> reader=runner->GetRecordReader(); // 记录读取器
            OutputFormat<key,value>* output_format=runner->GetOutPutFormat();  // 输出格式对象

            //连接Master主节点
            int sockfd;
            long long timerfd; // 通过调用 RpcClient::ConnectServer() 连接到 MapReduceMaster
            // 使用 RpcClient::ConnectServer 函数连接到 MapReduceMaster，
            // 并发送启动信号给 MapReduceMaster。如果连接成功，还会设置一个定时器，定期发送进度信息
            if(RpcClient::ConnectServer(runner->GetMasterIp(),runner->GetMasterPort(),&sockfd)){
                std::vector<std::string> parameters;
                parameters.push_back("Mapper");
                parameters.push_back(runner->GetFileName());
                parameters.push_back(MapReduceUtil::IntToString(runner->GetId()));
                parameters.push_back("Start");
                // 调用 RpcClient::Call 函数向 MapReduceCenter 发起远程过程调用（RPC）。
                // 该调用使用 parameters 作为参数，请求执行 "MapReduceCenter" 的方法
                if(RpcClient::Call("MapReduceCenter",parameters,&sockfd)[0]=="Receive"){
                    // 如果 RPC 调用成功并且返回的结果中第一个元素是 "Receive"，则通过调用 runner->GetRpcServer()->SetTimer 设置一个定时器。
                    // 定时器将在 2.0 秒后启动，使用 runner->GetTimerCallback() 作为回调函数，并传递 sockfd 和 reader 作为参数
                    timerfd=runner->GetRpcServer()->SetTimer(2.0,0.0,std::bind(runner->GetTimerCallback(),sockfd,reader));
                }else{
                    throw std::exception();
                }
            }
            // 启动数据溢写线程（StartSpillingThread()）
            runner->StartSpillingThread();
            sleep(1);
            while(reader->NextKeyValue()){
                // 循环读取记录并进行 Map 操作，将结果写入缓冲区
                runner->WriteToBuffer(map_(reader->GetCurrentKey(),reader->GetCurrentValue()));
            }
            // 在 Map 操作完成后，调用 CompleteMapping() 来处理剩余的缓冲区数据，并进行数据溢写
            runner->CompleteMapping(); //buffer在spill线程中销毁  == 缓冲区中的所有数据已经进行了 Map 处理，并且在溢写线程中被处理
            // 从 MapReduceMaster 接收到完成信号后，向 MapReduceMaster 发送任务完成的消息，
            // 从 MapReduceMaster 接收到任务完成的信号后，向 MapReduceMaster 发送任务完成的消息
            // 创建一个包含特定参数的 std::vector<std::string> 对象 parameters，然后调用 RpcClient::Call 函数向 MapReduceCenter 发起远程过程调用（RPC）。
            // 该调用使用 parameters 作为参数，请求执行 "MapReduceCenter" 的方法
            runner->GetRpcServer()->RemoveTimer(timerfd);
            std::vector<std::string> parameters;
            parameters.push_back("Mapper");  // 当前操作是 Mapper 的完成消息。
            parameters.push_back(runner->GetFileName());
            parameters.push_back(MapReduceUtil::IntToString(runner->GetId()));        // 当前 Mapper 的 ID
            parameters.push_back("Finish"); // 操作已完成
            parameters.push_back(runner->GetMapperIp()); // 当前 Mapper 的 IP
            parameters.push_back(runner->GetMapperPort());
            parameters.push_back(MapReduceUtil::IntToString(runner->GetSplitNum()));  // 表示当前 Mapper 处理的分片数量
            // Mapper 将向 MapReduceCenter 发送任务完成的消息，并将相关的 Shuffle 文件信息一并发送。
            std::vector<std::string>& shuffle=runner->GetShuffleFile();
            parameters.insert(parameters.end(),shuffle.begin(),shuffle.end());
            // 确保在向 MapReduceCenter 发送完成消息时，包含了与 Shuffle 相关的文件信息
            //printf("%s : working is finish!\n",&split_file_name[0]);
            RpcClient::Call("MapReduceCenter",parameters,&sockfd);
            // 使用 RpcClient::Call 函数发起远程过程调用，向 MapReduceCenter 发送完成消息。具体来说，
            // 使用 "MapReduceCenter" 作为方法名，parameters 作为参数，并将 sockfd 作为用于与 MapReduceCenter 通信的套接字。

            delete runner;

            close(sockfd);
            //close(fd);
            // 清理资源，关闭与 MapReduceMaster 的连接，返回 nullptr。
            return nullptr;

        },runner); // 该函数返回一个反序列化结果的空 std::vector<std::string>
        pthread_detach(*(map_threads+i)); //分离线程，使其在运行结束后自动释放资源
    }

    return Rpc::Deserialize(""); //用于反序列化从远程过程调用（RPC）返回的数据
}
// 获取指定文件的内容
// GetFile 函数可以获取指定文件的内容，并将其作为向量返回。这可用于在 Mapper 中处理需要从文件中读取数据的情况
template<typename reader_key,typename reader_value,typename key,typename value>
std::vector<std::string> Mapper<reader_key,reader_value,key,value>::GetFile(const std::vector<std::string>& input)
{
    std::vector<std::string> output;
    std::string content;
    printf("get file %s\n",&input[0][0]);
    int fd=open(&input[0][0],O_RDWR);//通过 open 函数使用文件名和标志 O_RDWR 打开文件
    // 将读取的数据存储在 content 字符串中
    while(1){
        char buffer[1024];
        int ret=read(fd,buffer,1024);
        for(int i=0;i<ret;i++){
            content.push_back(buffer[i]);
        }
        if(ret!=1024)break;
    }
    close(fd);
    if(content.size())output.push_back(content);

    return output;
}

template<typename reader_key,typename reader_value,typename key,typename value>
bool Mapper<reader_key,reader_value,key,value>::SetDefaultRecordReader()
{
    record_reader_class=new LineRecordReader();
    return true;
}

template<typename reader_key,typename reader_value,typename key,typename value>
bool Mapper<reader_key,reader_value,key,value>::SetDefaultMapFunction()
{
    // map=DefaultMapFunction;
    // map函数对象，接受offset和line_text，并返回一个键值对
    map_=[](int offset,const std::string& line_text)->std::pair<std::string,int>{
        return std::make_pair(line_text,1); // 返回键值对
    };

    return true;
}

template<typename reader_key,typename reader_value,typename key,typename value>
bool Mapper<reader_key,reader_value,key,value>::SetDefaultTimerCallback()
{
    timer_callback=DefaultTimerCallback;
    return true;
    // timer_callback=[](int sockfd, std::shared_ptr<RecordReader<reader_key,reader_value>> reader){
    //     printf("this is timer callback!\n");
    //     if(reader.use_count()==1){
    //     printf("mapper已完成!\n");
    //     return;
    //     }
    //     //printf("sockfd is %d\n",sockfd);
    //     std::string method="MapReduceCenter";
    //     std::vector<std::string> parameters;
    //     parameters.push_back("Mapper");
    //     parameters.push_back(reader->GetOutputFileName());
    //     parameters.push_back("Process");
    //     parameters.push_back(MapReduceUtil::DoubleToString(reader->GetProgress()));

    //     std::vector<std::string> recv_=RpcClient::Call(method,parameters,&sockfd);
    //     if(recv_.size()&&recv_[0]=="Receive"){
    //         printf("connect ok!\n");
    //     }else{
    //         printf("connect error!\n");
    //     }
    // }
}

template<typename reader_key,typename reader_value,typename key,typename value>
bool Mapper<reader_key,reader_value,key,value>::SetDefaultPartitioner()
{
    partitioner=new StringPartitioner();
    return true;
}

template<typename reader_key,typename reader_value,typename key,typename value>
bool Mapper<reader_key,reader_value,key,value>::SetDefaultOutputFormat()
{
    output_format=new TextOutputFormat();
}

template<typename reader_key,typename reader_value,typename key,typename value>
void Mapper<reader_key,reader_value,key,value>::loop()
{  // 新线程要执行的lambda函数
    pthread_create(rpc_server_thread,nullptr,[](void* argv)->void*{
        RpcServer* rpc_server=(RpcServer*)argv;
        rpc_server->loop();
    },this->rpc_server);
    pthread_detach(*rpc_server_thread); //新线程与主线程分离，使得主线程和新线程可以并行运行而互相独立
}

template<typename reader_key,typename reader_value,typename key,typename value>
void Mapper<reader_key,reader_value,key,value>::DefaultTimerCallback(int sockfd, std::shared_ptr<RecordReader<reader_key,reader_value>> reader)
{
    // printf("this is timer callback!\n");
    if(reader.use_count()==1){// 共享指针reader，为 1，表示当前任务已完成
    // 确保任务在没有其他任务使用共享资源时能够正确地完成，并避免对已完成的任务执行额外的操作
        printf("This Mapper Task Over!\n");
        return;
    }
    //printf("sockfd is %d\n",sockfd);
    // 构造要调用的远程过程调用（RPC）的方法名和远程调用参数
    std::string method="MapReduceCenter";
    std::vector<std::string> parameters;
    parameters.push_back("Mapper");
    parameters.push_back(reader->GetFileName());// 当前 reader 对象的文件名
    parameters.push_back(MapReduceUtil::IntToString(reader->GetSplitId()));
    parameters.push_back("Process");//处理进度信息
    parameters.push_back(MapReduceUtil::DoubleToString(reader->GetProgress()));
    // 发起 RPC 调用
    // 向指定的远程服务端发起 RPC 请求，并返回一个 recv_ 向量，其中包含收到的响应
    std::vector<std::string> recv_=RpcClient::Call(method,parameters,&sockfd);
    if(recv_.size()&&recv_[0]=="Receive"){
        // printf("connect ok!\n");
    }else{
        printf("connect error!\n");
    }
}

}



#endif