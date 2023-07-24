#include"MapReduceMaster.h"

#include<Imagine_Rpc/Imagine_Rpc/RpcClient.h>

#include"MapReduceUtil.h"

using namespace Imagine_Rpc;
using namespace Imagine_MapReduce;

MapReduceMaster::MapReduceMaster(const std::string& ip_, const std::string& port_, const std::string& keeper_ip_, const std::string& keeper_port_, const int reducer_num_):
    ip(ip_),port(port_),keeepr_ip(keeper_ip_),keeper_port(keeper_port_),reducer_num(reducer_num_)
{
    int temp_port=MapReduceUtil::StringToInt(port_);
    if(temp_port<0){
        throw std::exception();
    }
    
    rpc_server_thread=new pthread_t;
    if(!rpc_server_thread){
        throw std::exception();
    }

    // files.push_back("bbb.txt");

    rpc_server=new RpcServer(ip,port,keeper_ip_,keeper_port_);
     // 使用 rpc_server->Callee 注册 MapReduceCenter 函数作为 RPC 服务器的回调函数，使其能够处理来自客户端的 RPC 请求
    rpc_server->Callee("MapReduceCenter",std::bind(&MapReduceMaster::MapReduceCenter,this,std::placeholders::_1));
}

MapReduceMaster::~MapReduceMaster()
{
    delete rpc_server_thread;
    delete rpc_server;
}
//将多个string文件的路径传递给 file_list，以便在 MapReduce 过程中对这些文件进行处理
bool MapReduceMaster::MapReduce(const std::vector<std::string>& file_list, const int reducer_num_, const int split_size)
{   //  设置调用的方法为 "Map"
    std::string method="Map";
    std::vector<std::string> parameters;
    // 将传入的文件列表 file_list 存储到成员变量 files 中，并添加到参数列表中。
    for(int i=0;i<file_list.size();i++){
        files.push_back(file_list[i]);
        parameters.push_back(file_list[i]);
    }
    // 将分区大小和 MapReduceMaster 的 IP、端口添加到参数列表中
    parameters.push_back(MapReduceUtil::IntToString(split_size));
    parameters.push_back(ip);
    parameters.push_back(port);
    // 根据 reducer_num 创建对应数量的 ReducerNode，并将第一个文件添加到每个 ReducerNode 的文件列表中
    for(int i=1;i<=reducer_num;i++){
        ReducerNode* reducer_node=new ReducerNode;
        reducer_node->files.push_back(file_list[0]);
        reducer_map.insert(std::make_pair(i,reducer_node));// <reducer节点编号，节点>
    }
     // 调用 RpcClient::Caller 方法，向 Keeper 发送 Map 请求
    RpcClient::Caller(method,parameters,keeepr_ip,keeper_port);
    return true;
}
//向Reducer发送一个预热信息,注册当前MapReduceMaster,并开启心跳
// 向 Reducer 节点发送注册请求，并传递相关的文件和节点信息
bool MapReduceMaster::StartReducer(const std::string& reducer_ip, const std::string& reducer_port)
{
    /*
    信息格式
        -1.文件总数
        -2.文件名列表
        -3.本机ip
        -4.本机port
    */
   
    std::string method_name="Register";
    std::vector<std::string> parameters;
    // 添加文件总数到参数列表
    parameters.push_back(MapReduceUtil::IntToString(files.size()));
    // 添加每个文件名列表到参数列表
    for(int i=0;i<files.size();i++){
        parameters.push_back(files[i]);
    }
    // printf("master_ip is %s , master_port is %s\n",&ip[0],&port[0]);
    // 添加主节点 IP 和端口号到参数列表
    parameters.push_back(ip);
    parameters.push_back(port);
    // 调用 RpcClient::Call 方法，向 Reducer 节点发送 "Register" 请求
    std::vector<std::string> output=RpcClient::Call(method_name,parameters,reducer_ip,reducer_port);

    return true;
}

// bool MapReduceMaster::StartMapper(const std::string& file_name,const std::string& mapper_ip, const std::string& mapper_port)
// {
//     std::string method_name="Reduce";
//     std::vector<std::string> parameters;
//     if(file_name.size()){
//         parameters.push_back("GetFile");
//         parameters.push_back(mapper_ip);
//         parameters.push_back(mapper_port);
//     }else{
//         parameters.push_back("Start");
//         parameters.push_back(ip);
//         parameters.push_back(port);
//     }
// }

bool MapReduceMaster::ConnReducer(const std::string& split_num, const std::string& aim_file_name, const std::string& split_file_name, const std::string& mapper_ip, const std::string& mapper_port, const std::string& reducer_ip, const std::string& reducer_port)
{
    /*
    参数说明
        -1.split_num:文件被分成的split数
        -2.file_name:split前的文件名==目标文件名
        -3.split_file_name:当前要获取的split文件名==当前要获取的分割文件名
        -4.mapper_ip:文件所在的mapper的ip
        -5.mapper_port:文件所在的mapper的port
        -6.master_ip:master的ip
        -7.master_port:master的port
    */
    std::string method_name="Reduce";
    std::vector<std::string> parameters;
    parameters.push_back(split_num);
    parameters.push_back(aim_file_name);
    parameters.push_back(split_file_name);
    parameters.push_back(mapper_ip);
    parameters.push_back(mapper_port);
    parameters.push_back(ip);
    parameters.push_back(port);
    // 调用 RpcClient::Call 方法，向 Reducer 节点发送 "Reduce" 请求，建立与 Reducer 节点的连接
    RpcClient::Call(method_name,parameters,reducer_ip,reducer_port);

    return true;
}
// 根据 传入的信息 选择处理 消息的方法
std::vector<std::string> MapReduceMaster::MapReduceCenter(const std::vector<std::string>& message)
{
    /*
    信息格式:
    -身份信息:Mapper/Reducer  信息的第一个元素为身份信息（"Mapper" 或 "Reducer"）
    */
    printf("this is MapReduceCenter!\n");

    std::vector<std::string> send_;
    if(message[0]=="Mapper"){
        send_.push_back(ProcessMapperMessage(message));
    }else if(message[0]=="Reducer"){
        send_.push_back(ProcessReducerMessage(message));
    }else throw std::exception();

    return send_;
}

//根据传入的消息类型对来自Mapper节点的消息进行处理
std::string MapReduceMaster::ProcessMapperMessage(const std::vector<std::string>& message)
{
    /*
    信息格式:
    -身份信息:Mapper
    -mapper名
        -处理的文件名
        -splitID
    -类型
        -Process:正在执行，尚未完成
        -Start:任务开始
        -Finish:任务完成
    -信息内容:
        -Process:
            -进度百分比(保留两位小数)
        -Start:无信息
        -Finish:
            -mapperIp
            -mapperPort
            -split数目
            -shuffle文件名列表
    */
    const std::string& aim_file_name=message[1];// 处理的mapper文件名==未分割之前的文件名
    const std::string& split_id=message[2];     // splitID 分块id
    const std::string& message_type=message[3]; // 消息类型

    if(message_type=="Process"){
        const std::string& process_percent=message[4];// 进度百分比(保留两位小数)
        printf("Mapper : file %s split %s processing is %s !\n",&aim_file_name[0],&split_id[0],&process_percent[0]);
    }else if(message_type=="Start"){
        printf("Mapper : file %s split %s processing is starting !\n",&aim_file_name[0],&split_id[0]);
    }else if(message_type=="Finish"){
        printf("Mapper : file %s split %s processing is finish !\n",&aim_file_name[0],&split_id[0]);

        const std::string& mapper_ip=message[4];
        const std::string& mapper_port=message[5];
        const std::string& split_num=message[6];
        std::vector<std::string> shuffle_list;// shuffle 路径文件名列表
        for(int i=7;i<message.size();i++){
            shuffle_list.push_back(std::move(message[i]));
        }
        if(reducer_num!=shuffle_list.size()){// Reducer 的数量与 shuffle 路径文件名列表的数量匹配
            throw std::exception();
        }
        // 遍历 Reducer 节点，并与每个 Reducer 节点建立连接
        for(int i=1;i<=reducer_num;i++){
            const std::string& shuffle_name=shuffle_list[i-1];// shuffle 文件名
            std::unordered_map<int,ReducerNode*>::iterator it=reducer_map.find(i);// 存储了Reducer节点的映射，查找当前Reducer节点的信息，使用i作为键进行查找
            if(it==reducer_map.end()){
                throw std::exception();
            }
            // 检查当前 Reducer节点 是否就绪，即是否已连接到 Master主节点
            if(!(it->second->is_ready.load())){//原子地读取 is_ready 的值 == Reducer 未就绪
                //reducer节点未就绪
                pthread_mutex_lock(it->second->reducer_lock);
                if(!(it->second->is_ready.load())){
                    //再次确认  reducer节点未就绪
                    printf("Searching Reducer!\n");
                    // 在zookeeper获取一个可用的 Reducer节点 并与Master主节点建立连接    用于与一个 Reducer 节点进行通信，请求获取一个可用的 Reducer
                    RpcClient::CallerOne("Reduce",keeepr_ip,keeper_port,it->second->ip,it->second->port);// 获取一个reducer节点
                    StartReducer(it->second->ip,it->second->port);                                       // 启动与Master主节点连接的Reducer节点
                    it->second->is_ready.store(true); //Reducer 节点已就绪
                    pthread_mutex_unlock(it->second->reducer_lock);
                }else{
                    pthread_mutex_unlock(it->second->reducer_lock);//如果 Reducer 已就绪，则继续下一个 Reducer
                }
            }
            // 调用 ConnReducer 方法，向当前 Reducer 发送连接请求 （分块数量，Mapper处理的文件名==未分割的文件名，当前Reducer的shuffle文件名，
            ConnReducer(split_num,aim_file_name,shuffle_name,mapper_ip,mapper_port,it->second->ip,it->second->port);
        }
    }

    return "Receive";//在处理完 Mapper节点的消息后，方法将返回字符串 "Receive"，，指示消息处理完成并告知发送方
}
// 处理reducer消息
// 打印处理进度的信息
std::string MapReduceMaster::ProcessReducerMessage(const std::vector<std::string>& message)
{    
    if(message[2]=="Process"){
        printf("reducer's processing is %s\n",&message[3][0]);
        return "";
    }

}
//创建一个新线程，在该线程中执行 RpcServer 对象的 loop() 方法，并将该线程标记为分离状态
void MapReduceMaster::loop()
{
    pthread_create(rpc_server_thread,nullptr,[](void* argv)->void*{
        RpcServer* rpc_server=(RpcServer*)argv;
        rpc_server->loop();

        return nullptr;
    },this->rpc_server);
    pthread_detach(*rpc_server_thread); //该线程的资源在终止时会自动回收，不需要其他线程调用 pthread_join 来等待该线程的终止
}
//设置任务的文件列表
bool MapReduceMaster::SetTaskFile(std::vector<std::string>& files_)
{
    files.clear();
    for(int i=0;i<files_.size();i++){
        files.push_back(files_[i]);
    }

    return true;
}