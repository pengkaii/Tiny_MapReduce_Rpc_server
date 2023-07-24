#ifndef IMAGINE_MAPREDUCE_MASTER_H
#define IMAGINE_MAPREDUCE_MASTER_H

#include<atomic>

#include<Imagine_Muduo/Imagine_Muduo/EventLoop.h>
#include<Imagine_Rpc/Imagine_Rpc/RpcServer.h>

#include"Callbacks.h"

using namespace Imagine_Rpc;

/*
MapReduce通信格式
    -第一个字段表示身份(Mapper、Reducer、MapReduceMaster)
    -第二个字段表示消息类型
        -start:任务开始
        -process:任务进度
        -finish:任务完成
        -receive:收到信息
        -sendfile:要求mapper将文件传输给reducer
    -后面字段表示消息内容(任务进度信息或任务完成的地址或文件传输地址)
*/

namespace Imagine_MapReduce{
// 主节点类
class MapReduceMaster
{

public:
    class ReducerNode{
        public:
            ReducerNode():ip(""),port(""),is_ready(false)
            {
                reducer_lock=new pthread_mutex_t;
            }

        public:
            std::string ip;// Reducer节点的IP地址
            std::string port;
            std::vector<std::string> files;// 与该Reducer节点相关的路径文件列表

            pthread_mutex_t* reducer_lock;
            std::atomic<bool> is_ready;// Reducer节点是否准备就绪  的原子变量
    };

public:

    MapReduceMaster(const std::string& ip_, const std::string& port_, const std::string& keeper_ip_, const std::string& keeper_port_, const int reducer_num_=DEFAULT_REDUCER_NUM);

    ~MapReduceMaster();

    //目前只支持单文件处理,因为要区分不同文件则不同Mapper应该对应在不同文件的机器
    bool MapReduce(const std::vector<std::string>& file_list, const int reducer_num_=DEFAULT_REDUCER_NUM, const int split_size=DEFAULT_READ_SPLIT_SIZE);

    //向Reducer发送一个预热信息,注册当前MapReduceMaster,并开启心跳
    bool StartReducer(const std::string& reducer_ip, const std::string& reducer_port);
    // 连接Mapper节点
    bool ConnMapper();
    // 连接Reducer节点
    bool ConnReducer(const std::string& split_num, const std::string& file_name, const std::string& split_file_name, const std::string& mapper_ip, const std::string& mapper_port, const std::string& reducer_ip, const std::string& reducer_port);
    // 执行MapReduce中心逻辑
    std::vector<std::string> MapReduceCenter(const std::vector<std::string>& message);
    // 处理来自Mapper节点的消息
    std::string ProcessMapperMessage(const std::vector<std::string>& message);
    // 处理来自Reducer节点的消息
    std::string ProcessReducerMessage(const std::vector<std::string>& message);
    // 主循环函数
    void loop();
    // 设置任务文件
    bool SetTaskFile(std::vector<std::string>& files_);

private:
    const std::string ip; // MapReduceMaster主节点的IP地址
    const std::string port;

    const std::string keeepr_ip;
    const std::string keeper_port;

    const int reducer_num; // Reducer节点的数量

    // 用于接收Mapper和Reducer的任务进度信息的RPC服务器和线程
    RpcServer* rpc_server;       // RPC请求服务器
    pthread_t* rpc_server_thread;// RPC服务器的线程
    std::vector<std::string> files;//将多个string文件的路径传递给 file_list，以便在MapReduce过程中对这些文件进行处理

    std::unordered_map<int,ReducerNode*> reducer_map;  // 保存Reducer节点的信息，以Reducer的ID为键 <节点id,Reducer节点>
};


}



#endif