#include"RpcZooKeeper.h"

#include"Rpc.h"



using namespace Imagine_Rpc;
using namespace Imagine_ZooKeeper;


RpcZooKeeper::RpcZooKeeper(const std::string& ip_, const std::string& port_, EventCallback read_callback_, EventCallback write_callback_, EventCommunicateCallback communicate_callback_, double time_out_, int max_request_num):
    ZooKeeper(Rpc::StringToInt(port_),max_request_num,read_callback_,write_callback_,communicate_callback_),ip(ip_),port(port_),time_out(time_out_)
{
    if(!read_callback){
        SetDefaultReadCallback();
    }
    if(!write_callback){
        SetDefaultWriteCallback();
    }

    if(pthread_mutex_init(&heart_map_lock,nullptr)!=0){
        throw std::exception();
    }

    loop_=new EventLoop(Rpc::StringToInt(port_),max_request_num,read_callback,write_callback,communicate_callback);
}

RpcZooKeeper::RpcZooKeeper(const std::string& ip_, const std::string& port_, double time_out_, int max_request_num):
    ZooKeeper(Rpc::StringToInt(port_),max_request_num,nullptr,nullptr,nullptr),ip(ip_),port(port_),time_out(time_out_)
{
    SetDefaultReadCallback();
    SetDefaultWriteCallback();
    SetDefaultCommunicateCallback();
    SetDefaultTimerCallback();

    loop_=new EventLoop(Rpc::StringToInt(port_),max_request_num,read_callback,write_callback);
}

void RpcZooKeeper::SetDefaultReadCallback(){

    read_callback=[this](const struct iovec* input_iovec){

        int sockfd=*(int*)input_iovec[0].iov_base;
        printf("this is RpcZooKeeper!\n");
        std::string input=Rpc::GetIovec(input_iovec);

        //解析数据
        std::vector<std::string> de_input=Rpc::Deserialize(input);

        if(de_input[1]=="RpcClient"){
            //收到客户端消息,要查找函数所在的服务器
            /*
                -消息格式为:RpcClient\r\nFunc_\r\n
            */

            //找到函数,设置Watcher,目前设定默认都需要set_watcher
            struct sockaddr_in* addr=Rpc::GetAddr(input_iovec);
            //std::shared_ptr<RpcWatcher> new_watcher=std::make_shared<RpcWatcher>(Rpc::ConvertIpFromNetToString(addr->sin_addr.s_addr),Rpc::ConvertPortFromNetToString(addr->sin_port));

            //std::string output=this->SearchMethod(de_input[2],new_watcher);
            std::string output=this->SearchMethod(de_input[2]);
            if(output=="0\r\n"){
                //未找到函数
                struct iovec* output_iovec=Rpc::SetIovec(Rpc::GenerateDefaultFailureMessage(),12,false);
                return output_iovec;
            }
            struct iovec* output_iovec=Rpc::SetIovec(output,output.size(),false);
            return output_iovec;

        }else if(de_input[1]=="RpcServer"){
            //收到服务器消息
            /*
                -消息格式为:
                    -注册函数:RpcServer\r\nRegister\r\nCluster_name+Func_name\r\nIp_\r\nPort_\r\n
                    -下线函数:RpcServer\r\nDeRegister\r\nFunc\r\nIp_\r\nPort_\r\n
            */
           if(de_input[2]=="online"){
                //心跳检测包,返回成功即可,暂不处理各种异常
                printf("RpcZooKeeper Receive online!\n");
                pthread_mutex_lock(&heart_map_lock);
                heart_map.find(sockfd)->second->ReSetLastRequestTime();
                pthread_mutex_unlock(&heart_map_lock);
                struct iovec* output_iovec=Rpc::SetIovec(Rpc::GenerateDefaultSuccessMessage(),12);
                return output_iovec;
           }
           if(de_input.size()!=6){

                struct iovec* output_iovec=Rpc::SetIovec(Rpc::GenerateDefaultFailureMessage(),12,false);

                return output_iovec;
            }
           if(de_input[2]=="Register"){
                //请求注册函数
                //printf("ip is %s\nport is %s\n",&de_input[4][0],&de_input[5][0]);
                if(Register(de_input[3],de_input[4],de_input[5],sockfd)){
                    struct iovec* output_iovec=Rpc::SetIovec(Rpc::GenerateDefaultSuccessMessage(),12);
                    return output_iovec;
                }else{
                    struct iovec* output_iovec=Rpc::SetIovec(Rpc::GenerateDefaultFailureMessage(),12,false);
                    return output_iovec;
                }
           }else if(de_input[2]=="DeRegister"){
                //请求撤销函数注册

                if(DeRegister(de_input[3],de_input[4],de_input[5],sockfd)){
                    struct iovec* output_iovec=Rpc::SetIovec(Rpc::GenerateDefaultSuccessMessage(),12);
                    return output_iovec;
                }else{
                    struct iovec* output_iovec=Rpc::SetIovec(Rpc::GenerateDefaultFailureMessage(),12,false);
                    return output_iovec;
                }
           }else{
                struct iovec* output_iovec=Rpc::SetIovec(Rpc::GenerateDefaultFailureMessage(),12,false);
                return output_iovec;
           }
        }else{
            //错误的数据
            printf("小老板逮到!\n");
            struct iovec* output_iovec=Rpc::SetIovec(Rpc::GenerateDefaultFailureMessage(),12,false);

            return output_iovec;
        }
    };

    if(loop_)loop_->SetReadCallback(read_callback);
}

void RpcZooKeeper::SetDefaultWriteCallback()
{
    write_callback=[](const struct iovec* input_iovec){

        std::string input=Rpc::GetIovec(input_iovec);
        struct iovec* output_iovec=Rpc::SetIovec(input,input.size());
        *((char*)output_iovec[0].iov_base+4)='1';
        *((char*)output_iovec[0].iov_base+5)='0';

        return output_iovec;
    };

    if(loop_)loop_->SetWriteCallback(write_callback);
}

void RpcZooKeeper::SetDefaultCommunicateCallback(){
    communicate_callback=Rpc::DefaultCommunicateCallback;
    if(loop_)loop_->SetWriteCallback(write_callback);
}

void RpcZooKeeper::SetDefaultTimerCallback()
{
    timer_callback=[this](int sockfd,double time_out){

        printf("RpcZooKeeper TimerCallback!\n");

        std::string cluster_name;
        long long last_request_time;
        std::string stat;
        if(!GetHeartNodeInfo(sockfd,cluster_name,last_request_time,stat))return;
        std::vector<std::string> addr=Rpc::Deserialize(stat);

        if(TimeUtil::GetNow()>TimeUtil::MicroSecondsAddSeconds(last_request_time,time_out)){
            //已过期
            printf("RpcZooKeeper Timer Set offline!\n");
            this->loop_->Closefd(sockfd);
            DeRegister(cluster_name,addr[0],addr[1],sockfd);
            return;
        }else{
            //未过期,忽略
        }
    };
}

RpcZooKeeperTimerCallback RpcZooKeeper::GetTimerCallback()
{
    return timer_callback;
}

bool RpcZooKeeper::Register(const std::string& method, const std::string& ip_, const std::string& port_, int sockfd){

    if(InsertZnode(method,ip_+"\r\n"+port_+"\r\n",Load_Balance,"online")){
        RpcZKHeart* new_heart=new RpcZKHeart(method,ip_+"\r\n"+port_+"\r\n",SetTimer(15.0,0.0,std::bind(timer_callback,sockfd,time_out)));
        pthread_mutex_lock(&heart_map_lock);
        heart_map.insert(std::make_pair(sockfd,new_heart));
        pthread_mutex_unlock(&heart_map_lock);

        return true;
    }
    return false;
}

bool RpcZooKeeper::DeRegister(const std::string& method, const std::string& ip_, const std::string& port_, int sockfd){
    if(DeleteZnode(method,ip_+"\r\n"+port_+"\r\n","offline")){
        DeleteHeartNode(sockfd);
        return true;
    }

    return false;
}

std::string RpcZooKeeper::SearchMethod(const std::string& method, std::shared_ptr<RpcWatcher> new_watcher)
{
    std::string content=GetClusterZnodeStat(method,true,new_watcher);
    std::string head=Rpc::GenerateDefaultHead(content);

    return head+content;
}

bool RpcZooKeeper::DeleteHeartNode(int sockfd)
{
    pthread_mutex_lock(&heart_map_lock);
    std::unordered_map<int,RpcZKHeart*>::iterator it=heart_map.find(sockfd);
    if(it==heart_map.end()){
        return false;//已删除
        throw std::exception();
    }
    RpcZKHeart* heart_node=it->second;
    heart_map.erase(it);
    RemoveTimer(heart_node->GetTimerfd());
    delete heart_node;
    pthread_mutex_unlock(&heart_map_lock);

    return true;
}

long long RpcZooKeeper::SetTimer(double interval, double delay, RpcTimerCallback callback)
{
    return loop_->SetTimer(callback,interval,delay);
}

bool RpcZooKeeper::RemoveTimer(long long timerfd)
{
    return loop_->CloseTimer(timerfd);
}

bool RpcZooKeeper::GetHeartNodeInfo(int sockfd, std::string& cluster_name, long long& last_request_time, std::string& stat)
{
    pthread_mutex_lock(&heart_map_lock);
    std::unordered_map<int,RpcZKHeart*>::iterator it=heart_map.find(sockfd);
    if(it==heart_map.end()){
        //已删除
        pthread_mutex_unlock(&heart_map_lock);
        return false;
    }
    cluster_name=it->second->GetClusterName();
    last_request_time=it->second->GetLastRequestTime();
    stat=it->second->GetStat();
    pthread_mutex_unlock(&heart_map_lock);

    return true;
}

long long RpcZooKeeper::GetHeartNodeLastRequestTime(int sockfd)
{
    pthread_mutex_lock(&heart_map_lock);
    std::unordered_map<int,RpcZKHeart*>::iterator it=heart_map.find(sockfd);
    if(it==heart_map.end()){
        //已删除
        pthread_mutex_unlock(&heart_map_lock);
        return -1;
    }
    long long last_request_time=it->second->GetLastRequestTime();
    pthread_mutex_unlock(&heart_map_lock);
    
    return last_request_time;
}