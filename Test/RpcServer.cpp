#include<iostream>
#include<unordered_map>
#include<string>
#include<signal.h>
#include<memory.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<arpa/inet.h>
#include"Imagine_Rpc/Imagine_Rpc/RpcServer.h"
#include"Imagine_Rpc/Imagine_Rpc/RpcClient.h"
#include"Imagine_Rpc/Imagine_Rpc/RpcZooKeeper.h"

#include<sys/epoll.h>
#include<sys/timerfd.h>
#include<sys/types.h>
#include<functional>

using namespace std;
using namespace Imagine_Rpc;

void addsig(int sg,void(handler)(int)){
    struct sigaction sa;
    memset(&sa,'\0',sizeof(sa));
    sigfillset(&sa.sa_mask);
    sa.sa_flags=0;
    sa.sa_handler=handler;
    sigaction(sg,&sa,NULL);
}

auto add_func=[](const vector<string>& input){
    vector<string> output;
    int a=Rpc::StringToInt(input[0]);
    int b=Rpc::StringToInt(input[1]);
    output.push_back(Rpc::IntToString(a+b));

    return output;
};

int main(int argc,char* argv[]){

    // if(argc<=1){
    //     printf("please input two parameter at least!\n");
    //     return 0;
    // }

    //int port=atoi(argv[1]);

    addsig(SIGPIPE,SIG_IGN);

    unordered_map<string,RpcCallback> func_map;
    func_map.insert(make_pair("add",add_func));
    pthread_t* thread=new pthread_t[5];
    int temp_port=10002;
    RpcServer* rpc_server=new RpcServer[5]{RpcServer("172.19.22.228","10002",func_map,"172.19.22.228","9999"),RpcServer("172.19.22.228","10003",func_map,"172.19.22.228","9999"),RpcServer("172.19.22.228","10004",func_map,"172.19.22.228","9999"),RpcServer("172.19.22.228","10005",func_map,"172.19.22.228","9999"),RpcServer("172.19.22.228","10006",func_map,"172.19.22.228","9999")};

    for(int i=0;i<5;i++){
        // rpc_server[i].name=Rpc::IntToString(temp_port++);
        pthread_create(thread+i,nullptr,[](void* argc)->void*{
            
            RpcServer* rpc_server=(RpcServer*)argc;
            rpc_server->SetKeeper("192.168.83.129","9999");
            rpc_server->loop();

            delete rpc_server;
            
            return nullptr;
        },rpc_server+i);
        pthread_detach(*(thread+i));
    }
    while(1);
    
    return 0;
}