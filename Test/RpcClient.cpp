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

int main(int argc,char* argv[]){

    if(argc<=1){
        printf("please input two parameter at least!\n");
        return 0;
    }

    //int port=atoi(argv[1]);

    addsig(SIGPIPE,SIG_IGN);

    // unsigned int ip=Rpc::ConvertIpFromStringToNet("3.4.5.6");
    // unsigned short int po=Rpc::ConvertPortFromStringToNet("3456");

    // cout<<ip<<endl;
    // cout<<Rpc::ConvertIpFromNetToString(ip)<<endl;
    // cout<<po<<endl;
    // cout<<Rpc::ConvertPortFromNetToString(po)<<endl;
        string name="add";
        //string para_num="2\r\n";
        int value_a=1;
        int value_b=1;
        int sum;
        int time=3;
        string ip="172.19.22.228";
        string port="9999";
        //string port="10002";
        while(1){
            string para_a=Rpc::IntToString(value_a);
            string para_b=Rpc::IntToString(value_b);

            vector<string> parameters;
            parameters.push_back(para_a);
            parameters.push_back(para_b);
            vector<string> recv_=RpcClient::Caller(name,parameters,ip,port);
            //vector<string> recv_=RpcClient::Call(name,parameters,ip,port);
            sum=Rpc::StringToInt(recv_[0]);

            //printf("\n\nmain get return :\n");
            
            value_a=value_b;
            value_b=sum;
            printf("第 %d 个数的值为 : %d\n",time++,sum);
            // sleep(1);

            printf("\n");

            //sleep(1);
        }

    return 0;
}