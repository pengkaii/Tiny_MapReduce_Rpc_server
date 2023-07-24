#ifndef IMAGINE_MAPREDUCE_KVBUFFER_H
#define IMAGINE_MAPREDUCE_KVBUFFER_H

#include<fcntl.h>

#include"MapReduceUtil.h"
#include"Callbacks.h"

namespace Imagine_MapReduce{

// 在缓冲区中，键值对数据和元数据索引是分开存储的。键值对数据存储着实际的键和值，
// 而元数据索引则存储着键值对在缓冲区中的位置和长度等信息，用于快速定位和访问键值对
class KVBuffer
{
public:
    // KV缓冲区中的元数据索引
    // 在键值对缓冲区中，每个键值对都有对应的元数据
    // MetaIndex是KVBuffer的内部类，可访问并共享KVBuffer类的私有成员和方法，从而实现了对键值对缓冲区的有效管理和操作
    class MetaIndex{
        public:
            MetaIndex(int key_idx_, int value_idx_, int key_len_, int value_len_, int partition_idx_, char* buffer_, int border_)
            :key_idx(key_idx_),value_idx(value_idx_),key_len(key_len_),value_len(value_len_),partition_idx(partition_idx_),buffer(buffer_),border(border_)
            {
            }

            ~MetaIndex(){}
            // 获取键  key字符数组（长度key_len)
            char* GetKey(char* key)const
            {   // 跨越缓冲区边界读取键
                if(key_idx+key_len-1>=border){
                    // 从缓冲区中复制键的第一部分
                    memcpy(key,buffer+key_idx,border-key_idx);
                    // 从缓冲区的开头复制键的剩余部分
                    memcpy(key+border-key_idx,buffer,key_len-(border-key_idx));
                }else{
                // 在缓冲区内读取键
                    memcpy(key,buffer+key_idx,key_len);
                }
                return key;
            }

            int GetKeyLen()const
            {
                return key_len;
            }

            char* GetValue(char* value)const
            {
                if(value_idx+value_len-1>=border){
                    memcpy(value,buffer+value_idx,border-value_idx);
                    memcpy(value+border-value_idx,buffer,value_len-(border-value_idx));
                }else{
                    memcpy(value,buffer+value_idx,value_len);
                }
                return value;
            }

            int GetValueLen()const
            {
                return value_len;
            }
            // 获取分区索引 （file分几块）
            int GetPartition()const
            {
                return partition_idx;
            }
            // 从给定的缓冲区中提取元数据，并根据提取的信息创建一个MetaIndex对象
            // buffer_元数据缓冲区，border_元数据边界
            static MetaIndex GetMetaIndex(char* buffer_, int meta_idx_, int border_)
            {   // 元数据信息
                char meta_info[DEFAULT_META_SIZE];
                //  如果元数据索引加上 默认元数据大小 超过了元数组缓冲区边界，需要分两次读取元数据
                if(meta_idx_+DEFAULT_META_SIZE>border_){
                    //分两次读
                    memcpy(meta_info,buffer_+meta_idx_,border_-meta_idx_);
                    memcpy(meta_info+border_-meta_idx_,buffer_,DEFAULT_META_SIZE-(border_-meta_idx_));
                }else{
                    memcpy(meta_info,buffer_+meta_idx_,DEFAULT_META_SIZE);
                }
                // 元数据 == 键索引 + 值索引 + 值长度 + 分区索引
                int key_idx;
                memcpy(&key_idx,meta_info,sizeof(key_idx));
                int value_idx;
                memcpy(&value_idx,meta_info+sizeof(key_idx),sizeof(value_idx));
                // 计算键的长度
                int key_len=key_idx<value_idx?value_idx-key_idx:border_-key_idx+value_idx;
                int value_len;
                memcpy(&value_len,meta_info+sizeof(key_idx)+sizeof(value_idx),sizeof(value_len));
                int partition_idx;
                memcpy(&partition_idx,meta_info+sizeof(key_idx)+sizeof(value_idx)+sizeof(value_len),sizeof(partition_idx));
                // printf("key idx is %d, value idx is %d\n",key_idx,value_idx);
                // printf("key len is %d, value len is %d。。。。。。。。。。。。。。。。。\n",key_len,value_len);
                return MetaIndex(key_idx,value_idx,key_len,value_len,partition_idx,buffer_,border_);
            }

        private:

            int key_idx;
            int value_idx;
            int key_len;
            int value_len;
            int partition_idx;// 分区索引

            char* buffer;     // 元数据缓冲区
            int border;       // 元数据缓冲区的边界
    };
    // 重载函数调用运算符 operator()，比较两个MetaIndex对象
    class MetaCmp{
        public:
            bool operator()(const MetaIndex& a, const MetaIndex& b)
            {   // key的字符数组
                char* key_a=new char[a.GetKeyLen()+1];//加1 是为了存字符串结尾的 '\0'
                char* key_b=new char[b.GetKeyLen()+1];
                // 获取对象 a 和 b 的键，并将其存到字符数组中
                a.GetKey(key_a);
                b.GetKey(key_b);
                key_a[a.GetKeyLen()]='\0';
                key_b[b.GetKeyLen()]='\0';
                // 使用 strcmp 函数比较两个键的大小
                bool flag;
                flag=strcmp(key_a,key_b)<0?true:false;

                delete [] key_a;
                delete [] key_b;

                return flag;
            }
    };

public:
    // 分区数量、分区id、溢出文件
    KVBuffer(int partition_num_, int split_id_, std::vector<std::vector<std::string>>& spill_files_)
        :buffer_size(DEFAULT_SPLIT_BUFFER_SIZE),spill_size(DEFAULT_SPILL_SIZE),partition_num(partition_num_),split_id(split_id_),spill_files(spill_files_)
    {
        // 初始化 缓冲区互斥锁、溢写互斥锁、溢写条件变量
        buffer_lock=new pthread_mutex_t;
        if(pthread_mutex_init(buffer_lock,nullptr)!=0){
            throw std::exception();
        }

        spill_lock=new pthread_mutex_t;
        if(pthread_mutex_init(spill_lock,nullptr)!=0){
            throw std::exception();
        }

        spill_cond=new pthread_cond_t;
        if(pthread_cond_init(spill_cond,nullptr)!=0){
            throw std::exception();
        }
        // 分配缓冲区内存
        buffer=new char[buffer_size];
        // equator=kv_idx=kv_border=meta_border=0;
        // meta_idx=buffer_size-1;
        // 初始化各种索引和标志变量
        meta_idx=equator=kv_border=meta_border=0;
        kv_idx=1;
        is_spilling=false;

        spill_id=1;
        spill_buffer=false;
        quit=false;
        // 调整溢写文件的大小
        spill_files.resize(partition_num);
    }

    ~KVBuffer()
    {
        printf("delete buffer\n");
        delete [] buffer;
        delete buffer_lock;
        delete spill_lock;
        delete spill_cond;
    }
    // 缓冲区写入和溢写
    // 键值对数据content写入KV缓冲区，分到指定的分区 partition_idx
    bool WriteToBuffer(const std::pair<char*,char*>& content, int partition_idx);
    // 缓冲区溢写，缓冲区数据写入磁盘
    bool Spilling();
    // 缓冲区剩余空间（键值对）、元数据索引剩余空间 < 新加的键值对 content 的长度，需要溢写
    bool WriteJudgementWithSpilling(const std::pair<char*,char*>& content)
    {
        // KV键值对的边界（键、值数据区） 跟缓冲区大小不一样
        if((kv_idx<=kv_border?kv_border-kv_idx:buffer_size-kv_idx+kv_border)<strlen((char*)(content.first))+strlen((char*)(content.second))||(meta_border<=meta_idx?meta_idx-meta_border:meta_idx+buffer_size-meta_border)<DEFAULT_META_SIZE){
            return false;
        }

        return true;
    }
    // 键值对长度、当前缓冲区中的键值对和元数据索引的位置，判断是否有足够的空间将键值对写入缓冲区
    bool WriteJudgementWithoutSpilling(const std::pair<char*,char*>& content)
    {
        if(meta_idx<kv_idx?buffer_size-(kv_idx-meta_idx-1):meta_idx-kv_idx+1>strlen((char*)(content.first))+strlen((char*)(content.second))+DEFAULT_META_SIZE){
            return true;
        }

        return false;
    }
    // 触发缓冲区的溢写操作
    bool SpillBuffer()
    {
        spill_buffer=true;
        while(!quit){
            //pthread_cond_signal(spill_cond);
            pthread_cond_broadcast(spill_cond);// 广播信号给等待的线程，通知它们开始进行溢写操作
        }

        return true;
    }


private:

    const int partition_num;// 划分区数 == file分几块
    const int split_id;     // 分区下标
    int spill_id;           // 自增字段，表示第几次溢写

    char* buffer;           // 环形缓冲区  存键值对和元数据
    const int buffer_size;
    int equator;           // 赤道点
    int kv_idx;            // 下一个可写入键值对的位置
    int meta_idx;          // 元数据下标，当前元数据索引在缓冲区中的位置
    bool is_spilling;      // 是否在溢写
    int kv_border;         // 溢写时的kv边界， 下一个键值对的位置
    int meta_border;       // 溢写时的索引边界，当前键值对的上一个位置

    const double spill_size;// 当缓冲区的空闲空间超过该空闲空间比阈值时，会触发溢写操作

    pthread_mutex_t* buffer_lock;

    pthread_cond_t* spill_cond; // 溢写条件变量
    pthread_mutex_t* spill_lock;// 溢写互斥锁

    std::vector<std::vector<std::string>>& spill_files;// 溢写文件

    bool spill_buffer;          // 缓冲区溢写
    bool quit;                  // 终止溢写
};

}




#endif