#ifndef IMAGINE_MAPREDUCE_CALLBACKS_H
#define IMAGINE_MAPREDUCE_CALLBACKS_H

#include<string.h>
#include<functional>
#include<memory>

namespace Imagine_MapReduce{

template<typename reader_key,typename reader_value>
class RecordReader;

//map函数
template<typename reader_key,typename reader_value,typename key,typename value>
using MapCallback=std::function<std::pair<key,value>(reader_key,reader_value)>;

//reduce函数
using ReduceCallback=std::function<void(const std::string&)>;

//mapper定时向master发送进度的回调函数
template<typename reader_key,typename reader_value>
using MapTimerCallback=std::function<void(int,std::shared_ptr<RecordReader<reader_key,reader_value>>)>;

//reducer定时向master发送进度的回调函数



//map函数
#define MAP MapCallback<reader_key,reader_value,key,value>

//reduce函数
#define REDUCE ReduceCallback

//mapper定时向master发送进度的回调函数
#define MAPTIMER MapTimerCallback<reader_key,reader_value>


#define DEFAULT_META_SIZE 16//一个meta元数据索引的大小
#define DEFAULT_SPILL_SIZE 0.2//默认spill触发大小
#define DEFAULT_READ_SPLIT_SIZE 1024*1024*128//默认每次读100m
#define DEFAULT_SPLIT_BUFFER_SIZE 1024*1024*100//默认spilt缓冲区大小
#define DEFAULT_AVG_LINE_SIZE 100//平均一行大小(预读)
#define DEFAULT_MAP_SHUFFLE_NUM 5//默认shuffle大小
#define DEFAULT_PARTITION_NUM 5//默认分区数目
#define DEFAULT_DISK_MERGE_NUM 5//Reduce的Copy阶段,磁盘文件开始merge的文件阈值数
#define DEFAULT_MEMORY_MERGE_SIZE 1024*1024*100//Reduce的Copy阶段,内存空间开始merge的大小
#define DEFAULT_REDUCER_NUM DEFAULT_PARTITION_NUM

/*
    -用于多路归并排序的kv读取(基于默认方式)
        -" "作为key和value的分隔符
        -"\r\n"作为每一对数据的分隔符
*/
class KVReader{
        
    public:
        KVReader(std::string key_, std::string value_, int idx_):reader_key(key_),reader_value(value_),reader_idx(idx_){}

    public:
        std::string reader_key;
        std::string reader_value;
        int reader_idx;
};

class KVReaderCmp{
    public:
        bool operator()(KVReader* a, KVReader* b)
        {
            return strcmp(&a->reader_key[0],&b->reader_key[0])<0?true:false;
        }
};

class HashPair{
    public:
        template<typename first,typename second>
        std::size_t operator()(const std::pair<first,second>& p)const
        {
            auto hash1=std::hash<first>()(p.first);
            auto hash2=std::hash<second>()(p.second);
            return hash1^hash2;
        }
};

class EqualPair{
    public:
        template<typename first,typename second>
        bool operator()(const std::pair<first,second>& a, const std::pair<first,second>& b)const
        {
            return a.first==b.first&&a.second==b.second;
        }
};

}


#endif