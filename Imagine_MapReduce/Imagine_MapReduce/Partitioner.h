#ifndef IMAGINE_MAPREDUCE_PARTITIONER_H
#define IMAGINE_MAPREDUCE_PARTITIONER_H

#include"Callbacks.h"

namespace Imagine_MapReduce{
// 根据键的类型将数据分区到不同的分区中
template<typename key>
class Partitioner
{
    
public:

    Partitioner(int partition_num_=DEFAULT_PARTITION_NUM):partition_num(partition_num_){}

    virtual ~Partitioner(){}

    virtual int Partition(key key_)=0; // 基于键的哈希值或范围划分

protected:

    const int partition_num;
};



}


#endif