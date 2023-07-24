#ifndef IMAGINE_MAPREDUCE_PARTITIONER_H
#define IMAGINE_MAPREDUCE_PARTITIONER_H

#include"Callbacks.h"

namespace Imagine_MapReduce{
// ���ݼ������ͽ����ݷ�������ͬ�ķ�����
template<typename key>
class Partitioner
{
    
public:

    Partitioner(int partition_num_=DEFAULT_PARTITION_NUM):partition_num(partition_num_){}

    virtual ~Partitioner(){}

    virtual int Partition(key key_)=0; // ���ڼ��Ĺ�ϣֵ��Χ����

protected:

    const int partition_num;
};



}


#endif