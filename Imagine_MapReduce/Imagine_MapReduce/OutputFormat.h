#ifndef IMAGINE_MAPREDUCE_OUTPUTFORMAT_H
#define IMAGINE_MAPREDUCE_OUTPUTFORMAT_H
#include <string>
namespace Imagine_MapReduce{

template<typename key,typename value>
class OutputFormat
{

public:

    OutputFormat(){};

    virtual ~OutputFormat(){};
    // ���Ϊ ��ֵ��  ���ַ���
    virtual std::pair<char*,char*> ToString(std::pair<key,value> content)=0;
};


}


#endif