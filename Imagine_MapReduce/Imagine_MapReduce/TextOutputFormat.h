#ifndef IMAGINE_MAPREDUCE_TEXTOUTPUTFORMAT_H
#define IMAGINE_MAPREDUCE_TEXTOUTPUTFORMAT_H

#include<string>
#include<memory.h>

#include"OutputFormat.h"
#include"MapReduceUtil.h"

namespace Imagine_MapReduce{
// 将键值对格式化为文本输出
class TextOutputFormat : public OutputFormat<std::string,int>
{

public:

    TextOutputFormat(){};

    ~TextOutputFormat(){};

    std::pair<char*,char*> ToString(std::pair<std::string,int> content)
    {
        char* key=new char[content.first.size()+1];// +1额外分配空字符\0结尾
        // char* value=new char[sizeof(content.second)+1];
        char* value=new char[1];// 存\0空字符
        
        char c='\0';
        memcpy(key,&((content.first)[0]),content.first.size());
        // memcpy(value,&(content.second),sizeof(content.second));
        memcpy(key+content.first.size(),&c,1);//空字符 '\0'
        memcpy(value,&c,1);
        // memcpy(value+sizeof(content.second),&c,1);

        // printf("size total is %d.........................................key is %s..............\n",content.first.size(),&content.first[0]);

        return std::make_pair(key,value);
    }
};

}


#endif