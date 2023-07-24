#ifndef IMAGINE_MAPREDUCE_INPUTSPLIT_H
#define IMAGINE_MAPREDUCE_INPUTSPLIT_H

#include<string>

namespace Imagine_MapReduce{
// 输入数据的切片   == 分布式计算中划分数据块
class InputSplit
{
public:

    InputSplit(const std::string& file_name_, int offset_, int length_):file_name(file_name_),offset(offset_),length(length_){}

    ~InputSplit(){};

    int GetLength(){return length;}  // 切片的大小

    void GetLocations();             // 切片所在的位置信息

    void GetLocationInfo();          // 切片位置的详细信息

    std::string GetFileName(){return file_name;}// 切片对应的文件名
    
    int GetOffset(){return offset;}  // 切片的偏移量（起始位置）

private:
    std::string file_name;//文件名
    int offset;          //偏移量(起始位置)
    int length;          //分片大小
};



}



#endif