#ifndef IMAGINE_MAPREDUCE_RECORDREADER_H
#define IMAGINE_MAPREDUCE_RECORDREADER_H

#include"InputSplit.h"

namespace Imagine_MapReduce{


// 记录读取器（分块，分块id，文件名）
template <typename reader_key, typename reader_value>
class RecordReader
{
public:

    RecordReader(InputSplit* split_=nullptr, int split_id_=0):split(split_),split_id(split_id_),file_name(split_!=nullptr?split_->GetFileName():""){}

    virtual ~RecordReader()
    {
        if(split)delete split;
        printf("delete recordreader!\n");
    }

    virtual bool NextKeyValue()=0;

    virtual reader_key GetCurrentKey()=0;

    virtual reader_value GetCurrentValue()=0;
 
    virtual double GetProgress()=0;// 获取当前读取进度

    virtual void Close()=0;        // 关闭RecordReader

    virtual std::shared_ptr<RecordReader<reader_key,reader_value>> CreateRecordReader(InputSplit* split_=nullptr, int split_id_=0)=0;//创建自己类型返回
    // 设置输出文件名
    void SetOutputFileName(const std::string& name)
    {
        file_name = name;
    }
    // 分块ID
    int GetSplitId(){return split_id;}

    std::string GetFileName()
    {
        return file_name;
    }

protected:

    const int split_id;         // 分块ID
    const std::string file_name;// 输出文件名

    InputSplit* split;          // 分块对象
};

}


#endif