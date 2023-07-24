#ifndef IMAGINE_MAPREDUCE_LINERECORDREADER_H
#define IMAGINE_MAPREDUCE_LINERECORDREADER_H

#include<unistd.h>
#include<fcntl.h>
#include <memory>
#include"RecordReader.h"

namespace Imagine_MapReduce{

// template<typename key,typename value>
// 逐行读取文本文件的记录 ，继承模板类
// 行记录读取器 == 一行的 分块，分块id
class LineRecordReader : public RecordReader<int,std::string>
{

public:

    LineRecordReader(InputSplit* split_=nullptr, int split_id_=0);// 一个分块，分块id

    ~LineRecordReader(){};

    bool NextKeyValue();

    int GetCurrentKey();

    std::string GetCurrentValue();

    double GetProgress();// 获取当前读取进度

    void Close();        // 关闭RecordReader

    std::shared_ptr<RecordReader<int,std::string>> CreateRecordReader(InputSplit* split_, int split_id_);

    bool ReadLine();    // 从文件中读取一行文本

protected:
    int offset;           // 当前偏移量
    std::string line_text;// 当前行文本

};

// template<typename key,typename value>
LineRecordReader::LineRecordReader(InputSplit* split_, int split_id_):RecordReader(split_,split_id_)
{
    if(split){
        offset=split->GetOffset();// 分块的偏移量
        if(offset){
            ReadLine();           // 偏移量位置处的行文本
        }
    }
}

// template<typename key,typename value>
// 更新偏移量为当前行文本的大小，检查偏移量是否超出了分片的范围
bool LineRecordReader::NextKeyValue()
{
    offset+=line_text.size();//下一键值对的起始位置
    if(offset>=split->GetLength()+split->GetOffset())return false;//lenth+split是下一段的起始位置
    line_text.clear();
    return ReadLine();// 根据偏移量，读取下一键值对的文本
}

// template<typename key,typename value>
int LineRecordReader::GetCurrentKey()
{
    return offset;    // 当前记录的偏移量作为键值
}

// template<typename key,typename value>
std::string LineRecordReader::GetCurrentValue()
{
    return line_text.substr(0,line_text.size()-2);
    // 获取除去最后两个字符的子字符串
}

// template<typename key,typename value>
double LineRecordReader::GetProgress()
{
    return (offset-split->GetOffset())*1.0/split->GetLength();
}

// template<typename key,typename value>
void LineRecordReader::Close()
{

}

std::shared_ptr<RecordReader<int,std::string>> LineRecordReader::CreateRecordReader(InputSplit* split_, int split_id_)
{
    return std::make_shared<LineRecordReader>(split_,split_id_);
}
// 从文件的分区中offset读取一行文本
// ReadLine 函数实现了逐字符读取文件，并将字符逐个添加到 line_text 字符串中，
// 直到读取到一行文本的末尾。读取过程中会根据回车符判断行的结束
bool LineRecordReader::ReadLine()
{
    int fd=open(&split->GetFileName()[0],O_RDWR);// 从split 对象获取的文件名
    lseek(fd,offset,SEEK_SET);                   // 将文件偏移量设置为 offset，即当前记录的偏移量位置
    bool flag=false;                             // 标记是否遇到回车符
    while(1){
        char c;
        int ret=read(fd,&c,1);
        if(ret){
            line_text.push_back(c);
            if(flag&&c=='\n'){
                break;
            }
            if(c=='\r')flag=true;               // 是否为回车符
            else flag=false;
        }else{//读完了,文件不以\r\n结尾
            line_text+="\r\n";
            break;
        }
    }

    close(fd);
    return true;
}


}


#endif