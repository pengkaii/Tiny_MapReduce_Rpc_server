#include"MapReduceUtil.h"
#include<fcntl.h>
#include<unistd.h>
#include<vector>
#include<sys/stat.h>

using namespace Imagine_MapReduce;
// Map函数==读到一个分区 拆分为键值对，键的哈希值写入中间结果文件shuffle中
void MapReduceUtil::DefaultMapFunction(const std::string& read_split)
{
    int idx=read_split.find_first_of('\r')+2; // 索引跳过换行符 \r\n
    std::unordered_map<std::string,int> kv_map;
    //kv_map.insert(std::make_pair("sdfasdf",3));
    //DefaultMapFunctionHashHandler(read_split,kv_map);
    std::vector<int> shuffle_fd;
    //  打开并创建一组用于写入中间结果的文件描述符  == 文件描述符表示该文件名的唯一标识
    for(int i=0;i<DEFAULT_MAP_SHUFFLE_NUM;i++){
        shuffle_fd.push_back(open(&("shuffle"+MapReduceUtil::IntToString(i)+".txt")[0],O_CREAT|O_RDWR,0777));
    }

    auto hash_func=kv_map.hash_function();
    // for(std::unordered_map<std::string,int>::iterator it=kv_map.begin();it!=kv_map.end();it++){
    //     int hash_fd=hash_func(it->first)%5;
    //     write(shuffle_fd[hash_fd],&it->first[0],it->first.size());
    //     write(shuffle_fd[hash_fd],"\r\n",2);
    //     //printf("string %s occur %d times!\n",&(it->first[0]),it->second);
    // }
    // 输入字符串中的键值对逐个提取出来，并根据哈希值将键写入到对应的中间结果文件中。
    // 同时，每个键值对之间添加了一个换行符作为分隔符。这样，中间结果文件中就存储了按哈希值分配的键值对数据。
    for(int i=idx+1;i<read_split.size();i++){// 从回车符后的第一个字符开始
        if(read_split[i]=='\r'&&read_split[i+1]=='\n'){// 找到了一个完整的键值对，需要进行处理
            std::string temp_string=read_split.substr(idx,i-idx);// 从索引 idx 到 i-1 的子字符串，即提取出一个键值对的字符串 temp_string
            int hash_fd=hash_func(temp_string)%5;// 哈希值
            int ret=write(shuffle_fd[hash_fd],&temp_string[0],temp_string.size());
            // 键 temp_string 写入到对应的中间结果文件中，使用文件描述符 
            // printf("ret is %d\n",ret);
            write(shuffle_fd[hash_fd],"\r\n",2);// 确保每个键值对之间有一行空白
            idx=i+2;// 指向下一个键值对的起始位置
            i++;    // 处理下一个键值对，直到遍历完整个输入字符串
        }
    }
    // 关闭文件描述符  == 完成对中间结果文件的写入操作后，关闭相应的文件描述符，释放相关的资源
    for(int i=0;i<DEFAULT_MAP_SHUFFLE_NUM;i++){
        close(shuffle_fd[i]);
    }
}
// 处理输入字符串 input 并更新无序哈希映射 kv_map("sdfasdf",3)
void MapReduceUtil::DefaultMapFunctionHashHandler(const std::string& input, std::unordered_map<std::string,int>& kv_map)
{
    int idx=0;// 记录当前处理的子字符串的起始索引
    for(int i=0;i<input.size();i++){
        if(input[i]=='\r'&&input[i+1]=='\n'){// 则表示找到了一个完整的键值对，需要进行处理
            std::unordered_map<std::string,int>::iterator it=kv_map.find(input.substr(idx,i-idx));
            //提取出一个键的字符串，查找键是否存在，返回一个迭代器 it
            //printf("get string %s\n",&(input.substr(idx,i-idx))[0]);
            if(it==kv_map.end()){
                kv_map.insert(std::make_pair(input.substr(idx,i-idx),1));// 插入到哈希映射中，键为提取的子字符串，值初始化为 1
            }else{
                it->second++;
            }
            idx=i+2;// 使其指向下一个键值对的起始位置。
            i++;  // 处理下一个键值对，直到遍历完整个输入字符串
        }
    }
}
// 从文件中读取数据，将其分割为块，并使用哈希处理函数逐块处理
void MapReduceUtil::DefaultReduceFunction(const std::string& input)
{
    int file_fd=open(&input[0],O_RDWR);// 指定的文件，以读写模式打开，并返回一个文件描述符
    if(file_fd==-1){
        perror("open");
    }

    fcntl(file_fd,O_NONBLOCK);// 非阻塞==当读取文件时，如果没有数据可用，read函数将立即返回而不会等待
    std::string tail_string;  // 保存读取数据时剩余的未处理部分
    std::unordered_map<std::string,int> kv_map;
    while(1){                 // 找到一个完整行（\r\n结尾），继续tail_string==后面未处理数据的 读取
        bool flag=true;       // 是否找到完整的数据行
        std::string read_string=tail_string;// 将剩余的未处理部分赋值给作为待处理的字符串
        read_string.resize(tail_string.size()+DEFAULT_READ_SPLIT_SIZE);
        // 从文件fd读到 read_string
        int ret=read(file_fd,&read_string[tail_string.size()],DEFAULT_READ_SPLIT_SIZE);
        if(ret==-1){
            perror("read");
        }
        if(!ret)break;
        read_string.resize(tail_string.size()+ret);//将其缩小到实际读取的字节数
        tail_string.resize(0);//清空剩余未处理部分的字符串
        // 在read_string中检测回车换行符，如果找到则表示找到了一行完整的数据。
        // 将未处理部分的字符串保存到tail_string中，并将read_string的大小调整为已处理的数据部分。这样，
        //在下一次循环迭代中，read_string将只包含未处理的数据部分，而tail_string将保存之前未处理完的数据。
        for(int i=read_string.size()-1;i>=0;i--){
            // 如果找到回车换行符"\r\n"，表示找到了一行完整的数据
            if(i>0&&read_string[i]=='\n'&&read_string[i-1]=='\r'){
                if(i==read_string.size()-1)break;//刚好读完一句
                // 将剩余未处理部分的字符串赋值为从回车换行符后面开始的部分
                tail_string=read_string.substr(i+1,read_string.size()-i-1);
                read_string.resize(i+1);
                //大小调整为回车换行符前的部分，即将已经处理完整的数据部分保留下来，其他部分被丢弃
                break;
            }
            if(i==0){//找不到
                // 以便在下一次循环中继续处理未完整的数据
                flag=false;//当在read_string中无法找到回车换行符时（即整个字符串都没有完整的数据行）
                tail_string=read_string;//以便在下一次循环中继续处理未完整的数据
                break;
            }
        }
        // 找到了完整的数据行，找到一个完整行（\r\n结尾），进行处理
        if(flag)DefaultReduceFunctionHashHandler(read_string,kv_map);
    }
}
// shuffle :[Bear 1]、[Bear 1] ==> Reduce [Bear 2]
void MapReduceUtil::DefaultReduceFunctionHashHandler(const std::string& input, std::unordered_map<std::string,int>& kv_map)
{    
    int idx=0;
    int split_idx;//空格位置
    for(int i=0;i<input.size();i++){
        if(input[i]==' '){
            split_idx=i;
        }else if(input[i]=='\r'&&input[i+1]=='\n'){
            // 根据键查找映射中的元素，判断是否已存在该键
            std::unordered_map<std::string,int>::iterator it=kv_map.find(input.substr(idx,split_idx-idx));
            if(it==kv_map.end()){
                // 键Bear,值1（value_字符数组转化为整型1）
                // 如果不存在，则将该键和对应的值（从 split_idx+1 到 i）作为一个新的键值对插入到 kv_map 中
                kv_map.insert(std::make_pair(input.substr(idx,i-idx),StringToInt(input.substr(split_idx+1,i-split_idx-1))));
            }else{
                it->second+=StringToInt(input.substr(split_idx+1,i-split_idx-1));// 键值对数据中值的长度
            }
            idx=i+2;
            i++;//下一键值对
        }
    }
}
//将输入文件拆分为多个InputSplit对象，
//并将这些对象存储在一个std::vector<InputSplit*>容器中，最后返回该容器
//读文件的一个分块到分块数组里面
std::vector<InputSplit*> MapReduceUtil::DefaultReadSplitFunction(const std::string& file_name, int split_size)
{   // 声明一个指向InputSplit对象的指针容器splits，用于存储拆分后的输入数据块
    std::vector<InputSplit*> splits;
    struct stat statbuf;// 获取文件的状态信息
    int offset = 0;       // 分块的偏移位置
    // &file_name[0] 是获取file_name字符串的第一个字符的地址。在C++中，字符串是字符数组，可以通过访问第一个字符的地址来获取整个字符串的指针。
    int ret=stat(&file_name[0],&statbuf);//获取文件的状态信息，并将结果存储在statbuf中
    if(ret==-1){
        perror("stat");
    }
    // split_size == 一个分块大小
    // 文件的大小是否仍然足够进行下一个拆分块。如果是，则继续创建拆分块，并更新文件大小和偏移量
    while(statbuf.st_size>=split_size){
        // 创建一个InputSplit对象，表示一个拆分块，然后将该对象的指针添加到splits容器中
        splits.push_back(new InputSplit(file_name,offset,split_size));
        statbuf.st_size-=split_size;// 更新文件的大小，减去当前拆分块的大小
        offset+=split_size;         // 更新分块偏移量，增加当前拆分块的大小
    }
    // 处理文件大小不足以形成完整拆分块
    // 确保文件还有剩余的未处理部分，new表示剩余的未处理部分作为一个拆分块
    if(statbuf.st_size)splits.push_back(new InputSplit(file_name,offset,statbuf.st_size));

    // for(int i=0;i<splits.size();i++){
    //     printf("%d\n",splits[i]->GetLength());
    // }

    return splits;
}

int MapReduceUtil::StringToInt(const std::string& input)
{
    int output=0;
    int size=input.size();
    for(int i=0;i<size;i++){
        output=output*10+input[i]-'0';
    }

    return output;
}

std::string MapReduceUtil::IntToString(int input)
{
    std::string output_;
    std::string output;
    
    if(!input)return "0";
    while(input){
       output_.push_back(input%10+'0');
       input/=10;
    }

    for(int i=output_.size()-1;i>=0;i--){
        output.push_back(output_[i]);
    }

    return output;
}

std::string MapReduceUtil::DoubleToString(double input)
{
    int time=0;
    int integer_part=(int)input;
    std::string output=IntToString(integer_part);
    output.push_back('.');
    input-=integer_part;
    while(input&&time<2){
        time++;
        int temp_value=input*10;
        output+=IntToString(temp_value);
        input=input*10-temp_value;
    }

    if(!time)output.push_back('0');

    return output;
}
// 用于从内存或磁盘中读取键值对数据
bool MapReduceUtil::ReadKVReaderFromMemory(const std::string& content, int& idx, std::string& key_, std::string& value_)//从spill文件读取kv对
{
    //假设空格是key和value的分隔符,且每个kv以\r\n结尾,且kv中不包含这三个字符
    if(idx>=content.size())return false;//已经读取完所有的数据，
    int start_idx=idx;//记录每个键值对的起始位置
    while(content[++idx]!=' ');//找到空格字符，表示键和值之间的分隔符
    key_=content.substr(start_idx,idx-start_idx);//从 start_idx 到 idx 的子串作为键存储到 key_ 中

    bool flag=false;
    start_idx=idx+1;
    while(1){//是否找到了完整的值
    // 找到回车换行符 \r\n 并且 flag 为真，表示找到了完整的值
        if(content[idx]=='\n'&&flag){
            value_=content.substr(start_idx,idx-start_idx-1);
            idx++;//指向下一个键值对的起点位置
            return true;
        }
        if(content[idx]=='\r')flag=true;
        else flag=false;
        idx++;
    }
}
// 从磁盘中读取键值对数据的函数  fd文件描述符，用于从文件中读取数据
// 键存储到 key_ 中，将值存储到 value_ 中，并返回读取结果
bool MapReduceUtil::ReadKVReaderFromDisk(const int fd, std::string& key_, std::string& value_)
{
    //假设空格是key和value的分隔符,且每个kv以\r\n结尾,且kv中不包含这三个字符
    bool flag=false;//是否找到回车符 \r
    char c;
    int ret=read(fd,&c,1);
    if(!ret)return false;//文件读空
    
    key_.push_back(c);
    while(1){
        // printf("here\n");
        read(fd,&c,1);
        if(c==' ')break;//找到空格字符，表示键和值之间的分隔符
        key_.push_back(c);
    }
    while(1){
        read(fd,&c,1);
        value_.push_back(c);
        if(c=='\n'&&flag){//找到了完整的值
            value_.pop_back();//最后两个字符（即回车换行符）移除
            value_.pop_back();
            return true;
        }
        if(c=='\r')flag=true;
        else flag=false;
    }
}
// 于将键值对数据写入磁盘文件，并从多个文件中合并键值对数据到一个文件中
bool MapReduceUtil::WriteKVReaderToDisk(const int fd, const KVReader* const kv_reader)
{   // 将键值对数据写入磁盘文件
    char c=' ';//空格字符 ' ' 写入文件，作为键和值之间的分隔符
    char cc[3]="\r\n";//第三个元素存储字符串的结束符 \0
    write(fd,&kv_reader->reader_key[0],kv_reader->reader_key.size());    //键
    write(fd,&c,1);
    write(fd,&kv_reader->reader_value[0],kv_reader->reader_value.size());//值
    write(fd,cc,2);

    return true;//成功写入键值对数据
}
// 合并多个文件中的键值对数据到一个文件中
// 将键值对数据写入磁盘文件，并从多个文件中合并键值对数据到一个文件中。
// 这在MapReduce任务中的Reduce阶段常常被用到

// 迭代处理多个文件描述符，从每个文件中读取键值对数据，并将读取成功的键值对数据封装为 KVReader 对象，
// 最后将这些对象按优先级（根据 KVReaderCmp 比较器）存储在优先级队列 heap_ 中
bool MapReduceUtil::MergeKVReaderFromDisk(const int* const fds, const int fd_num, const std::string& file_name)
{   //存储多个KVReader对象，并根据其比较器 KVReaderCmp 进行优先级排序
    std::priority_queue<KVReader*,std::vector<KVReader*>,KVReaderCmp> heap_;
    // 创建一个新的目标文件，并返回其文件描述符 fd
    int fd=open(&file_name[0],O_CREAT|O_RDWR,0777);// 访问第一个字符的地址来获取整个字符串==指定磁盘
    // for 循环用于迭代处理多个文件描述符，从每个文件中读取键值对数据
    // 并将新创建的 KVReader 对象放入优先级队列 heap_ 中
    for(int i=0;i<fd_num;i++){
        std::string key_;
        std::string value_;
        // 使用ReadKVReaderFromDisk函数从对应的文件中读取键值对数据。
        // 如果成功读取，将新创建的KVReader对象放入heap_中
        if(ReadKVReaderFromDisk(fds[i],key_,value_)) heap_.push(new KVReader(key_,value_,fds[i]));
    }
    while(heap_.size()){
        KVReader* next_=heap_.top();
        heap_.pop();
        std::string key_;
        std::string value_;
        // 使用ReadKVReaderFromDisk函数从文件描述符 fds[i] 对应的文件中读取键值对数据
        // 如果成功读取，将新创建的KVReader对象放入heap_中。
        // 在读取成功的情况下，使用 new KVReader(key_, value_, fds[i]) 创建一个新的 KVReader 对象，并将该对象的指针放入优先级队列 heap_ 中。
        // 对应的文件中读取键值对数据。如果成功读取，说明文件中还有键值对数据。
        if(ReadKVReaderFromDisk(next_->reader_idx,key_,value_))heap_.push(new KVReader(key_,value_,next_->reader_idx));
        WriteKVReaderToDisk(fd,next_);//将next_中的键值对数据写入目标文件fd

        delete next_;
    }

    close(fd);

    return true;//成功合并键值对数据到一个文件中
}