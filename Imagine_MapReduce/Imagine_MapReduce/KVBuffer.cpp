#include"KVBuffer.h"

using namespace Imagine_MapReduce;
// 键值对数据content写到环形缓冲区，分到指定的分区 partition_idx
bool KVBuffer::WriteToBuffer(const std::pair<char*,char*>& content, int partition_idx)
{
    bool flag=true;
    int key_len_=strlen(content.first);
    int value_len_=strlen(content.second);
    int key_point_;                    // 元数据 键点位置
    int value_point_;
    char* key_=content.first;          // 键字符数组
    char* value_=content.second;       // 值字符数组
    char meta_info_[DEFAULT_META_SIZE];// 元数据信息
    while(flag){
        //逻辑上保证kv_idx!=meta_idx恒成立,即永远不允许写满
        //kv_idx始终向上增长,meta_idx始终向下增长
        // printf("key is %d,value is %d!!!!!!!!!!!!!!!!!!!!!!!\n",key_len,value_len);
        pthread_mutex_lock(buffer_lock);
        if(is_spilling?WriteJudgementWithSpilling(content):WriteJudgementWithoutSpilling(content)){
            //空间足够写入
            flag=false;
            key_point_=kv_idx;//键值对位置（大缓冲区） 
            // 元数据信息 == 键索引+值索引+值长度+分区索引
            memcpy(meta_info_,&key_point_,sizeof(key_point_));
            memcpy(meta_info_+sizeof(key_point_)+sizeof(value_point_),&value_len_,sizeof(value_len_));
            memcpy(meta_info_+sizeof(key_point_)+sizeof(value_point_)+sizeof(value_len_),&partition_idx,sizeof(partition_idx));
            // 缓冲区剩余空间能不能写完键值对和元数据索引，元数据索引是否在元数据缓冲区末尾
            if(meta_idx<kv_idx&&(buffer_size-kv_idx<key_len_+value_len_||meta_idx+1<DEFAULT_META_SIZE)){
                // kv或meta不能一次写完
                // 缓冲区剩余空间写不下键值对数据，分两次写键值对
                if(buffer_size-kv_idx<key_len_+value_len_){
                    // 两次写入kv,一次写完meta
                    if(buffer_size-kv_idx>=key_len_){
                        // 能完整写入key
                        memcpy(buffer+kv_idx,key_,key_len_);
                        kv_idx=(kv_idx+key_len_)%buffer_size;
                        value_point_=(kv_idx+key_len_)%buffer_size;  // 应该等于kv_idx
                        memcpy(meta_info_+sizeof(key_point_),&value_point_,sizeof(value_point_));
                        // 环形缓冲区溢出写 value值
                        memcpy(buffer+kv_idx,value_,buffer_size-kv_idx);
                        memcpy(buffer,value_+buffer_size-kv_idx,value_len_-(buffer_size-kv_idx));//值从大缓冲区开始写
                        kv_idx=value_len_-(buffer_size-kv_idx);//键值对下标开始
                    }else{
                        // 不能完整写入key_字符数组，环形缓冲区分开重新写
                        memcpy(buffer+kv_idx,key_,buffer_size-kv_idx);
                        memcpy(buffer,key_+buffer_size-kv_idx,key_len_-(buffer_size-kv_idx));
                        kv_idx=key_len_-(buffer_size-kv_idx);

                        value_point_=kv_idx;
                        memcpy(meta_info_+sizeof(key_point_),&value_point_,sizeof(value_point_));
                        // 一次写入值
                        memcpy(buffer+kv_idx,value_,value_len_);
                        kv_idx+=value_len_;
                    }
                    // 写meta 向下增长：保每个元数据索引在缓冲区中的位置不会重叠
                    memcpy(buffer+meta_idx-DEFAULT_META_SIZE+1,meta_info_,strlen(meta_info_));
                    meta_idx-=DEFAULT_META_SIZE;
                    if(meta_idx+1==kv_idx) meta_idx=kv_idx;//缓冲区写满
                }else if(meta_idx+1<DEFAULT_META_SIZE){// 缓冲区剩余空间不能写一个元数据索引
                    //两次写入meta,一次写完kv
                    memcpy(buffer+kv_idx,key_,key_len_);
                    kv_idx+=key_len_;

                    value_point_=kv_idx;
                    memcpy(meta_info_+sizeof(key_point_),&value_point_,sizeof(value_point_));// 元数据更新值点位置

                    memcpy(buffer+kv_idx,value_,value_len_);
                    kv_idx+=value_len_;
                    //写meta
                    // 元数据索引的后半部分写入缓冲区的开头
                    // 元数据向下生长（元数组小地址写到缓冲区小地址）
                    memcpy(buffer,meta_info_+DEFAULT_META_SIZE-(meta_idx+1),meta_idx+1);
                    // 元数据索引的前半部分写入 缓冲区的末尾
                    // meta_idx+1 == 当前元数据索引部分的长度——————DEFAULT_META_SIZE-(meta_idx+1) 表示剩余未写入的元数据索引前半部分的长度
                    memcpy(buffer+buffer_size-DEFAULT_META_SIZE+(meta_idx+1),meta_info_,DEFAULT_META_SIZE-(meta_idx+1));
                    meta_idx=buffer_size-(DEFAULT_META_SIZE-(meta_idx+1))-1;
                    if(meta_idx+1==kv_idx)meta_idx=kv_idx;//写满
                }
            }else{
                memcpy(buffer+kv_idx,key_,key_len_);
                kv_idx+=key_len_;

                //printf("valuepoint is %d\n",kv_idx);
                value_point_=kv_idx;
                memcpy(meta_info_+sizeof(key_point_),&value_point_,sizeof(value_point_));

                memcpy(buffer+kv_idx,value_,value_len_);
                kv_idx=(kv_idx+value_len_)%buffer_size;
                // 元数据 向下增长
                memcpy(buffer+meta_idx-DEFAULT_META_SIZE+1,meta_info_,DEFAULT_META_SIZE);
                meta_idx-=DEFAULT_META_SIZE;
                if(meta_idx==-1){
                    // kv_idx 的位置更新为缓冲区的末尾位置或起始位置
                    if(kv_idx==0)meta_idx=0;
                    else meta_idx=buffer_size-1;
                }else if(meta_idx<kv_idx)meta_idx=kv_idx;//已满
            }
        }else{
            //主动休眠
        }
        //printf("content is key-%s,value-%s\n",content.first,content.second);
        pthread_mutex_unlock(buffer_lock);
        //（溢写操作触发的空闲空间比例阈值），则说明可用空间不够了，可以触发溢写操作  
        if((meta_idx<kv_idx?buffer_size-(kv_idx-meta_idx-1):meta_idx-kv_idx+1)*1.0/buffer_size<=spill_size){
            pthread_cond_signal(spill_cond);
        }
    }

    return true;
}

bool KVBuffer::Spilling()
{
    while(!spill_buffer||(meta_idx<kv_idx?kv_idx-meta_idx:buffer_size-(meta_idx-kv_idx))!=1){
        pthread_mutex_lock(spill_lock);

        pthread_mutex_lock(buffer_lock);
        //Spill结束,保证spilling在临界区改变值
        is_spilling=false;
        pthread_mutex_unlock(buffer_lock);
        // 可用空间 > spill溢写比例，当前线程阻塞等待溢写条件的发生
        while((!spill_buffer)&&(meta_idx<kv_idx?buffer_size-(kv_idx-meta_idx-1):meta_idx-kv_idx+1)*1.0/buffer_size>spill_size){
            pthread_cond_wait(spill_cond,spill_lock);
        }
        //开始spill
        pthread_mutex_unlock(spill_lock);
        pthread_mutex_lock(buffer_lock);
        is_spilling=true;
        int old_equator=equator;
        int old_kv_idx=kv_idx;
        int old_meta_idx=meta_idx%buffer_size;
        // 计算新赤道点  可用空间/2+kv_idx 
        equator=((meta_idx<kv_idx?buffer_size-(kv_idx-meta_idx-1):meta_idx-kv_idx+1)/2+kv_idx)%buffer_size;
        kv_border=(meta_idx+1)%buffer_size; // 下一个键值对的位置
        meta_border=kv_idx-1;               // 当前键值对的上一个位置
        if(meta_border<0)meta_border=buffer_size-1;
        kv_idx=(equator+1)%buffer_size;     // 下一个可写入键值对的位置
        meta_idx=equator;
        pthread_mutex_unlock(buffer_lock);
        // meta_queue==待溢写的元数据索引对象，
        // spill_files==溢写文件的文件名，fds中存储了打开的文件描述符，用于后续的写入操作
        std::priority_queue<MetaIndex,std::vector<MetaIndex>,MetaCmp> meta_queue;
        while((old_meta_idx<=old_equator?old_equator-old_meta_idx:buffer_size-old_meta_idx+old_equator)>=DEFAULT_META_SIZE){
            // printf("buffer_size is %d,meta_idx is %d,equator is %d\n",buffer_size,old_meta_idx,old_equator);
            meta_queue.push(MetaIndex::GetMetaIndex(buffer,(old_meta_idx+1)%buffer_size,buffer_size));
            old_meta_idx=(old_meta_idx+DEFAULT_META_SIZE)%buffer_size;//下一批元数据索引
        }
        // 溢出文件名spill_<spill_id>split<split_id>shuffle"  ==spill_id为溢写次数，split_id为拆分位置
        // std::string file_name="split_"+MapReduceUtil::IntToString(split_id)+"_spill_"+MapReduceUtil::IntToString(spill_id)+"_shuffle_";
        std::string file_name="spill_"+MapReduceUtil::IntToString(spill_id)+"_split_"+MapReduceUtil::IntToString(split_id)+"_shuffle_";
        int fds[partition_num];
        for(int i=0;i<partition_num;i++){
            spill_files[i].push_back(file_name+MapReduceUtil::IntToString(i+1)+".txt");
            fds[i]=open(&(spill_files[i].back()[0]),O_CREAT|O_RDWR,0777);
        }
        // 键值对 写入对应 分区溢出文件 里面
        while(meta_queue.size()){
            MetaIndex top_meta=meta_queue.top();
            meta_queue.pop();
            int key_len=top_meta.GetKeyLen();
            char* key=new char[key_len];
            int value_len=top_meta.GetValueLen();
            char* value=new char[value_len];
            top_meta.GetKey(key);
            top_meta.GetValue(value);

            int fd=fds[top_meta.GetPartition()-1];
            write(fd,key,key_len);
            char c=' ';
            write(fd,&c,1);
            write(fd,value,value_len);
            char cc[]="\r\n";
            write(fd,cc,2);

            delete [] key;
            delete [] value;
        }
        // spill_file_name.push_back(file_name);//装入临时文件名
        spill_id++;

        for(int i=0;i<partition_num;i++) close(fds[i]);
        //尝试唤醒
    }
    quit=true;
}