////
// @file index.h
// @brief
// B+ tree 索引
//
// @author junix
//

#ifndef __DB_BPLUSTREE_H__
#define __DB_BPLUSTREE_H__

#include <db/schema.h>
#include <db/block.h>
#include <db/record.h>
#include <string>
#include <utility>
#include <vector>
#include <db/config.h>
#include <algorithm>
#include <stack>

namespace db {

//比较
struct treeCompare;
class BPlusTree
{
  public:
    //友元类声明
    friend struct treeCompare;

  public:
    BPlusTree();
    ~BPlusTree();

  public:
    //创建表的索引
    int create(const char *name, RelationInfo &info);
    //打开表的索引
    int open(const char *name);
    //关闭一张表
    void close(const char *name);
    //摧毁一张表
    int destroy(const char *name);
    //初始化
    int initial();
    //分裂indexblock
    int splitIndexBlock(int blockid, int &newid, struct iovec &field);
    //合并indexblock
    int combineIndexBlock(
        int blockid,
        int comblockid,
        int fatherid,
        struct iovec *field);
    //读取指定id的block
    int readIndexBlock(int blockid);
    //写指定id的block
    int writeIndexBlock(int blockid);
    //更新root
    int writeRoot(int treeRoot);
    //!返回当前block的num,测试需要
    unsigned int blockNum();
    //!返回当前block的slotsNum,测试需要
    unsigned short slotsNum();
    //查找，返回dataBlock的id
    int sraech(struct iovec &field, std::stack<int> &path);
    //插入
    int insert(struct iovec &field, int rightid, std::stack<int> &path);
    //删除
    int remove(struct iovec &field, std::stack<int> &path);

    //更新父节点的key
    int updata(
        int blockid,
        struct iovec &oldField,
        struct iovec &newField,
        int pointer);
    //找节点的兄弟节点
    int getBrother(int fatherid, int blockid, int &brotherid, int &isRight);

  private:
    unsigned char *buffer_;     // block，TODO: 缓冲模块
    RelationInfo *relationInfo; //表信息
    int root_;                  //根节点id
    unsigned int IndexBlockCnt; // indexblock数目
};
struct treeCompare
{
  private:
    FieldInfo &fin;
    unsigned int key;
    BPlusTree &bplustree;

  public:
    treeCompare(FieldInfo &f, unsigned int k, BPlusTree &ibplustree)
        : fin(f)
        , key(k)
        , bplustree(ibplustree)
    {}
    bool operator()(const unsigned short &x, const unsigned short &y) const
    {
        //根据x, y偏移量，引用两条记录；
        Record rx, ry;
        rx.attach(bplustree.buffer_ + x, Block::BLOCK_SIZE);
        ry.attach(bplustree.buffer_ + y, Block::BLOCK_SIZE);
        iovec keyx, keyy;
        rx.specialRef(keyx, key);
        ry.specialRef(keyy, key);
        return fin.type->compare(
            keyx.iov_base, keyy.iov_base, keyx.iov_len, keyy.iov_len);
    }
};
} // namespace db

#endif // __DB_BPLUSTREE_H__