////
// @file index.cc
// @brief
// B+ tree 索引
//
// @author junix
//
#include <db/bplustree.h>

namespace db {
BPlusTree::BPlusTree()
    : root_(0)
    , IndexBlockCnt(0)
{
    buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
}
BPlusTree::~BPlusTree() { free(buffer_); }
int BPlusTree::create(const char *name, RelationInfo &info)
{
    return gschema.create(name, info);
}
int BPlusTree::open(const char *name)
{
    // 查找schema
    std::pair<Schema::TableSpace::iterator, bool> bret = gschema.lookup(name);
    if (!bret.second) return EINVAL;
    // 找到后，加载meta信息
    gschema.loadIndex(bret.first);

    relationInfo = &bret.first->second;
    if (relationInfo->fields[relationInfo->key].type == NULL)
        relationInfo->fields[relationInfo->key].type = findDataType(
            relationInfo->fields[relationInfo->key].fieldType.c_str());

    return S_OK;
}
void BPlusTree::close(const char *name) { relationInfo->indexFile.close(); }
int BPlusTree::destroy(const char *name)
{
    return relationInfo->indexFile.remove(name);
}
int BPlusTree::initial()
{
    unsigned long long length;
    int ret = relationInfo->indexFile.length(length);
    if (ret) return ret;
    // 加载
    if (length) {
        relationInfo->indexFile.read(0, (char *) buffer_, Root::ROOT_SIZE);
        Root root;
        root.attach(buffer_);
        root_ = root.getHead();
        IndexBlockCnt = root.getCnt();
        size_t offset = (root_ - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        relationInfo->indexFile.read(
            offset, (char *) buffer_, Block::BLOCK_SIZE);
    } else {
        Root root;
        unsigned char rb[Root::ROOT_SIZE];
        root.attach(rb);
        root.clear(BLOCK_TYPE_INDEX);
        root.setHead(1);
        // 创建第1个block
        IndexBlock block;
        block.attach(buffer_);
        block.clear(1);
        block.setNextid(1);
        block.setNodeType(NODE_TYPE_POINT_TO_LEAF);
        root_ = 1;
        IndexBlockCnt = 1;
        root.setCnt(IndexBlockCnt);
        // 写root和block
        relationInfo->indexFile.write(0, (const char *) rb, Root::ROOT_SIZE);
        relationInfo->indexFile.write(
            Root::ROOT_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);
    }
    return S_OK;
}
int BPlusTree::readIndexBlock(int blockid)
{
    size_t offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->indexFile.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    return S_OK;
}
int BPlusTree::writeIndexBlock(int blockid)
{
    size_t offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->indexFile.write(
        offset, (const char *) buffer_, Block::BLOCK_SIZE);
    return S_OK;
}
int BPlusTree::writeRoot(int treeRoot)
{
    relationInfo->indexFile.read(0, (char *) buffer_, Root::ROOT_SIZE);
    Root root;
    root.attach(buffer_);
    root.setCnt(IndexBlockCnt);
    if (treeRoot) root.setHead(treeRoot);
    relationInfo->indexFile.write(0, (const char *) buffer_, Root::ROOT_SIZE);
    return S_OK;
}
unsigned int BPlusTree::blockNum() { return IndexBlockCnt; }
unsigned short BPlusTree::slotsNum()
{
    IndexBlock block;
    block.attach(buffer_);
    return block.getSlotsNum();
}
int BPlusTree::sraech(struct iovec &field, std::stack<int> &path)
{
    bool ret = initial();
    if (ret) return ret;
    IndexBlock index;
    path.push(root_);
    index.attach(buffer_);
    int pointer =
        index.getNextid(); //返回的指针，也就是定位的DataBlock的blockid

    // 索引条目：key---right pointer
    struct iovec rec[2];
    unsigned char header;

    //从上往下进行查找
    while (1) {
        int rows = index.getSlotsNum();

        //枚举key字段,寻找第一个键值大于等于key的位置
        for (int i = 0; i < rows; i++) {
            Record record;
            unsigned short reoff = index.getSlot(i);
            record.attach(buffer_ + reoff, Block::BLOCK_SIZE);
            record.ref(rec, 2, &header);

            //找到第一个键值大于等于key的位置
            if (!relationInfo->fields[relationInfo->key].type->compare(
                    rec[0].iov_base,
                    field.iov_base,
                    rec[0].iov_len,
                    field.iov_len)) {
                break;
            }
            //更新pointer
            pointer = *((int *) rec[1].iov_base);
        }
        //如果查询进行到了指向叶子节点的内部节点，则退出
        if (index.getNodeType() == NODE_TYPE_POINT_TO_LEAF) break;
        //把blockid加入栈，保存查询路径
        path.push(pointer);
        //读下一个indexblock
        size_t offset = (pointer - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        relationInfo->indexFile.read(
            offset, (char *) buffer_, Block::BLOCK_SIZE);
    }

    //返回所得到的DataBlock的blockid
    return pointer;
}
int BPlusTree::combineIndexBlock(
    int blockid,
    int comblockid,
    int fatherid,
    struct iovec *field)
{
    IndexBlock block;
    readIndexBlock(blockid);
    block.attach(buffer_);

    //被合并的block
    IndexBlock comBlock;
    unsigned char db[Block::BLOCK_SIZE];
    comBlock.attach(db);
    size_t offset = (comblockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->indexFile.read(offset, (char *) db, Block::BLOCK_SIZE);

    // comblock的key-pointer记录
    unsigned short slotsNum = comBlock.getSlotsNum();
    for (unsigned short index = 0; index < slotsNum; index++) {
        unsigned short recOffset = comBlock.getSlot(index);
        Record record;
        record.attach(db + recOffset, Block::BLOCK_SIZE);
        // 先分配iovec
        struct iovec iov[2];
        unsigned char header;
        // 从记录得到iovec
        record.ref(iov, 2, &header);

        int ret = block.allocate(&header, iov, 2);
        if (!ret) return S_FALSE;
    }

    // comblock的最左边指针
    int leftPointer = comBlock.getNextid();

    //要插入的key-pointer
    unsigned char insertHeader = 0x00;
    struct iovec insertRecord[2];
    insertRecord[0].iov_base = NULL;
    insertRecord[0].iov_len = 0;
    insertRecord[1].iov_base = &leftPointer;
    insertRecord[1].iov_len = sizeof(int);

    // comblock的父亲节点
    IndexBlock faBlock;
    faBlock.attach(db);
    offset = (fatherid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->indexFile.read(offset, (char *) db, Block::BLOCK_SIZE);

    //从父节点得到comblock的最左边指针对应的键值
    slotsNum = faBlock.getSlotsNum();
    for (unsigned short index = 0; index < slotsNum; index++) {
        unsigned short recOffset = faBlock.getSlot(index);
        Record record;
        record.attach(db + recOffset, Block::BLOCK_SIZE);
        struct iovec iov[2];
        unsigned char header;
        record.ref(iov, 2, &header);
        int bid = *((int *) iov[1].iov_base);
        if (bid == comblockid) {
            insertRecord[0].iov_base = iov[0].iov_base;
            insertRecord[0].iov_len = iov[0].iov_len;
            break;
        }
    }
    if (insertRecord[0].iov_base == NULL) return S_FALSE;
    //返回字段
    field->iov_base = malloc(insertRecord[0].iov_len);
    ::memcpy(
        field->iov_base, insertRecord[0].iov_base, insertRecord[0].iov_len);
    field->iov_len = insertRecord[0].iov_len;
    //插入
    int ret = block.allocate(&insertHeader, insertRecord, 2);
    if (!ret) return S_FALSE;

    // 排序
    std::vector<unsigned short> slotsv;
    for (int i = 0; i < block.getSlotsNum(); i++)
        slotsv.push_back(block.getSlot(i));
    treeCompare cmp(relationInfo->fields[relationInfo->key], 0, *this);
    std::sort(slotsv.begin(), slotsv.end(), cmp);
    for (int i = 0; i < block.getSlotsNum(); i++)
        block.setSlot(i, slotsv[i]);

    writeIndexBlock(blockid);
    return S_OK;
}
int BPlusTree::insert(struct iovec &field, int rightid, std::stack<int> &path)
{
    int insertid = path.top();
    path.pop();
    struct iovec retField;

    IndexBlock block;
    readIndexBlock(insertid);
    block.attach(buffer_);

    // 插入record字段：key---right pointer
    unsigned char insertHeader = 0x00;
    struct iovec insertRecord[2];
    insertRecord[0].iov_base = field.iov_base;
    insertRecord[0].iov_len = field.iov_len;
    insertRecord[1].iov_base = &rightid;
    insertRecord[1].iov_len = sizeof(int);

    int ret = block.allocate(&insertHeader, insertRecord, 2);

    //插入成功
    if (ret) {
        // 排序
        std::vector<unsigned short> slotsv;
        for (int i = 0; i < block.getSlotsNum(); i++)
            slotsv.push_back(block.getSlot(i));
        treeCompare cmp(relationInfo->fields[relationInfo->key], 0, *this);
        std::sort(slotsv.begin(), slotsv.end(), cmp);
        for (int i = 0; i < block.getSlotsNum(); i++)
            block.setSlot(i, slotsv[i]);

        //写block
        ret = writeIndexBlock(insertid);
        if (ret) return ret;

        //返回成功
        return S_OK;
    }

    // IndexBlock几个特殊record的key字段
    unsigned short slotsNum = block.getSlotsNum();
    Record record;
    struct iovec halfField, halfPlusField;
    unsigned short recOffset = block.getSlot(slotsNum / 2 - 1);
    record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
    record.specialRef(halfField, 0);
    recOffset = block.getSlot(slotsNum / 2);
    record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
    record.specialRef(halfPlusField, 0);

    // IndexBlock分裂成block1和block2
    IndexBlock block1, block2;
    unsigned char db1[Block::BLOCK_SIZE];
    unsigned char db2[Block::BLOCK_SIZE];
    block1.attach(db1);
    block1.clear(insertid);
    block1.setNextid(block.getNextid()); //设置block1最左边指针
    block1.setNodeType(block.getNodeType());
    int newid = ++IndexBlockCnt;
    block2.attach(db2);
    block2.clear(newid);
    block2.setNodeType(block.getNodeType());

    //情况1:field在中间位置
    if (relationInfo->fields[relationInfo->key].type->compare(
            field.iov_base,
            halfPlusField.iov_base,
            field.iov_len,
            halfPlusField.iov_len) &&
        relationInfo->fields[relationInfo->key].type->compare(
            halfField.iov_base,
            field.iov_base,
            halfField.iov_len,
            field.iov_len)) {
        //分裂IndexBlock，得到block1
        for (unsigned short index = 0; index < slotsNum / 2; index++) {
            unsigned short recOffset = block.getSlot(index);
            Record record;
            record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
            // 先分配iovec
            size_t fields = record.fields();
            struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
            unsigned char header;
            // 从记录得到iovec
            record.ref(iov, (int) fields, &header);
            // 插入block1
            block1.allocate(&header, iov, (int) fields);
            free(iov);
        }
        //分裂IndexBlock，得到block2
        for (unsigned short index = slotsNum / 2; index < slotsNum; index++) {
            unsigned short recOffset = block.getSlot(index);
            Record record;
            record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
            // 先分配iovec
            size_t fields = record.fields();
            struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
            unsigned char header;
            // 从记录得到iovec
            record.ref(iov, (int) fields, &header);
            // 插入block2
            block2.allocate(&header, iov, (int) fields);
            free(iov);
        }
        block2.setNextid(rightid); //设置block2最左边指针
        //设置返回字段
        retField.iov_base = malloc(field.iov_len);
        ::memcpy(retField.iov_base, field.iov_base, field.iov_len);
        retField.iov_len = field.iov_len;
    }
    //情况2:field不在中间位置
    else {
        int pos = 0;
        if (relationInfo->fields[relationInfo->key].type->compare(
                field.iov_base,
                halfField.iov_base,
                field.iov_len,
                halfField.iov_len))
            pos = slotsNum / 2 - 1;
        else
            pos = slotsNum / 2;

        //分裂IndexBlock，得到block1
        for (unsigned short index = 0; index < pos; index++) {
            unsigned short recOffset = block.getSlot(index);
            Record record;
            record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
            // 先分配iovec
            size_t fields = record.fields();
            struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
            unsigned char header;
            // 从记录得到iovec
            record.ref(iov, (int) fields, &header);

            // //测试
            // long long id = *((long long *) iov[0].iov_base);
            // int bid = *((int *) iov[1].iov_base);

            // 插入block1
            block1.allocate(&header, iov, (int) fields);
            free(iov);
        }
        //分裂IndexBlock，得到block2
        for (unsigned short index = pos + 1; index < slotsNum; index++) {
            unsigned short recOffset = block.getSlot(index);
            Record record;
            record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
            // 先分配iovec
            size_t fields = record.fields();
            struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
            unsigned char header;
            // 从记录得到iovec
            record.ref(iov, (int) fields, &header);

            // //测试
            // long long id = *((long long *) iov[0].iov_base);
            // int bid = *((int *) iov[1].iov_base);

            // 插入block2
            block2.allocate(&header, iov, (int) fields);
            free(iov);
        }

        // pos位置的record
        unsigned short recOffset = block.getSlot(pos);
        Record record;
        record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
        // 先分配iovec
        size_t fields = record.fields();
        struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
        unsigned char header;
        // 从记录得到iovec
        record.ref(iov, (int) fields, &header);
        //设置block2最左边指针
        block2.setNextid(*((int *) iov[1].iov_base));
        //设置返回字段
        retField.iov_base = malloc(iov[0].iov_len);
        ::memcpy(retField.iov_base, iov[0].iov_base, iov[0].iov_len);
        retField.iov_len = iov[0].iov_len;

        //插入record
        if (pos == slotsNum / 2 - 1) {
            ret = block1.allocate(&insertHeader, insertRecord, 2);
            if (!ret) return ret;
            // 排序
            std::vector<unsigned short> slotsv;
            for (int i = 0; i < block1.getSlotsNum(); i++)
                slotsv.push_back(block1.getSlot(i));
            treeCompare cmp(relationInfo->fields[relationInfo->key], 0, *this);
            std::sort(slotsv.begin(), slotsv.end(), cmp);
            for (int i = 0; i < block1.getSlotsNum(); i++)
                block1.setSlot(i, slotsv[i]);
        } else {
            ret = block2.allocate(&insertHeader, insertRecord, 2);
            if (!ret) return ret;
            // 排序
            std::vector<unsigned short> slotsv;
            for (int i = 0; i < block2.getSlotsNum(); i++)
                slotsv.push_back(block2.getSlot(i));
            treeCompare cmp(relationInfo->fields[relationInfo->key], 0, *this);
            std::sort(slotsv.begin(), slotsv.end(), cmp);
            for (int i = 0; i < block2.getSlotsNum(); i++)
                block2.setSlot(i, slotsv[i]);
        }
    }
    //写block
    size_t offset;
    offset = (block1.blockid() - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->indexFile.write(
        offset, (const char *) db1, Block::BLOCK_SIZE);

    offset = (block2.blockid() - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->indexFile.write(
        offset, (const char *) db2, Block::BLOCK_SIZE);

    // 直到根结点都满了，新生成根结点
    if (path.empty()) {
        IndexBlock newroot;
        newroot.attach(buffer_);
        newroot.clear(++IndexBlockCnt);
        newroot.setNextid(insertid); //设置newroot最左边指针
        newroot.setNodeType(NODE_TYPE_INTERNAL);

        //插入的记录
        insertRecord[0].iov_base = retField.iov_base;
        insertRecord[0].iov_len = retField.iov_len;
        insertRecord[1].iov_base = &newid;
        insertRecord[1].iov_len = sizeof(int);
        //插入记录
        ret = newroot.allocate(&insertHeader, insertRecord, 2);
        //更新b+tree root
        root_ = newroot.blockid();
        // 写newroot
        relationInfo->indexFile.write(
            (root_ - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE,
            (const char *) buffer_,
            Block::BLOCK_SIZE);
        //更新文件root
        ret = writeRoot(root_);
        if (ret) return ret;
        return S_OK;
    }
    //更新root
    ret = writeRoot(0);
    if (ret) return ret;
    //递归插入
    ret = insert(retField, newid, path);
    if (ret) return ret;
    free(retField.iov_base);
    return S_OK;
}
int BPlusTree::updata(
    int blockid,
    struct iovec &oldField,
    struct iovec &newField,
    int pointer)
{
    IndexBlock block;
    readIndexBlock(blockid);
    block.attach(buffer_);

    // 新的record字段：key---right pointer
    unsigned char insertHeader = 0x00;
    struct iovec insertRecord[2];
    insertRecord[0].iov_base = newField.iov_base;
    insertRecord[0].iov_len = newField.iov_len;
    insertRecord[1].iov_base = &pointer;
    insertRecord[1].iov_len = sizeof(int);

    int ret = block.recDelete(&oldField, relationInfo);
    if (ret == -1) return S_OK;
    ret = block.allocate(&insertHeader, insertRecord, 2);
    writeIndexBlock(blockid);
    return S_OK;
}
int BPlusTree::getBrother(
    int fatherid,
    int blockid,
    int &brotherid,
    int &isRight)
{
    IndexBlock block;
    readIndexBlock(fatherid);
    block.attach(buffer_);

    int broIndex = -1;
    if (block.getNextid() == blockid && block.getSlotsNum() > 0) {
        broIndex = 0;
        isRight = 1;
    } else {
        unsigned short slotsNum = block.getSlotsNum();
        //确定兄弟节点的broIndex
        for (unsigned short index = 0; index < slotsNum; index++) {
            unsigned short recOffset = block.getSlot(index);
            Record record;
            record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
            struct iovec bidField;
            record.specialRef(bidField, 1);
            int bid = *((int *) bidField.iov_base);
            if (bid == blockid) {
                if (index == slotsNum - 1) {
                    broIndex = index - 1;
                    isRight = 0;
                } else {
                    broIndex = index + 1;
                    isRight = 1;
                }
                break;
            }
        }
    }
    if (broIndex == -1)
        brotherid = -1;
    else {
        //得到兄弟节点的blockid
        unsigned short recOffset = block.getSlot(broIndex);
        Record record;
        record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
        struct iovec bidField;
        record.specialRef(bidField, 1);
        brotherid = *((int *) bidField.iov_base);
    }
    return S_OK;
}
int BPlusTree::remove(struct iovec &field, std::stack<int> &path)
{
    unsigned int key = relationInfo->key;

    //删除的索引条目所在的block
    int deleteid = path.top();
    path.pop();

    IndexBlock block;
    readIndexBlock(deleteid);
    block.attach(buffer_);

    //删除
    int deleteIndex;
    deleteIndex = block.recDelete(&field, relationInfo);
    if (deleteIndex == -1) return S_FALSE;
    writeIndexBlock(deleteid);

    if (path.empty()) //到根节点
    {
        return S_OK;
    }
    // 删除后结点填充度仍>=50%
    if (block.getUsedspace() >= block.INITIAL_FREE_SPACE_SIZE / 4) {
        //更新index
        if (deleteIndex == 0) {
            struct iovec updateField;
            unsigned short recOffset = block.getSlot(0);
            Record record;
            record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
            record.specialRef(updateField, key);
            int ret = updata(path.top(), field, updateField, deleteid);
            if (ret) return ret;
        }
        return S_OK;
    }
    //找到一个最近的兄弟节点
    int brotherid, isRight;
    getBrother(path.top(), deleteid, brotherid, isRight);
    if (brotherid == deleteid) return S_FALSE;

    //如果父节点只剩下一个儿子节点
    if (brotherid == -1) {
        //更新index
        if (deleteIndex == 0) {
            struct iovec updateField;
            unsigned short recOffset = block.getSlot(0);
            Record record;
            record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
            record.specialRef(updateField, key);
            int ret = updata(path.top(), field, updateField, deleteid);
            if (ret) return ret;
        }
        return S_OK;
    }

    //读兄弟节点到db
    unsigned char db[Block::BLOCK_SIZE];
    size_t offset = (brotherid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->dataFile.read(offset, (char *) db, Block::BLOCK_SIZE);
    IndexBlock brother;
    brother.attach(buffer_);
    //兄弟结点填充度>50%,从兄弟节点借
    if (brother.getUsedspace() > brother.INITIAL_FREE_SPACE_SIZE * 2 / 3) {
        // TODO:从兄弟节点借
        return S_FALSE;
    }
    //兄弟结点填充度<=50%
    struct iovec delField;
    if (isRight) {
        int ret = combineIndexBlock(deleteid, brotherid, path.top(), &delField);
        if (ret) return ret;
    } else {
        int ret = combineIndexBlock(brotherid, deleteid, path.top(), &delField);
        if (ret) return ret;
    }
    //递归删除
    int ret = remove(delField, path);
    if (ret) return ret;
    return S_OK;
}
} // namespace db