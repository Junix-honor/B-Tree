////
// @dataFile tableindex.cc
// @brief
// 实现b+tree的聚集存储
//
// @author junix
//
#include <db/tableindex.h>
namespace db {

Table::Table()
    : relationInfo(NULL)
    , DataBlockCnt(0)
{
    buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
}
Table::~Table() { free(buffer_); }

int Table::create(const char *name, RelationInfo &info)
{
    return gschema.create(name, info);
}
int Table::open(const char *name)
{
    // 查找schema
    std::pair<Schema::TableSpace::iterator, bool> bret = gschema.lookup(name);
    if (!bret.second) return EINVAL;
    // 找到后，加载meta信息
    gschema.loadData(bret.first);

    relationInfo = &bret.first->second;
    unsigned int key = relationInfo->key;
    if (relationInfo->fields[key].type == NULL)
        relationInfo->fields[key].type =
            findDataType(relationInfo->fields[0].fieldType.c_str());
    //索引
    index_.open(name);

    return S_OK;
}
void Table::close(const char *name)
{
    relationInfo->dataFile.close();
    index_.close(name);
}
int Table::destroy(const char *dataPath, const char *indexPath)
{
    int ret = index_.destroy(indexPath);
    if (ret) return ret;
    ret = relationInfo->dataFile.remove(dataPath);
    if (ret) return ret;
    return S_OK;
}
int Table::initial()
{
    unsigned long long length;
    int ret = relationInfo->dataFile.length(length);
    if (ret) return ret;
    // 加载
    if (length) {
        relationInfo->dataFile.read(0, (char *) buffer_, Root::ROOT_SIZE);
        Root root;
        root.attach(buffer_);
        unsigned int first = root.getHead();
        DataBlockCnt = root.getCnt();
        readDataBlock(first);
    } else {
        Root root;
        unsigned char rb[Root::ROOT_SIZE];
        root.attach(rb);
        root.clear(BLOCK_TYPE_DATA);
        root.setHead(1);
        // 创建第1个block
        DataBlock block;
        block.attach(buffer_);
        block.clear(1);
        block.setNextid(-1);
        DataBlockCnt = 1;
        root.setCnt(DataBlockCnt);
        // 写root和block
        relationInfo->dataFile.write(0, (const char *) rb, Root::ROOT_SIZE);
        relationInfo->dataFile.write(
            Root::ROOT_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);
    }
    ret = index_.initial();
    if (ret) return ret;
    return S_OK;
}
int Table::splitDataBlock(int blockid, int &newid, struct iovec *field)
{
    unsigned int key = relationInfo->key;

    //原block
    int nextid;
    DataBlock block;
    readDataBlock(blockid);
    block.attach(buffer_);
    nextid = block.getNextid();

    //分裂的新block
    DataBlock newBlock1, newBlock2;
    unsigned char db1[Block::BLOCK_SIZE];
    unsigned char db2[Block::BLOCK_SIZE];
    newBlock1.attach(db1);
    newBlock1.clear(blockid);
    newid = ++DataBlockCnt;
    newBlock1.setNextid(newid);
    newBlock2.attach(db2);
    newBlock2.clear(newid);
    newBlock2.setNextid(nextid);

    unsigned short slotsNum = block.getSlotsNum();
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

        newBlock1.allocate(&header, iov, (int) fields);
        free(iov);
    }

    for (unsigned short index = slotsNum / 2; index < slotsNum; index++) {
        unsigned short recOffset = block.getSlot(index);
        Record record;
        record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);

        // 先分配iovec
        size_t fields = record.fields();
        struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
        unsigned char header;
        // 从记录得到iovec
        int ret = record.ref(iov, (int) fields, &header);
        if (!ret) return S_FALSE;
        //新block的第一个key字段
        if (index == slotsNum / 2) {
            field->iov_base = malloc(iov[key].iov_len);
            ::memcpy(field->iov_base, iov[key].iov_base, iov[key].iov_len);
            field->iov_len = iov[key].iov_len;
        }

        newBlock2.allocate(&header, iov, (int) fields);
        free(iov);
    }

    //写block
    size_t offset;
    offset = (newBlock1.blockid() - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->dataFile.write(offset, (const char *) db1, Block::BLOCK_SIZE);

    offset = (newBlock2.blockid() - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->dataFile.write(offset, (const char *) db2, Block::BLOCK_SIZE);

    //更新root
    int ret = writeRoot();
    if (ret) return ret;
    return S_OK;
}
int Table::combineDataBlock(
    int blockid,
    int comblockid,
    struct iovec *field,
    int isRight)
{
    unsigned int key = relationInfo->key;

    //原block
    DataBlock block;
    readDataBlock(blockid);
    block.attach(buffer_);

    //合并的block
    DataBlock comBlock;
    unsigned char db[Block::BLOCK_SIZE];
    comBlock.attach(db);
    size_t offset = (comblockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->dataFile.read(offset, (char *) db, Block::BLOCK_SIZE);

    unsigned short slotsNum = comBlock.getSlotsNum();
    for (unsigned short index = 0; index < slotsNum; index++) {
        unsigned short recOffset = comBlock.getSlot(index);
        Record record;
        record.attach(db + recOffset, Block::BLOCK_SIZE);
        // 先分配iovec
        size_t fields = record.fields();
        struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
        unsigned char header;
        // 从记录得到iovec
        record.ref(iov, (int) fields, &header);

        //得到删除block的第一个字段
        if (index == 0) {
            field->iov_base = malloc(iov[key].iov_len);
            ::memcpy(field->iov_base, iov[key].iov_base, iov[key].iov_len);
            field->iov_len = iov[key].iov_len;
        }

        int ret = block.allocate(&header, iov, (int) fields);
        if (!ret) return S_FALSE;
        free(iov);
    }

    // 排序
    std::vector<unsigned short> slotsv;
    for (int i = 0; i < block.getSlotsNum(); i++)
        slotsv.push_back(block.getSlot(i));
    if (relationInfo->fields[key].type == NULL)
        relationInfo->fields[key].type =
            findDataType(relationInfo->fields[key].fieldType.c_str());
    Compare cmp(relationInfo->fields[key], key, *this);
    std::sort(slotsv.begin(), slotsv.end(), cmp);
    for (int i = 0; i < block.getSlotsNum(); i++)
        block.setSlot(i, slotsv[i]);

    //调整nextid
    if (isRight) {
        block.setNextid(comBlock.getNextid());
        writeDataBlock(blockid);
    } else {
        writeDataBlock(blockid);
        DataBlock prepreBlock;
        auto bit = blockBegin();
        for (; bit != blockEnd(); ++bit) {
            prepreBlock = *bit;
            if (prepreBlock.getNextid() == comblockid) break;
        }
        if (bit == blockEnd()) return S_FALSE;

        prepreBlock.setNextid(blockid);
        writeDataBlock(prepreBlock.blockid());
    }
    return S_OK;
}
int Table::blockid()
{
    DataBlock block;
    block.attach(buffer_);
    return block.blockid();
}
unsigned int Table::blockNum() { return DataBlockCnt; }
unsigned int Table::indexBlockNum() { return index_.blockNum(); }
unsigned short Table::indexSlotsNum() { return index_.slotsNum(); }
unsigned short Table::freelength()
{
    DataBlock block;
    block.attach(buffer_);
    return block.getFreeLength();
}
unsigned short Table::slotsNum()
{
    DataBlock block;
    block.attach(buffer_);
    return block.getSlotsNum();
}
int Table::readDataBlock(int blockid)
{
    size_t offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->dataFile.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    return S_OK;
}
int Table::writeDataBlock(int blockid)
{
    size_t offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->dataFile.write(
        offset, (const char *) buffer_, Block::BLOCK_SIZE);
    return S_OK;
}
int Table::writeRoot()
{
    relationInfo->dataFile.read(0, (char *) buffer_, Root::ROOT_SIZE);
    Root root;
    root.attach(buffer_);
    root.setCnt(DataBlockCnt);
    relationInfo->dataFile.write(0, (const char *) buffer_, Root::ROOT_SIZE);
    return S_OK;
}
int Table::insert(const unsigned char *header, struct iovec *record, int iovcnt)
{
    //打开block
    bool ret = initial();
    if (ret) return ret;
    unsigned int key = relationInfo->key;
    iovec &keyField = record[key];
    DataBlock data;

    // TODO:检查是否重复插入

    //路径
    std::stack<int> path;
    //定位，插入位置的blockid
    int insertid = index_.sraech(record[key], path);

    readDataBlock(insertid);
    data.attach(buffer_);

    //插入
    ret = data.allocate(header, record, iovcnt);

    //插入失败则分裂
    if (!ret) {
        struct iovec field;
        int newid;
        splitDataBlock(data.blockid(), newid, &field); //分裂

        //判断插入的block的位置
        if (relationInfo->fields[key].type->compare(
                keyField.iov_base,
                field.iov_base,
                keyField.iov_len,
                field.iov_len))
            insertid = insertid;
        else
            insertid = newid;
        readDataBlock(insertid);
        data.attach(buffer_);
        ret = data.allocate(header, record, iovcnt);
        if (!ret) return S_FALSE;

        //更新b+tree
        ret = index_.insert(field, newid, path);
        if (ret) return ret;
    }

    // TODO:更新schema

    // 排序
    std::vector<unsigned short> slotsv;
    for (int i = 0; i < data.getSlotsNum(); i++)
        slotsv.push_back(data.getSlot(i));
    if (relationInfo->fields[key].type == NULL)
        relationInfo->fields[key].type =
            findDataType(relationInfo->fields[key].fieldType.c_str());
    Compare cmp(relationInfo->fields[key], key, *this);
    std::sort(slotsv.begin(), slotsv.end(), cmp);
    for (int i = 0; i < data.getSlotsNum(); i++)
        data.setSlot(i, slotsv[i]);

    // 处理checksum
    data.setChecksum();

    //写block
    ret = writeDataBlock(insertid);
    if (ret) return ret;
    return S_OK;
}
int Table::remove(struct iovec keyField)
{
    //打开block
    bool ret = initial();
    if (ret) return ret;
    unsigned int key = relationInfo->key;
    DataBlock data;

    //路径
    std::stack<int> path;
    //定位，目标位置的blockid
    int targetid = index_.sraech(keyField, path);

    readDataBlock(targetid);
    data.attach(buffer_);

    //删除
    int deleteIndex;
    deleteIndex = data.recDelete(&keyField, relationInfo);
    writeDataBlock(targetid);

    // 删除后结点填充度仍>=50%
    if (data.getUsedspace() >= data.INITIAL_FREE_SPACE_SIZE / 3) {
        //更新index
        if (deleteIndex ==
            0) //如果删除的记录是原本的第一条记录，那么blcok的最小键值发生了改变
        {
            //获取删除后blcok的最小键值
            struct iovec updateField;
            unsigned short recOffset = data.getSlot(0);
            Record record;
            record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
            record.specialRef(updateField, key);

            //更新父节点（IndexBlock）中指向这个DataBlock的右指针对应的键值
            ret = index_.updata(path.top(), keyField, updateField, targetid);
            if (ret) return ret;
        }
        return S_OK;
    }
    //找到一个最近的兄弟节点
    int brotherid, isRight; //兄弟节点的blockid，兄弟节点是否是右兄弟节点
    index_.getBrother(path.top(), targetid, brotherid, isRight);

    //如果没有兄弟节点，则直接删除即可，无需其他合并、借操作
    if (brotherid == -1) {
        //更新index
        if (deleteIndex ==
            0) //如果删除的记录是原本的第一条记录，那么blcok的最小键值发生了改
        {
            //获取删除后blcok的最小键值
            struct iovec updateField;
            unsigned short recOffset = data.getSlot(0);
            Record record;
            record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
            record.specialRef(updateField, key);

            //更新父节点（IndexBlock）中指向这个DataBlock的右指针对应的键值
            ret = index_.updata(path.top(), keyField, updateField, targetid);
            if (ret) return ret;
        }
        return S_OK;
    }

    //读兄弟节点到db
    unsigned char db[Block::BLOCK_SIZE];
    size_t offset = (brotherid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->dataFile.read(offset, (char *) db, Block::BLOCK_SIZE);
    DataBlock brother;
    brother.attach(db);

    // 兄弟结点填充度>50%,从兄弟节点借
    if (brother.getUsedspace() > brother.INITIAL_FREE_SPACE_SIZE * 2 / 3) {
        if (isRight) //如果是右兄弟节点
        {
            unsigned short recOffset = brother.getSlot(0);
            Record record;
            record.attach(db + recOffset, Block::BLOCK_SIZE);
            // 先分配iovec
            size_t fields = record.fields();
            struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
            unsigned char header;
            // 从记录得到iovec
            record.ref(iov, (int) fields, &header);
            data.allocate(&header, iov, (int) fields);

            // 排序
            std::vector<unsigned short> slotsv;
            for (int i = 0; i < data.getSlotsNum(); i++)
                slotsv.push_back(data.getSlot(i));
            if (relationInfo->fields[key].type == NULL)
                relationInfo->fields[key].type =
                    findDataType(relationInfo->fields[key].fieldType.c_str());
            Compare cmp(relationInfo->fields[key], key, *this);
            std::sort(slotsv.begin(), slotsv.end(), cmp);
            for (int i = 0; i < data.getSlotsNum(); i++)
                data.setSlot(i, slotsv[i]);

            //删除兄弟节点所借的记录
            brother.recDelete(&iov[key], relationInfo);
            //写兄弟节点
            size_t offset =
                (brotherid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            relationInfo->dataFile.write(
                offset, (const char *) db, Block::BLOCK_SIZE);

            //更新兄弟的父节点
            struct iovec updateField;
            recOffset = brother.getSlot(0);
            record.attach(db + recOffset, Block::BLOCK_SIZE);
            record.specialRef(updateField, key);
            ret = index_.updata(path.top(), iov[key], updateField, targetid);
            free(iov);
            if (ret) return ret;
        } else //如果是左兄弟节点
        {
            unsigned short recOffset =
                brother.getSlot(brother.getSlotsNum() - 1);
            Record record;
            record.attach(db + recOffset, Block::BLOCK_SIZE);
            // 先分配iovec
            size_t fields = record.fields();
            struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
            unsigned char header;
            // 从记录得到iovec
            record.ref(iov, (int) fields, &header);

            //更新父节点
            struct iovec oldField;
            Record rec;
            recOffset = data.getSlot(0);
            rec.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
            rec.specialRef(oldField, key);
            ret = index_.updata(path.top(), oldField, iov[key], targetid);
            if (ret) return ret;

            //借到的记录插入
            data.allocate(&header, iov, (int) fields);

            // 排序
            std::vector<unsigned short> slotsv;
            for (int i = 0; i < data.getSlotsNum(); i++)
                slotsv.push_back(data.getSlot(i));
            if (relationInfo->fields[key].type == NULL)
                relationInfo->fields[key].type =
                    findDataType(relationInfo->fields[key].fieldType.c_str());
            Compare cmp(relationInfo->fields[key], key, *this);
            std::sort(slotsv.begin(), slotsv.end(), cmp);
            for (int i = 0; i < data.getSlotsNum(); i++)
                data.setSlot(i, slotsv[i]);

            //删除兄弟节点所借的记录
            brother.recDelete(&iov[key], relationInfo);
            //写兄弟节点
            size_t offset =
                (brotherid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            relationInfo->dataFile.write(
                offset, (const char *) db, Block::BLOCK_SIZE);
            free(iov);
        }
        //写block
        ret = writeDataBlock(targetid);
        if (ret) return ret;
        return S_OK;
    }

    //兄弟结点填充度<=50%
    struct iovec field;
    if (isRight) //如果是右兄弟节点
        combineDataBlock(targetid, brotherid, &field, 1);//兄弟节点合并到当前节点
    else //如果是左兄弟节点
        combineDataBlock(brotherid, targetid, &field, 0);//当前节点合并到兄弟节点

    //更新b+tree
    ret = index_.remove(field, path);
    if (ret) return ret;
    return S_OK;
}
} // namespace db