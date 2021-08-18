////
// @file tableTest.cc
// @brief
// 测试存储管理
//
//
#include "../catch.hpp"
#include <db/tableindex.h>
#include <iostream>
#include <fstream>
using namespace db;

TEST_CASE("db/tableindex.h")
{
    SECTION("create")
    {
        Table table;
        int ret = dbInitialize();
        REQUIRE(ret == S_OK);
        // 填充关系
        RelationInfo relation;
        relation.dataPath = "tablee.dat";
        relation.indexPath = "tablee.idx";

        // id char(20) varchar
        FieldInfo field;
        field.name = "id";
        field.index = 0;
        field.length = 8;
        field.fieldType = "BIGINT";
        relation.fields.push_back(field);

        field.name = "phone";
        field.index = 1;
        field.length = 20;
        field.fieldType = "CHAR";
        relation.fields.push_back(field);

        field.name = "name";
        field.index = 2;
        field.length = -255;
        field.fieldType = "VARCHAR";
        relation.fields.push_back(field);

        relation.count = 3;
        relation.key = 0;

        ret = table.create("tablee", relation);
        REQUIRE(ret == S_OK);
    }
    SECTION("open")
    {
        Table table;
        int ret = table.open("tablee");
        REQUIRE(ret == S_OK);
        ret = table.initial();
        REQUIRE(ret == S_OK);
        table.close("tablee");
    }
    SECTION("insert")
    {
        std::ofstream outputfile;
        outputfile.open("insert.txt");
        Table table;
        int ret = table.open("tablee");
        REQUIRE(ret == S_OK);
        ret = table.initial();
        REQUIRE(ret == S_OK);
        std::cout << "block:" << table.blockNum() << std::endl;
        std::cout << "index:" << table.indexBlockNum() << std::endl;
        std::cout << "freelength:" << table.freelength() << std::endl;
        std::cout << "slotsNum:" << table.slotsNum() << std::endl;
        for (int i = 100000; i > 0; i--) {
            struct iovec iov[3];
            long long id = i;
            iov[0].iov_base = &id;
            iov[0].iov_len = sizeof(long long);
            char *phone = "13534500702";
            iov[1].iov_base = (void *) phone;
            iov[1].iov_len = strlen(phone) + 1;
            char *name =
                "JunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixx"
                "xxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJuni"
                "JunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixx"
                "JunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixx"
                "xxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJuni"
                "xxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJuni"
                "xxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJunixxxxJun"
                "i";
            iov[2].iov_base = (void *) name;
            iov[2].iov_len = strlen(name) + 1;
            unsigned char header = 0x84;
            ret = table.insert(&header, iov, 3);
            REQUIRE(ret == S_OK);

            std::cout << "insert:" << i << std::endl;
            outputfile << "-----insert:" << i << "-----" << std::endl;
            outputfile << "blockNum:" << table.blockNum() << std::endl;
            outputfile << "indexNum:" << table.indexBlockNum() << std::endl;
            outputfile << "blockid:" << table.blockid() << std::endl;
            outputfile << "freelength:" << table.freelength() << std::endl;
            outputfile << "slotsNum:" << table.slotsNum() << std::endl;
            outputfile << "indexslotsNum:" << table.indexSlotsNum()
                       << std::endl;
            outputfile << std::endl;
        }
        long long cnt = 1;
        for (auto it1 = table.blockBegin(); it1 != table.blockEnd(); ++it1) {
            for (auto it2 = table.begin(it1); it2 != table.end(it1); ++it2) {
                Record record = *it2;
                iovec keyField;
                iovec Field;

                record.specialRef(keyField, 0);
                long long *keyFieldPointer = (long long *) keyField.iov_base;
                REQUIRE(*keyFieldPointer == cnt++);

                record.specialRef(Field, 1);
                char *FieldPointer = (char *) Field.iov_base;
                char *phone = "13534500702";
                REQUIRE(
                    strncmp(FieldPointer, phone, strlen(FieldPointer)) == 0);
                // std::cout << "ACK:" << cnt << std::endl;
            }
            std::cout << "blockACK:" << (*it1).blockid() << std::endl;
        }
        table.close("tablee");
        outputfile.close();
    }
    SECTION("remove")
    {
        std::ofstream outputfile;
        outputfile.open("remove.txt");
        Table table;
        int ret = table.open("tablee");
        REQUIRE(ret == S_OK);
        ret = table.initial();
        REQUIRE(ret == S_OK);
        for (long long i = 1; i < 80000; i++) {
            iovec field;
            long long id = i;
            field.iov_base = &id;
            field.iov_len = sizeof(long long);

            ret = table.remove(field);
            REQUIRE(ret == S_OK);

            auto bit = table.blockBegin();
            DataBlock block = *bit;
            while (block.getSlotsNum() == 0) {
                ++bit;
                block = *bit;
            }
            auto it = table.begin(bit);
            Record record = *it;
            iovec Field;
            record.specialRef(Field, 0);
            long long *keyFieldPointer = (long long *) Field.iov_base;
            REQUIRE(*keyFieldPointer == i + 1);

            record.specialRef(Field, 1);
            char *FieldPointer = (char *) Field.iov_base;
            char *phone = "13534500702";
            REQUIRE(strncmp(FieldPointer, phone, strlen(FieldPointer)) == 0);

            std::cout << "remove:" << i << std::endl;
            outputfile << "-----remove:" << i << "-----" << std::endl;
            std::cout << "remove:" << i << std::endl;
            outputfile << std::endl;
        }
        outputfile.close();
        table.close("tablee.dat");
    }
    SECTION("destroy")
    {
        Table table;
        int ret = table.open("tablee");
        REQUIRE(ret == S_OK);
        table.close("tablee");
        REQUIRE(table.destroy("tablee.dat", "tablee.idx") == S_OK);
        REQUIRE(gschema.destroy() == S_OK);
    }
}